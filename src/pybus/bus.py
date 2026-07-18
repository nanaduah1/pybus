from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
import time
from dataclasses import dataclass
from threading import Event
from typing import cast
import unicodedata

from pybus.codecs import PayloadCodec
from pybus.delivery import CommandDeliveryObserver
from pybus.durable import (
    DurableJobDraft,
    DurableJobController,
    DurableJobHandle,
    DurableJobPoller,
    DurableJobPolicy,
    DurableJobRunner,
    DurableJobStore,
)
from pybus.dispatcher import Dispatcher
from pybus.envelope import MessageEnvelope
from pybus.exceptions import (
    DurableJobsNotConfiguredError,
    JobSeriesNotSupportedError,
    InvalidMessageDefinitionError,
    MessageTimeoutError,
)
from pybus.listener import Listener
from pybus.messages import (
    BaseMessage,
    RequestMessage,
    ResponseMessage,
    is_typed_message_class,
    message_class_for_kind,
    typed_message_payload,
    validate_queue_name,
)
from pybus.contracts import Transport
from pybus.queues import QueueTopology
from pybus.request_response import (
    RequestResponseCoordinator,
    default_reply_queue_name,
)
from pybus.recurrence import (
    Recurrence,
    JobSeriesDraft,
    JobSeriesHandle,
    JobSeriesStore,
)
from pybus.serializer import JsonSerializer
from pybus.worker import Worker, WorkerHook


MAX_MESSAGE_ID_LENGTH = 255
FRAMEWORK_DELIVERY_HEADERS = frozenset(
    {
        "dead_lettered_from",
        "last_attempt",
        "pybus_durable_generation",
        "pybus_durable_record",
        "retries",
    }
)


class _Unset:
    pass


_UNSET = _Unset()


def _validate_message_id(message_id: object | None) -> str | None:
    if message_id is None:
        return None
    if not isinstance(message_id, str):
        raise InvalidMessageDefinitionError("message_id must be a string or None")
    if not message_id.strip():
        raise InvalidMessageDefinitionError("message_id must be non-empty")
    if len(message_id) > MAX_MESSAGE_ID_LENGTH:
        raise InvalidMessageDefinitionError(
            f"message_id cannot exceed {MAX_MESSAGE_ID_LENGTH} characters"
        )
    if any(unicodedata.category(character) == "Cc" for character in message_id):
        raise InvalidMessageDefinitionError(
            "message_id cannot contain control characters"
        )
    return message_id


def _copy_headers(headers: Mapping[str, object] | None) -> dict[str, object]:
    if headers is None:
        return {}
    if not isinstance(headers, Mapping):
        raise InvalidMessageDefinitionError("headers must be a mapping")
    copied = dict(headers)
    if any(not isinstance(key, str) for key in copied):
        raise InvalidMessageDefinitionError("header keys must be strings")
    reserved = FRAMEWORK_DELIVERY_HEADERS.intersection(copied)
    if reserved:
        names = ", ".join(sorted(reserved))
        raise InvalidMessageDefinitionError(
            f"headers contain framework-reserved keys: {names}"
        )
    return copied


@dataclass(slots=True)
class Pybus:
    transport: Transport
    dispatcher: Dispatcher
    serializer: JsonSerializer
    topology: QueueTopology
    listener: Listener
    coordinator: RequestResponseCoordinator
    reply_queue: str
    payload_codec: PayloadCodec
    worker_hook_factories: tuple[Callable[[], WorkerHook], ...]
    command_delivery_observers: tuple[CommandDeliveryObserver, ...]
    _durable_job_store: DurableJobStore | None
    _durable_job_policy: DurableJobPolicy

    def __init__(
        self,
        transport: object,
        *,
        dispatcher: Dispatcher | None = None,
        serializer: JsonSerializer | None = None,
        topology: QueueTopology | None = None,
        handler_targets: Sequence[object] | None = None,
        payload_codec: PayloadCodec | None = None,
        worker_hook_factories: Sequence[Callable[[], WorkerHook]] = (),
        command_delivery_observers: Sequence[CommandDeliveryObserver] = (),
        durable_job_store: DurableJobStore | None | _Unset = _UNSET,
        durable_job_policy: DurableJobPolicy | None | _Unset = _UNSET,
        durable_command_store: DurableJobStore | None | _Unset = _UNSET,
        durable_command_policy: DurableJobPolicy | None | _Unset = _UNSET,
    ) -> None:
        if durable_job_store is not _UNSET and durable_command_store is not _UNSET:
            raise TypeError(
                "durable_job_store and durable_command_store cannot both be supplied"
            )
        if durable_job_policy is not _UNSET and durable_command_policy is not _UNSET:
            raise TypeError(
                "durable_job_policy and durable_command_policy cannot both be supplied"
            )
        resolved_store = (
            durable_job_store
            if durable_job_store is not _UNSET
            else durable_command_store
            if durable_command_store is not _UNSET
            else None
        )
        resolved_policy = (
            durable_job_policy
            if durable_job_policy is not _UNSET
            else durable_command_policy
            if durable_command_policy is not _UNSET
            else None
        )
        if resolved_policy is not None and not isinstance(
            resolved_policy, DurableJobPolicy
        ):
            raise TypeError("durable_job_policy must be a DurableJobPolicy")
        self.transport = transport
        if dispatcher is not None and payload_codec is not None:
            raise ValueError(
                "payload_codec must be configured on the supplied dispatcher"
            )
        self.dispatcher = dispatcher or Dispatcher(payload_codec=payload_codec)
        self.payload_codec = self.dispatcher.payload_codec
        self.serializer = serializer or JsonSerializer()
        self.topology = topology or QueueTopology()
        self._durable_job_store = resolved_store
        self._durable_job_policy = resolved_policy or DurableJobPolicy()
        self.listener = Listener(
            transport=transport,
            dispatcher=self.dispatcher,
            serializer=self.serializer,
            dead_letter_channel=self.topology.dead_letter_queue,
            topology=self.topology,
            command_delivery_observers=command_delivery_observers,
            durable_job_controller=(
                None
                if resolved_store is None
                else DurableJobController(resolved_store, self.durable_job_policy)
            ),
        )
        self.coordinator = RequestResponseCoordinator()
        self.reply_queue = default_reply_queue_name()
        self.worker_hook_factories = tuple(worker_hook_factories)
        self.command_delivery_observers = tuple(command_delivery_observers)
        if any(not callable(factory) for factory in self.worker_hook_factories):
            raise TypeError("worker_hook_factories must contain callables")
        if any(not callable(observer) for observer in self.command_delivery_observers):
            raise TypeError("command_delivery_observers must contain callables")
        if handler_targets:
            from pybus.handlers import register_handlers

            register_handlers(*handler_targets, registry=self.dispatcher.registry)

    @property
    def durable_job_store(self) -> DurableJobStore | None:
        return self._durable_job_store

    @durable_job_store.setter
    def durable_job_store(self, value: DurableJobStore | None) -> None:
        self._durable_job_store = value
        self._refresh_durable_job_controller()

    @property
    def durable_job_policy(self) -> DurableJobPolicy:
        return self._durable_job_policy

    @durable_job_policy.setter
    def durable_job_policy(self, value: DurableJobPolicy) -> None:
        if not isinstance(value, DurableJobPolicy):
            raise TypeError("durable_job_policy must be a DurableJobPolicy")
        self._durable_job_policy = value
        self._refresh_durable_job_controller()

    @property
    def durable_command_store(self) -> DurableJobStore | None:
        """Compatibility alias for :attr:`durable_job_store`."""

        return self.durable_job_store

    @durable_command_store.setter
    def durable_command_store(self, value: DurableJobStore | None) -> None:
        self.durable_job_store = value

    @property
    def durable_command_policy(self) -> DurableJobPolicy:
        """Compatibility alias for :attr:`durable_job_policy`."""

        return self.durable_job_policy

    @durable_command_policy.setter
    def durable_command_policy(self, value: DurableJobPolicy) -> None:
        self.durable_job_policy = value

    def _refresh_durable_job_controller(self) -> None:
        if not hasattr(self, "listener"):
            return
        controller = (
            None
            if self._durable_job_store is None
            else DurableJobController(self._durable_job_store, self._durable_job_policy)
        )
        self.listener._durable_job_controller = controller
        self.listener._durable_command_controller = controller

    def publish_event(
        self,
        event: object,
        *,
        queue: str | None = None,
        message_id: str | None = None,
        headers: Mapping[str, object] | None = None,
    ) -> MessageEnvelope:
        return self._publish(
            event,
            expected_kind="event",
            queue=queue,
            message_id=message_id,
            headers=headers,
        )

    def send_command(
        self,
        command: object,
        *,
        queue: str | None = None,
        message_id: str | None = None,
        headers: Mapping[str, object] | None = None,
    ) -> MessageEnvelope:
        return self._publish(
            command,
            expected_kind="command",
            queue=queue,
            message_id=message_id,
            headers=headers,
        )

    def schedule_command(
        self,
        command: object,
        *,
        run_at: datetime | None = None,
        recurrence: Recurrence | None = None,
        idempotency_key: str | None = None,
    ) -> DurableJobHandle:
        if self.durable_job_store is None:
            raise DurableJobsNotConfiguredError(
                "durable jobs require a durable_job_store"
            )
        if not is_typed_message_class(type(command)):
            raise InvalidMessageDefinitionError(
                "schedule_command requires an @command typed command"
            )
        if idempotency_key is not None and (
            not isinstance(idempotency_key, str)
            or not idempotency_key.strip()
            or len(idempotency_key) > 255
        ):
            raise InvalidMessageDefinitionError(
                "idempotency_key must be a non-empty string of at most 255 characters"
            )
        if run_at is not None:
            if (
                not isinstance(run_at, datetime)
                or run_at.tzinfo is None
                or run_at.utcoffset() is None
            ):
                raise InvalidMessageDefinitionError("run_at must be timezone-aware")
        if recurrence is not None and not isinstance(recurrence, Recurrence):
            raise InvalidMessageDefinitionError("recurrence must be a Recurrence")
        envelope = self.prepare_command(command)
        queue = self._resolve_publication_queue(command, None)
        normalized_run_at = (
            envelope.created_at if run_at is None else run_at.astimezone(timezone.utc)
        )
        fingerprint_fields = {
            "message_type": envelope.message_type,
            "version": envelope.version,
            "payload": envelope.payload,
            "headers": envelope.headers,
            "queue": queue,
        }
        if run_at is not None:
            fingerprint_fields["run_at"] = normalized_run_at.isoformat()
        if recurrence is not None:
            normalized_end = (
                None
                if recurrence.ends_at is None
                else recurrence.ends_at.astimezone(timezone.utc)
            )
            if normalized_end is not None and normalized_run_at >= normalized_end:
                raise InvalidMessageDefinitionError("run_at must be before ends_at")
            fingerprint_fields["recurrence"] = {
                "cadence": recurrence.cadence.value,
                "timezone": recurrence.timezone,
                "ends_at": (
                    None if normalized_end is None else normalized_end.isoformat()
                ),
                "run_at": (None if run_at is None else normalized_run_at.isoformat()),
            }
        fingerprint = self.serializer.dumps(fingerprint_fields)
        draft = DurableJobDraft(
            message_id=envelope.message_id,
            message_type=envelope.message_type,
            version=envelope.version,
            payload=envelope.payload,
            headers=dict(envelope.headers),
            created_at=envelope.created_at,
            available_at=normalized_run_at,
            queue=queue,
            fingerprint=fingerprint,
            idempotency_key=idempotency_key,
        )
        if recurrence is not None:
            if not isinstance(self.durable_job_store, JobSeriesStore):
                raise JobSeriesNotSupportedError(
                    "configured durable store does not support recurrence"
                )
            _, record = self.durable_job_store.schedule_recurring(
                JobSeriesDraft(
                    first_occurrence=draft,
                    recurrence=recurrence,
                    starts_at=normalized_run_at,
                    fingerprint=fingerprint,
                    idempotency_key=idempotency_key,
                )
            )
        else:
            record = self.durable_job_store.schedule(draft)
        return DurableJobHandle.from_record(record)

    def cancel_recurring_command(self, series_id: str) -> JobSeriesHandle:
        if self.durable_job_store is None:
            raise DurableJobsNotConfiguredError(
                "durable jobs require a durable_job_store"
            )
        if not isinstance(self.durable_job_store, JobSeriesStore):
            raise JobSeriesNotSupportedError(
                "configured durable store does not support recurrence"
            )
        if not isinstance(series_id, str) or not series_id.strip():
            raise InvalidMessageDefinitionError("series_id must be a non-empty string")
        record = self.durable_job_store.cancel_recurring(
            series_id=series_id,
            cancelled_at=datetime.now(timezone.utc),
        )
        return JobSeriesHandle.from_record(record)

    def create_durable_job_worker(
        self,
        *,
        hooks: Sequence[WorkerHook] | None = None,
        error_delay: float = 1.0,
        logger=None,
        stop_event=None,
        policy: DurableJobPolicy | None = None,
        idle_delay: float = 1.0,
    ) -> Worker:
        if self.durable_job_store is None:
            raise DurableJobsNotConfiguredError(
                "durable jobs require a durable_job_store"
            )
        resolved_hooks = (
            tuple(factory() for factory in self.worker_hook_factories)
            if hooks is None
            else tuple(hooks)
        )
        if any(not isinstance(hook, WorkerHook) for hook in resolved_hooks):
            raise TypeError("worker hook factories must return WorkerHook instances")
        resolved_stop_event = Event() if stop_event is None else stop_event
        poller = DurableJobPoller(
            DurableJobRunner(
                self.durable_job_store,
                lambda envelope, queue: self.publish_prepared(envelope, queue=queue),
                policy=policy or self.durable_job_policy,
            ),
            idle_delay=idle_delay,
            stop_event=resolved_stop_event,
        )
        return Worker(
            poller,
            "__pybus_durable_commands__",
            hooks=resolved_hooks,
            error_delay=error_delay,
            logger=logger,
            stop_event=resolved_stop_event,
        )

    def prepare_event(
        self,
        event: object,
        *,
        message_id: str | None = None,
        headers: Mapping[str, object] | None = None,
    ) -> MessageEnvelope:
        return self._prepare_message(
            event, expected_kind="event", message_id=message_id, headers=headers
        )

    def prepare_command(
        self,
        command: object,
        *,
        message_id: str | None = None,
        headers: Mapping[str, object] | None = None,
    ) -> MessageEnvelope:
        return self._prepare_message(
            command,
            expected_kind="command",
            message_id=message_id,
            headers=headers,
        )

    def publish_prepared(
        self,
        envelope: MessageEnvelope,
        *,
        queue: str | None = None,
    ) -> MessageEnvelope:
        self._validate_prepared_envelope(envelope)
        target_queue = self._resolve_prepared_queue(queue)
        return self._publish_envelope(envelope, queue=target_queue)

    def _validate_prepared_envelope(self, envelope: MessageEnvelope) -> None:
        if not isinstance(envelope, MessageEnvelope):
            raise TypeError("envelope must be a MessageEnvelope")
        envelope.validate()
        _validate_message_id(envelope.message_id)

    def _resolve_prepared_queue(self, queue: str | None) -> str:
        target_queue = validate_queue_name(queue or self.topology.default_queue)
        if not self.topology.has_queue(target_queue):
            raise InvalidMessageDefinitionError(
                f"queue {target_queue!r} is not declared in the bus topology"
            )
        return target_queue

    def request(
        self,
        request: RequestMessage,
        *,
        timeout: int | None = 5,
        queue: str | None = None,
        reply_to: str | None = None,
    ) -> ResponseMessage:
        request_queue = queue or self.topology.default_queue
        reply_queue = reply_to or request.reply_to or self.reply_queue
        request_envelope, ticket = self.coordinator.prepare_request(
            request,
            timeout=timeout,
            reply_to=reply_queue,
            payload_codec=self.payload_codec,
        )
        self.transport.publish(
            request_queue,
            self.serializer.dump(request_envelope),
        )
        response_envelope = self._await_response(
            ticket.correlation_id,
            timeout,
            reply_queue,
        )
        response_cls = message_class_for_kind(response_envelope.message_kind)
        return cast(
            ResponseMessage,
            response_cls.from_envelope(
                response_envelope,
                payload_codec=self.payload_codec,
            ),
        )

    def listen_once(self, channel: str | tuple[str, ...] | list[str]) -> object | None:
        return self.listener.listen_once(channel)

    def listen(
        self,
        channel: str | tuple[str, ...] | list[str],
        *,
        max_messages: int | None = None,
    ) -> list[object]:
        return self.listener.listen(channel, max_messages=max_messages)

    def create_worker(
        self,
        channel: str | Sequence[str] | None = None,
        *,
        hooks: Sequence[WorkerHook] | None = None,
        error_delay: float = 1.0,
        logger=None,
        stop_event=None,
    ) -> Worker:
        resolved_hooks = (
            tuple(factory() for factory in self.worker_hook_factories)
            if hooks is None
            else tuple(hooks)
        )
        if any(not isinstance(hook, WorkerHook) for hook in resolved_hooks):
            raise TypeError("worker hook factories must return WorkerHook instances")
        return Worker(
            self.listener,
            self.topology.default_queue if channel is None else channel,
            hooks=resolved_hooks,
            error_delay=error_delay,
            logger=logger,
            stop_event=stop_event,
        )

    def _publish(
        self,
        message: object,
        *,
        expected_kind: str,
        queue: str | None = None,
        message_id: str | None = None,
        headers: Mapping[str, object] | None = None,
    ) -> MessageEnvelope:
        envelope = self._prepare_message(
            message,
            expected_kind=expected_kind,
            message_id=message_id,
            headers=headers,
        )
        target_queue = self._resolve_publication_queue(message, queue)
        return self._publish_envelope(envelope, queue=target_queue)

    def _resolve_publication_queue(self, message: object, queue: str | None) -> str:
        if queue is not None:
            resolved_queue = queue
        elif is_typed_message_class(type(message)):
            declared_queue = getattr(type(message), "__pybus_default_queue__", None)
            resolved_queue = (
                self.topology.default_queue
                if declared_queue is None
                else declared_queue
            )
        else:
            resolved_queue = self.topology.default_queue
        resolved_queue = validate_queue_name(resolved_queue)
        if not self.topology.has_queue(resolved_queue):
            raise InvalidMessageDefinitionError(
                f"queue {resolved_queue!r} is not declared in the bus topology"
            )
        return resolved_queue

    def _prepare_message(
        self,
        message: object,
        *,
        expected_kind: str,
        message_id: str | None = None,
        headers: Mapping[str, object] | None = None,
    ) -> MessageEnvelope:
        resolved_message_id = _validate_message_id(message_id)
        supplied_headers = _copy_headers(headers)
        message_kind = getattr(message, "message_kind", None)
        if message_kind != expected_kind:
            raise InvalidMessageDefinitionError(
                f"Expected an {expected_kind}, got {message_kind!r}"
            )
        if isinstance(message, BaseMessage):
            message.validate()
            merged_headers = {**message.headers, **supplied_headers}
            return MessageEnvelope.create(
                message_id=resolved_message_id,
                message_type=message.message_type,
                message_kind=message.message_kind,
                version=message.version,
                payload=self.payload_codec.encode(
                    message.payload, context=merged_headers
                ),
                headers=self.payload_codec.encode(merged_headers),
                correlation_id=message.correlation_id,
                causation_id=message.causation_id,
                reply_to=message.reply_to,
                expires_at=message.expires_at,
                content_type=message.content_type,
                content_encoding=message.content_encoding,
            )
        if not is_typed_message_class(type(message)):
            raise InvalidMessageDefinitionError(
                f"{type(message).__name__} is not a declared pybus message"
            )
        registered_class = self.dispatcher.registry.message_class_for(
            message.message_kind,
            message.message_type,
        )
        if registered_class is not None and type(message) is not registered_class:
            raise InvalidMessageDefinitionError(
                f"Expected {registered_class.__name__} for "
                f"{message.message_kind}:{message.message_type}"
            )
        payload = typed_message_payload(message)
        return MessageEnvelope.create(
            message_type=message.message_type,
            message_kind=message.message_kind,
            version=message.version,
            payload=self.payload_codec.encode(payload, context=supplied_headers),
            headers=self.payload_codec.encode(supplied_headers),
            message_id=resolved_message_id,
        )

    def _publish_envelope(
        self,
        envelope: MessageEnvelope,
        *,
        queue: str | None = None,
    ) -> MessageEnvelope:
        self.transport.publish(
            queue or self.topology.default_queue, self.serializer.dump(envelope)
        )
        return envelope

    def _await_response(
        self,
        correlation_id: str,
        timeout: int | None,
        reply_queue: str,
    ) -> MessageEnvelope:
        deadline = None
        if timeout is not None:
            deadline = time.monotonic() + timeout

        while True:
            buffered = self.coordinator.take_response(correlation_id)
            if buffered is not None:
                return buffered

            if deadline is not None and time.monotonic() >= deadline:
                raise MessageTimeoutError(
                    f"Timed out waiting for response {correlation_id}"
                )

            poll_timeout = 1
            sleep_interval = 0.01
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise MessageTimeoutError(
                        f"Timed out waiting for response {correlation_id}"
                    )
                # Poll in short slices so sub-second timeouts are respected.
                if remaining < 1:
                    poll_timeout = 0
                    sleep_interval = min(0.01, remaining)
                else:
                    poll_timeout = 1
                    sleep_interval = 0.01

            raw_message = self.transport.consume(reply_queue, timeout=poll_timeout)
            if raw_message is None:
                time.sleep(sleep_interval)
                continue

            envelope = MessageEnvelope.from_dict(self.serializer.loads(raw_message))
            self.coordinator.store_response(envelope)


Pybus.create_durable_command_worker = Pybus.create_durable_job_worker


_default_bus: Pybus | None = None


def _set_default_bus(bus: Pybus) -> Pybus:
    global _default_bus
    _default_bus = bus
    return bus


def configure_transport(
    transport: object,
    *,
    dispatcher: Dispatcher | None = None,
    serializer: JsonSerializer | None = None,
    topology: QueueTopology | None = None,
    handler_targets: Sequence[object] | None = None,
    payload_codec: PayloadCodec | None = None,
    command_delivery_observers: Sequence[CommandDeliveryObserver] = (),
) -> Pybus:
    return _set_default_bus(
        Pybus(
            transport,
            dispatcher=dispatcher,
            serializer=serializer,
            topology=topology,
            handler_targets=handler_targets,
            payload_codec=payload_codec,
            command_delivery_observers=command_delivery_observers,
        )
    )


def get_bus() -> Pybus:
    if _default_bus is None:
        raise RuntimeError("pybus transport has not been configured")
    return _default_bus


def publish_event(
    event: object,
    *,
    queue: str | None = None,
    message_id: str | None = None,
    headers: Mapping[str, object] | None = None,
) -> MessageEnvelope:
    return get_bus().publish_event(
        event, queue=queue, message_id=message_id, headers=headers
    )


def send_command(
    command: object,
    *,
    queue: str | None = None,
    message_id: str | None = None,
    headers: Mapping[str, object] | None = None,
) -> MessageEnvelope:
    return get_bus().send_command(
        command, queue=queue, message_id=message_id, headers=headers
    )


def schedule_command(
    command: object,
    *,
    run_at: datetime | None = None,
    recurrence: Recurrence | None = None,
    idempotency_key: str | None = None,
) -> DurableJobHandle:
    return get_bus().schedule_command(
        command,
        run_at=run_at,
        recurrence=recurrence,
        idempotency_key=idempotency_key,
    )


def cancel_recurring_command(series_id: str) -> JobSeriesHandle:
    return get_bus().cancel_recurring_command(series_id)


def prepare_event(
    event: object,
    *,
    message_id: str | None = None,
    headers: Mapping[str, object] | None = None,
) -> MessageEnvelope:
    return get_bus().prepare_event(event, message_id=message_id, headers=headers)


def prepare_command(
    command: object,
    *,
    message_id: str | None = None,
    headers: Mapping[str, object] | None = None,
) -> MessageEnvelope:
    return get_bus().prepare_command(command, message_id=message_id, headers=headers)


def publish_prepared(
    envelope: MessageEnvelope,
    *,
    queue: str | None = None,
) -> MessageEnvelope:
    return get_bus().publish_prepared(envelope, queue=queue)


def request(
    request: RequestMessage,
    *,
    timeout: int | None = 5,
    queue: str | None = None,
    reply_to: str | None = None,
) -> ResponseMessage:
    return get_bus().request(request, timeout=timeout, queue=queue, reply_to=reply_to)
