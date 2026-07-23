from __future__ import annotations

import base64
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta, timezone
import logging
import time

from pybus.batching import batched_buffer_key
from pybus.delivery import (
    CommandDeliveryObserver,
    CommandDeliveryOutcome,
    CommandDeliveryStatus,
)
from pybus.dispatcher import Dispatcher
from pybus.envelope import MessageEnvelope
from pybus.durable import DurableJobController, DurableDeliveryDecision
from pybus.exceptions import (
    DeliveryObservationError,
    HandlerNotFoundError,
    IndeterminateDeliveryError,
    InvalidMessageDefinitionError,
    WorkerAbortError,
)
from pybus.handlers import ContinueProcessing, HandlerSpec, _handler_spec
from pybus.messages import CommandMessage, RequestMessage, ResponseMessage
from pybus.recurrence import EndRecurrence, ScheduleNextOccurrence
from pybus.queues import (
    DEFAULT_FAILED_QUEUE_NAME as _DEFAULT_FAILED_QUEUE_NAME,
    DEFAULT_QUEUE_NAME as _DEFAULT_QUEUE_NAME,
    DEFAULT_SLOW_QUEUE_NAME as _DEFAULT_SLOW_QUEUE_NAME,
    QueueTopology,
)
from pybus.request_response import DEFAULT_REPLY_QUEUE
from pybus.retries import (
    RetryPolicy,
    next_retry_payload,
    resolve_last_attempt,
    resolve_retry_count,
)
from pybus.serializer import JsonSerializer

DEFAULT_QUEUE_NAME = _DEFAULT_QUEUE_NAME
DEFAULT_SLOW_QUEUE_NAME = _DEFAULT_SLOW_QUEUE_NAME
DEFAULT_FAILED_QUEUE_NAME = _DEFAULT_FAILED_QUEUE_NAME
MULTI_QUEUE_CONSUME_TIMEOUT = 1


class _Unset:
    pass


_UNSET = _Unset()


class _ClaimSettled:
    """Sentinel `_dispatch` returns when it already acked the claim itself
    (retry, dead-letter, continuation, or batch buffering), so `listen_once`
    doesn't redundantly ack the same claim again."""


_CLAIM_SETTLED = _ClaimSettled()


class Listener:
    def __init__(
        self,
        transport,
        dispatcher: Dispatcher | None = None,
        serializer: JsonSerializer | None = None,
        retry_policy: RetryPolicy | None = None,
        dead_letter_channel: str = DEFAULT_FAILED_QUEUE_NAME,
        now_fn: Callable[[], datetime] | None = None,
        topology: QueueTopology | None = None,
        sleep_fn: Callable[[float], None] | None = None,
        command_delivery_observers: Sequence[CommandDeliveryObserver] = (),
        durable_command_controller: DurableJobController | None | _Unset = _UNSET,
        durable_job_controller: DurableJobController | None | _Unset = _UNSET,
    ) -> None:
        if (
            durable_job_controller is not _UNSET
            and durable_command_controller is not _UNSET
        ):
            raise TypeError(
                "durable_job_controller and durable_command_controller "
                "cannot both be supplied"
            )
        resolved_controller = (
            durable_job_controller
            if durable_job_controller is not _UNSET
            else durable_command_controller
            if durable_command_controller is not _UNSET
            else None
        )
        self.transport = transport
        self.dispatcher = dispatcher or Dispatcher()
        self.serializer = serializer or JsonSerializer()
        self.retry_policy = retry_policy or RetryPolicy()
        self.dead_letter_channel = dead_letter_channel
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))
        self._topology = topology
        self._sleep_fn = sleep_fn or time.sleep
        self._command_delivery_observers = tuple(command_delivery_observers)
        self._durable_job_controller = resolved_controller
        self._durable_command_controller = resolved_controller
        if any(not callable(observer) for observer in self._command_delivery_observers):
            raise TypeError("command_delivery_observers must contain callables")
        self._logger = logging.getLogger("pybus.listener")
        self._batch_started_at: dict[str, datetime] = {}
        self._batch_retry_after: dict[str, datetime] = {}

    def listen_once(self, channel: str | Sequence[str]) -> object | None:
        self._flush_ready_batches()
        active_channels = self._active_channels(channel)
        if not active_channels:
            return None
        channel_name, envelope, receipt = self._consume_envelope(active_channels)
        if envelope is None:
            return None

        self._verify_durable_configuration(channel_name, envelope, receipt=receipt)

        try:
            result = self._dispatch(channel_name, envelope, receipt=receipt)
        except (IndeterminateDeliveryError, WorkerAbortError):
            raise
        except HandlerNotFoundError:
            self._dead_letter(
                channel_name,
                envelope,
                max_retries=self.retry_policy.max_retries,
                receipt=receipt,
            )
            return None
        except Exception:
            if self._should_retry(envelope):
                self._requeue(
                    channel_name,
                    envelope,
                    max_retries=self.retry_policy.max_retries,
                    receipt=receipt,
                )
            else:
                self._dead_letter(
                    channel_name,
                    envelope,
                    max_retries=self.retry_policy.max_retries,
                    receipt=receipt,
                )
            return None

        if result is _CLAIM_SETTLED:
            return None

        if envelope.message_kind == RequestMessage.message_kind and isinstance(
            result, ResponseMessage
        ):
            self._publish_response(envelope, result, receipt=receipt)
        else:
            self._ack_claim(receipt)
        return result

    def _verify_durable_configuration(
        self,
        channel_name: str,
        envelope: MessageEnvelope,
        *,
        receipt: str | None = None,
    ) -> None:
        try:
            identity = DurableJobController.identity(envelope)
        except Exception as exc:
            self._restore_claimed_command_and_abort(
                channel_name,
                envelope,
                reason="durable job metadata is invalid",
                error_type=WorkerAbortError,
                cause=exc,
                receipt=receipt,
            )
        if identity is not None and self._durable_job_controller is None:
            self._restore_claimed_command_and_abort(
                channel_name,
                envelope,
                reason="durable job store is not configured on this worker",
                error_type=WorkerAbortError,
                receipt=receipt,
            )
        if (
            identity is not None
            and envelope.message_kind != CommandMessage.message_kind
        ):
            self._restore_claimed_command_and_abort(
                channel_name,
                envelope,
                reason="durable delivery metadata is valid only on commands",
                error_type=WorkerAbortError,
                receipt=receipt,
            )

    def _admit_durable_delivery(
        self,
        channel_name: str,
        envelope: MessageEnvelope,
        *,
        max_retries: int,
        receipt: str | None = None,
    ) -> bool:
        if self._durable_job_controller is None:
            return True
        try:
            decision = self._durable_job_controller.admit(
                envelope,
                source_queue=channel_name,
                retry_count=self._retry_count(envelope),
                max_retries=max_retries,
            )
        except Exception as exc:
            self._restore_claimed_command_and_abort(
                channel_name,
                envelope,
                reason="durable job admission failed",
                error_type=WorkerAbortError,
                cause=exc,
                receipt=receipt,
            )
        if decision is None or decision == DurableDeliveryDecision.PROCEED:
            return True
        if decision == DurableDeliveryDecision.DROP:
            return False
        self._restore_claimed_command_and_abort(
            channel_name,
            envelope,
            reason="durable job admission could not be verified",
            error_type=WorkerAbortError,
            receipt=receipt,
        )
        return False

    def _active_channels(self, channel: str | Sequence[str]) -> tuple[str, ...]:
        channels = (channel,) if isinstance(channel, str) else tuple(channel)
        return tuple(
            channel_name
            for channel_name in channels
            if channel_name != self.dead_letter_channel
        )

    def listen(
        self,
        channel: str | Sequence[str],
        *,
        max_messages: int | None = None,
    ) -> list[object]:
        results: list[object] = []
        count = 0
        while max_messages is None or count < max_messages:
            result = self.listen_once(channel)
            if result is None:
                break
            results.append(result)
            count += 1
        return results

    def _consume_envelope(
        self, channel: str | Sequence[str]
    ) -> tuple[str | None, MessageEnvelope | None, str | None]:
        channels = (channel,) if isinstance(channel, str) else tuple(channel)
        consume_timeout = 5 if len(channels) == 1 else MULTI_QUEUE_CONSUME_TIMEOUT
        for channel_name in channels:
            raw_message = self.transport.consume(channel_name, timeout=consume_timeout)
            if raw_message is None:
                continue
            receipt = getattr(raw_message, "receipt", None)
            try:
                envelope = MessageEnvelope.from_dict(self.serializer.loads(raw_message))
            except Exception as exc:
                self._dead_letter_undecodable(
                    channel_name, raw_message, exc, receipt=receipt
                )
                return channel_name, None, None
            return channel_name, envelope, receipt
        return None, None, None

    def _dispatch(
        self,
        channel_name: str,
        envelope: MessageEnvelope,
        *,
        receipt: str | None = None,
    ) -> object | None:
        handlers = self.dispatcher.registry.handlers_for(
            envelope.message_kind,
            envelope.message_type,
        )
        durable_identity = DurableJobController.identity(envelope)
        is_durable = durable_identity is not None
        command_max_retries: int | None = None
        if envelope.message_kind == CommandMessage.message_kind and is_durable:
            if len(handlers) > 1:
                self._restore_claimed_command_and_abort(
                    channel_name,
                    envelope,
                    reason="durable jobs require exactly one command handler",
                    error_type=WorkerAbortError,
                    receipt=receipt,
                )
            command_max_retries = self.retry_policy.max_retries
            if handlers:
                command_spec = self._spec_for(handlers[0])
                if command_spec.retry_limit is not None:
                    command_max_retries = command_spec.retry_limit
            if not self._admit_durable_delivery(
                channel_name,
                envelope,
                max_retries=command_max_retries,
                receipt=receipt,
            ):
                return None

        if not handlers:
            raise HandlerNotFoundError(
                f"No handlers registered for {envelope.message_kind}:"
                f"{envelope.message_type}"
            )

        message = self.dispatcher.decode(envelope)

        if self._has_batched_handlers(handlers):
            self._buffer_batched_message(
                message.message_type, envelope, receipt=receipt
            )
            self._flush_ready_batches()
            return _CLAIM_SETTLED

        if envelope.message_kind == CommandMessage.message_kind and (
            self._command_delivery_observers or is_durable
        ):
            if len(handlers) != 1:
                self._restore_claimed_command_and_abort(
                    channel_name,
                    envelope,
                    reason="command delivery outcomes require exactly one handler",
                    error_type=WorkerAbortError,
                    receipt=receipt,
                )
            if command_max_retries is None:
                command_spec = self._spec_for(handlers[0])
                command_max_retries = (
                    self.retry_policy.max_retries
                    if command_spec.retry_limit is None
                    else command_spec.retry_limit
                )
            try:
                self._notify_command_outcome(
                    envelope,
                    status=CommandDeliveryStatus.STARTED,
                    source_queue=channel_name,
                    destination_queue=None,
                    retry_count=self._retry_count(envelope),
                    max_retries=command_max_retries,
                )
            except DeliveryObservationError as exc:
                if is_durable and self._durable_job_controller is not None:
                    try:
                        self._durable_job_controller.release(envelope)
                    except Exception as release_exc:
                        raise IndeterminateDeliveryError(
                            "durable job admission could not be released"
                        ) from release_exc
                self._restore_claimed_command_and_abort(
                    channel_name,
                    envelope,
                    reason="command delivery start observation failed",
                    error_type=DeliveryObservationError,
                    cause=exc,
                    receipt=receipt,
                )

        results: list[object] = []
        continued = False
        for handler in handlers:
            spec = self._spec_for(handler)
            if self._should_delay(envelope, spec):
                time.sleep(spec.delay)

            try:
                result = handler(message)
            except Exception:
                self._handle_failed_delivery(
                    channel_name, envelope, spec, receipt=receipt
                )
                return _CLAIM_SETTLED

            if isinstance(result, ContinueProcessing):
                self._continue_processing(
                    channel_name,
                    envelope,
                    result,
                    max_retries=(
                        self.retry_policy.max_retries
                        if spec.retry_limit is None
                        else spec.retry_limit
                    ),
                    receipt=receipt,
                )
                continued = True
                results.append(None)
                continue

            if isinstance(result, (ScheduleNextOccurrence, EndRecurrence)) and (
                envelope.message_kind != CommandMessage.message_kind or not is_durable
            ):
                self._handle_failed_delivery(
                    channel_name, envelope, spec, receipt=receipt
                )
                return _CLAIM_SETTLED

            results.append(result)

        if command_max_retries is not None and not continued:
            if is_durable and self._durable_job_controller is not None:
                outcome = self._command_delivery_outcome(
                    envelope,
                    status=CommandDeliveryStatus.SUCCEEDED,
                    source_queue=channel_name,
                    destination_queue=None,
                    retry_count=self._retry_count(envelope),
                    max_retries=command_max_retries,
                )
                try:
                    self._durable_job_controller.complete_success(
                        outcome,
                        results[0],
                        completed_at=self._now_fn(),
                    )
                except InvalidMessageDefinitionError:
                    self._handle_failed_delivery(
                        channel_name,
                        envelope,
                        self._spec_for(handlers[0]),
                        receipt=receipt,
                    )
                    return _CLAIM_SETTLED
                except Exception as exc:
                    self._logger.exception(
                        "Durable job success settlement failed for %s",
                        envelope.message_id,
                    )
                    raise DeliveryObservationError(
                        "durable job success settlement failed after handler completion"
                    ) from exc
            self._notify_command_outcome(
                envelope,
                status=CommandDeliveryStatus.SUCCEEDED,
                source_queue=channel_name,
                destination_queue=None,
                retry_count=self._retry_count(envelope),
                max_retries=command_max_retries,
                apply_durable=not is_durable,
            )

        if continued:
            return _CLAIM_SETTLED

        return results[0] if len(results) == 1 else results

    def _has_batched_handlers(self, handlers: Sequence[object]) -> bool:
        return any(self._spec_for(handler).is_batched for handler in handlers)

    def _spec_for(self, handler: object) -> HandlerSpec:
        spec = _handler_spec(handler)
        if spec is None:
            return HandlerSpec(message_kind="", message_type="")
        return spec

    def _should_delay(self, envelope: MessageEnvelope, spec: HandlerSpec) -> bool:
        if spec.delay <= 0:
            return False

        retry_payload = None if self._is_typed(envelope) else envelope.payload
        last_attempt = resolve_last_attempt(retry_payload, envelope.headers)
        if last_attempt is None:
            return False

        return (self._now_fn() - last_attempt).total_seconds() < spec.delay

    def _handle_failed_delivery(
        self,
        channel_name: str,
        envelope: MessageEnvelope,
        spec: HandlerSpec,
        *,
        receipt: str | None = None,
    ) -> None:
        max_retries = (
            self.retry_policy.max_retries
            if spec.retry_limit is None
            else spec.retry_limit
        )
        if self._retry_count(envelope) < max_retries:
            if spec.delay > 0:
                time.sleep(spec.delay)
            self._requeue(
                channel_name, envelope, max_retries=max_retries, receipt=receipt
            )
            return
        self._dead_letter(
            channel_name, envelope, max_retries=max_retries, receipt=receipt
        )

    def _continue_processing(
        self,
        channel_name: str,
        envelope: MessageEnvelope,
        continuation: ContinueProcessing,
        *,
        max_retries: int,
        receipt: str | None = None,
    ) -> None:
        target_queue = continuation.queue or channel_name
        if target_queue == self.dead_letter_channel:
            raise ValueError("dead-letter channel is terminal")
        if (
            continuation.queue is not None
            and target_queue != channel_name
            and self._topology is not None
            and not self._topology.has_queue(target_queue)
        ):
            raise ValueError(f"Unknown continuation queue: {target_queue}")
        if continuation.delay > 0:
            try:
                self._sleep_fn(continuation.delay)
            except Exception as exc:
                raise IndeterminateDeliveryError(
                    "continuation delay failed after message claim"
                ) from exc
        self._checkpoint_durable_settlement(
            envelope,
            status=CommandDeliveryStatus.CONTINUED,
            source_queue=channel_name,
            destination_queue=target_queue,
            retry_count=self._retry_count(envelope),
            max_retries=max_retries,
            receipt=receipt,
        )
        self._settle_after_claim(
            "continuation",
            lambda: self.transport.publish(
                target_queue, self.serializer.dump(envelope)
            ),
        )
        self._ack_claim(receipt)
        self._notify_command_outcome(
            envelope,
            status=CommandDeliveryStatus.CONTINUED,
            source_queue=channel_name,
            destination_queue=target_queue,
            retry_count=self._retry_count(envelope),
            max_retries=max_retries,
        )

    def _flush_ready_batches(self) -> None:
        if not hasattr(self.transport, "size") or not hasattr(
            self.transport, "consume_many"
        ):
            return

        registry = self.dispatcher.registry
        for (message_kind, message_type), handlers in registry.items():
            if message_kind != "event":
                continue
            for handler in handlers:
                spec = self._spec_for(handler)
                if not spec.is_batched:
                    continue
                if self._batch_in_backoff(message_type):
                    continue
                buffer_key = batched_buffer_key(message_type)
                buffer_size = self.transport.size(buffer_key)
                if buffer_size == 0:
                    self._batch_started_at.pop(message_type, None)
                    continue
                started_at = self._batch_started_at.setdefault(
                    message_type,
                    self._now_fn(),
                )
                ready = (
                    buffer_size >= spec.batch_size
                    or (self._now_fn() - started_at).total_seconds() >= spec.max_wait
                )
                if ready:
                    self._flush_batch(message_type, handler, spec)

    def _batch_in_backoff(self, message_type: str) -> bool:
        retry_at = self._batch_retry_after.get(message_type)
        return retry_at is not None and self._now_fn() < retry_at

    def _buffer_batched_message(
        self,
        message_type: str,
        envelope: MessageEnvelope,
        *,
        receipt: str | None = None,
    ) -> None:
        if not hasattr(self.transport, "size"):
            return
        buffer_key = batched_buffer_key(message_type)
        if self.transport.size(buffer_key) == 0:
            self._batch_started_at[message_type] = self._now_fn()
        self._settle_after_claim(
            "batch buffering",
            lambda: self.transport.publish(buffer_key, self.serializer.dump(envelope)),
        )
        self._ack_claim(receipt)

    def _flush_batch(
        self,
        message_type: str,
        handler: object,
        spec: HandlerSpec,
    ) -> None:
        buffer_key = batched_buffer_key(message_type)
        raw_messages = self.transport.consume_many(buffer_key, spec.batch_size)
        if not raw_messages:
            return

        envelopes: list[MessageEnvelope] = []
        receipts: list[str | None] = []
        events: list[object] = []
        for raw_message in raw_messages:
            receipt = getattr(raw_message, "receipt", None)
            try:
                envelope = MessageEnvelope.from_dict(self.serializer.loads(raw_message))
                event = self.dispatcher.decode(envelope)
            except Exception as exc:
                self._dead_letter_undecodable(
                    buffer_key, raw_message, exc, receipt=receipt
                )
                continue
            envelopes.append(envelope)
            receipts.append(receipt)
            events.append(event)

        if not events:
            self._batch_started_at.pop(message_type, None)
            self._batch_retry_after.pop(message_type, None)
            return
        try:
            handler(events)
        except Exception:
            requeued = False
            for envelope, receipt in zip(envelopes, receipts):
                max_retries = (
                    self.retry_policy.max_retries
                    if spec.retry_limit is None
                    else spec.retry_limit
                )
                if self._retry_count(envelope) < max_retries:
                    self._requeue(
                        buffer_key, envelope, max_retries=max_retries, receipt=receipt
                    )
                    requeued = True
                else:
                    self._dead_letter(
                        buffer_key, envelope, max_retries=max_retries, receipt=receipt
                    )
            if requeued:
                self._batch_retry_after[message_type] = self._now_fn() + timedelta(
                    seconds=spec.max_wait
                )
            else:
                self._batch_retry_after.pop(message_type, None)
            return

        for receipt in receipts:
            self._ack_claim(receipt)
        self._batch_started_at.pop(message_type, None)
        self._batch_retry_after.pop(message_type, None)

    def _should_retry(self, envelope: MessageEnvelope) -> bool:
        return self.retry_policy.should_retry(self._retry_count(envelope))

    def _retry_count(self, envelope: MessageEnvelope) -> int:
        retry_payload = None if self._is_typed(envelope) else envelope.payload
        return resolve_retry_count(retry_payload, envelope.headers)

    def _is_typed(self, envelope: MessageEnvelope) -> bool:
        return (
            self.dispatcher.registry.message_class_for(
                envelope.message_kind,
                envelope.message_type,
            )
            is not None
        )

    def _requeue(
        self,
        channel_name: str,
        envelope: MessageEnvelope,
        *,
        max_retries: int,
        receipt: str | None = None,
    ) -> None:
        # Blocks the listener for the configured delay. On multi-channel listeners
        # this stalls all channels for the duration; keep delay values small.
        retries = self._retry_count(envelope)
        delay = self.retry_policy.next_delay(retries)
        if delay > 0:
            self._sleep_fn(delay)
        now = self._now_fn()
        if not self._is_typed(envelope) and isinstance(envelope.payload, dict):
            payload = next_retry_payload(
                envelope.payload,
                attempted_at=now,
                retries=retries,
            )
        else:
            payload = envelope.payload
        headers = dict(envelope.headers)
        headers["retries"] = retries + 1
        headers["last_attempt"] = now.isoformat()
        self._checkpoint_durable_settlement(
            envelope,
            status=CommandDeliveryStatus.RETRY_SCHEDULED,
            source_queue=channel_name,
            destination_queue=channel_name,
            retry_count=retries + 1,
            max_retries=max_retries,
            last_attempt_at=now,
            receipt=receipt,
        )
        self._settle_after_claim(
            "retry requeue",
            lambda: self.transport.publish(
                channel_name,
                self.serializer.dump(
                    MessageEnvelope.create(
                        message_id=envelope.message_id,
                        message_type=envelope.message_type,
                        message_kind=envelope.message_kind,
                        version=envelope.version,
                        payload=payload,
                        headers=headers,
                        created_at=envelope.created_at,
                        correlation_id=envelope.correlation_id,
                        causation_id=envelope.causation_id,
                        reply_to=envelope.reply_to,
                        expires_at=envelope.expires_at,
                        content_type=envelope.content_type,
                        content_encoding=envelope.content_encoding,
                    )
                ),
            ),
        )
        self._ack_claim(receipt)
        self._notify_command_outcome(
            envelope,
            status=CommandDeliveryStatus.RETRY_SCHEDULED,
            source_queue=channel_name,
            destination_queue=channel_name,
            retry_count=retries + 1,
            max_retries=max_retries,
        )

    def _dead_letter(
        self,
        channel_name: str,
        envelope: MessageEnvelope,
        *,
        max_retries: int,
        receipt: str | None = None,
    ) -> None:
        headers = dict(envelope.headers)
        headers["dead_lettered_from"] = channel_name
        self._checkpoint_durable_settlement(
            envelope,
            status=CommandDeliveryStatus.DEAD_LETTERED,
            source_queue=channel_name,
            destination_queue=self.dead_letter_channel,
            retry_count=self._retry_count(envelope),
            max_retries=max_retries,
            receipt=receipt,
        )
        self._settle_after_claim(
            "dead-letter publication",
            lambda: self.transport.publish(
                self.dead_letter_channel,
                self.serializer.dump(
                    MessageEnvelope.create(
                        message_id=envelope.message_id,
                        message_type=envelope.message_type,
                        message_kind=envelope.message_kind,
                        version=envelope.version,
                        payload=envelope.payload,
                        headers=headers,
                        created_at=envelope.created_at,
                        correlation_id=envelope.correlation_id,
                        causation_id=envelope.causation_id,
                        reply_to=envelope.reply_to,
                        expires_at=envelope.expires_at,
                        content_type=envelope.content_type,
                        content_encoding=envelope.content_encoding,
                    )
                ),
            ),
        )
        self._ack_claim(receipt)
        self._notify_command_outcome(
            envelope,
            status=CommandDeliveryStatus.DEAD_LETTERED,
            source_queue=channel_name,
            destination_queue=self.dead_letter_channel,
            retry_count=self._retry_count(envelope),
            max_retries=max_retries,
        )

    def _notify_command_outcome(
        self,
        envelope: MessageEnvelope,
        *,
        status: CommandDeliveryStatus,
        source_queue: str,
        destination_queue: str | None,
        retry_count: int,
        max_retries: int,
        apply_durable: bool = True,
    ) -> None:
        if envelope.message_kind != CommandMessage.message_kind:
            return
        outcome = self._command_delivery_outcome(
            envelope,
            status=status,
            source_queue=source_queue,
            destination_queue=destination_queue,
            retry_count=retry_count,
            max_retries=max_retries,
        )
        failures: list[Exception] = []
        if apply_durable and self._durable_job_controller is not None:
            try:
                self._durable_job_controller.apply_outcome(outcome)
            except Exception as exc:
                failures.append(exc)
                self._logger.exception(
                    "Durable job outcome failed for %s", envelope.message_id
                )
        for observer in self._command_delivery_observers:
            try:
                observer(outcome)
            except Exception as exc:
                failures.append(exc)
                self._logger.exception(
                    "Command delivery observer failed for %s", envelope.message_id
                )
        if failures:
            raise DeliveryObservationError(
                "command delivery observer failed after a known message settlement"
            ) from failures[0]

    @staticmethod
    def _command_delivery_outcome(
        envelope: MessageEnvelope,
        *,
        status: CommandDeliveryStatus,
        source_queue: str,
        destination_queue: str | None,
        retry_count: int,
        max_retries: int,
    ) -> CommandDeliveryOutcome:
        durable_record_id = envelope.headers.get("pybus_durable_record")
        durable_generation = envelope.headers.get("pybus_durable_generation")
        if durable_record_id is not None and not isinstance(durable_record_id, str):
            durable_record_id = None
        if durable_generation is not None and not isinstance(durable_generation, int):
            durable_generation = None
        return CommandDeliveryOutcome(
            status=status,
            message_id=envelope.message_id,
            message_type=envelope.message_type,
            version=envelope.version,
            source_queue=source_queue,
            destination_queue=destination_queue,
            retry_count=retry_count,
            max_retries=max_retries,
            durable_record_id=durable_record_id,
            durable_generation=durable_generation,
        )

    def _checkpoint_durable_settlement(
        self,
        envelope: MessageEnvelope,
        *,
        status: CommandDeliveryStatus,
        source_queue: str,
        destination_queue: str | None,
        retry_count: int,
        max_retries: int,
        last_attempt_at: datetime | None = None,
        receipt: str | None = None,
    ) -> None:
        if self._durable_job_controller is None:
            return
        try:
            self._durable_job_controller.checkpoint(
                envelope,
                status=status,
                source_queue=source_queue,
                destination_queue=destination_queue,
                retry_count=retry_count,
                max_retries=max_retries,
                last_attempt_at=last_attempt_at,
            )
        except Exception as exc:
            self._restore_claimed_command_and_abort(
                source_queue,
                envelope,
                reason="durable job settlement checkpoint failed",
                error_type=WorkerAbortError,
                cause=exc,
                receipt=receipt,
            )

    def _restore_claimed_command_and_abort(
        self,
        channel_name: str,
        envelope: MessageEnvelope,
        *,
        reason: str,
        error_type: type[WorkerAbortError],
        cause: Exception | None = None,
        receipt: str | None = None,
    ) -> None:
        self._settle_after_claim(
            "command claim recovery",
            lambda: self.transport.publish(
                channel_name,
                self.serializer.dump(envelope),
            ),
        )
        self._ack_claim(receipt)
        error = error_type(
            f"{reason}; the unchanged command was restored before handler execution"
        )
        if cause is None:
            raise error
        raise error from cause

    def _dead_letter_undecodable(
        self,
        channel_name: str,
        raw_message: bytes | str,
        exception: Exception,
        *,
        receipt: str | None = None,
    ) -> None:
        raw_bytes = (
            raw_message.encode("utf-8") if isinstance(raw_message, str) else raw_message
        )
        poison = MessageEnvelope.create(
            message_type="pybus.message.decode_failed",
            message_kind="event",
            version=1,
            payload={
                "raw_message": base64.b64encode(raw_bytes).decode("ascii"),
                "raw_message_encoding": "base64",
                "error_type": type(exception).__name__,
            },
            headers={"dead_lettered_from": channel_name},
        )
        self._settle_after_claim(
            "decode-failure publication",
            lambda: self.transport.publish(
                self.dead_letter_channel, self.serializer.dump(poison)
            ),
        )
        self._ack_claim(receipt)

    def _publish_response(
        self,
        request_envelope: MessageEnvelope,
        response: ResponseMessage,
        *,
        receipt: str | None = None,
    ) -> None:
        def publish_response() -> None:
            reply_queue = request_envelope.reply_to or DEFAULT_REPLY_QUEUE
            codec = self.dispatcher.payload_codec
            encoded_headers = codec.encode(response.headers)
            response_envelope = MessageEnvelope.create(
                message_type=response.message_type,
                message_kind=response.message_kind,
                version=response.version,
                payload=codec.encode(response.payload, context=response.headers),
                headers=encoded_headers,
                created_at=datetime.now(timezone.utc),
                correlation_id=request_envelope.correlation_id,
                causation_id=request_envelope.message_id,
                reply_to=reply_queue,
                expires_at=response.expires_at,
                content_type=response.content_type,
                content_encoding=response.content_encoding,
            )
            self.transport.publish(reply_queue, self.serializer.dump(response_envelope))

        self._settle_after_claim("response publication", publish_response)
        self._ack_claim(receipt)

    def _settle_after_claim(
        self,
        action: str,
        operation: Callable[[], None],
    ) -> None:
        try:
            operation()
        except IndeterminateDeliveryError:
            raise
        except Exception as exc:
            raise IndeterminateDeliveryError(
                f"{action} failed after a message was claimed from the transport"
            ) from exc

    def _ack_claim(self, receipt: str | None) -> None:
        if receipt is None:
            return
        try:
            self.transport.ack(receipt)
        except IndeterminateDeliveryError:
            raise
        except Exception as exc:
            raise IndeterminateDeliveryError(
                "claim ack failed after a message outcome was durably published"
            ) from exc
