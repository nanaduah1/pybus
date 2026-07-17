from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from typing import Any, Callable

from pybus.bus import get_bus
from pybus.composition import BusConfiguration as CoreBusConfiguration
from pybus.envelope import MessageEnvelope
from pybus.queues import DEFAULT_QUEUE_NAME
from pybus.worker import Worker, WorkerHook


def _load_transaction_module() -> Any:
    try:
        return import_module("django.db.transaction")
    except ModuleNotFoundError as exc:  # pragma: no cover - import safety
        raise RuntimeError(
            "Django is required for transaction-aware publishing"
        ) from exc


class DjangoConnectionCleanupHook(WorkerHook):
    """Close obsolete Django connections around worker polling."""

    def __init__(
        self,
        *,
        close_connections_fn: Callable[[], None] | None = None,
    ) -> None:
        self._close_connections = close_connections_fn or self._load_close_connections()

    @staticmethod
    def _load_close_connections() -> Callable[[], None]:
        try:
            django_db = import_module("django.db")
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Django is required to use DjangoConnectionCleanupHook"
            ) from exc
        return django_db.close_old_connections

    def before_poll(self, worker: Worker) -> None:
        self._close_connections()

    def after_poll(self, worker: Worker, result: object | None) -> None:
        self._close_connections()

    def on_stop(self, worker: Worker) -> None:
        self._close_connections()


@dataclass(frozen=True, slots=True)
class BusConfiguration(CoreBusConfiguration):
    """Django composition with connection cleanup enabled by default."""

    def __post_init__(self) -> None:
        if self.worker_hook_factories is None:
            object.__setattr__(
                self,
                "worker_hook_factories",
                (DjangoConnectionCleanupHook,),
            )
        CoreBusConfiguration.__post_init__(self)


class DjangoBusAdapter:
    """Schedule publications with Django transaction semantics."""

    def __init__(
        self,
        publish_fn: Callable[[str, Any], Any],
        *,
        transaction_module: Any | None = None,
        default_queue: str = DEFAULT_QUEUE_NAME,
    ) -> None:
        self._publish_fn = publish_fn
        self._transaction = transaction_module or _load_transaction_module()
        self.default_queue = default_queue

    def schedule(
        self,
        event_type: str,
        data: Any,
        *,
        queue: str | None = None,
        publish_on_commit: bool = True,
    ) -> Any:
        target_queue = queue or self.default_queue
        if publish_on_commit and self._transaction.get_connection().in_atomic_block:
            self._transaction.on_commit(
                lambda: self._publish_fn(event_type, data, queue=target_queue)
            )
            return None
        return self._publish_fn(event_type, data, queue=target_queue)

    def publish(
        self,
        event_type: str,
        data: Any,
        *,
        queue: str | None = None,
        publish_on_commit: bool = True,
    ) -> Any:
        return self.schedule(
            event_type,
            data,
            queue=queue,
            publish_on_commit=publish_on_commit,
        )


def _publish_message(
    message: object,
    *,
    expected_kind: str,
    queue: str | None,
    message_id: str | None,
    headers: Mapping[str, object] | None,
) -> MessageEnvelope | None:
    bus = get_bus()
    envelope = bus._prepare_message(
        message,
        expected_kind=expected_kind,
        message_id=message_id,
        headers=headers,
    )
    target_queue = bus._resolve_publication_queue(message, queue)

    def callback() -> MessageEnvelope:
        return bus._publish_envelope(envelope, queue=target_queue)

    transaction = _load_transaction_module()
    if transaction.get_connection().in_atomic_block:
        transaction.on_commit(callback)
        return None
    return callback()


def publish_event(
    event: object,
    *,
    queue: str | None = None,
    message_id: str | None = None,
    headers: Mapping[str, object] | None = None,
) -> MessageEnvelope | None:
    """Django transaction-aware override of :func:`pybus.publish_event`."""
    return _publish_message(
        event,
        expected_kind="event",
        queue=queue,
        message_id=message_id,
        headers=headers,
    )


def send_command(
    command: object,
    *,
    queue: str | None = None,
    message_id: str | None = None,
    headers: Mapping[str, object] | None = None,
) -> MessageEnvelope | None:
    """Django transaction-aware override of :func:`pybus.send_command`."""
    return _publish_message(
        command,
        expected_kind="command",
        queue=queue,
        message_id=message_id,
        headers=headers,
    )


def prepare_event(
    event: object,
    *,
    message_id: str | None = None,
    headers: Mapping[str, object] | None = None,
) -> MessageEnvelope:
    """Prepare an event using the same contract as :func:`pybus.prepare_event`."""
    return get_bus().prepare_event(event, message_id=message_id, headers=headers)


def prepare_command(
    command: object,
    *,
    message_id: str | None = None,
    headers: Mapping[str, object] | None = None,
) -> MessageEnvelope:
    """Prepare a command using the same contract as :func:`pybus.prepare_command`."""
    return get_bus().prepare_command(command, message_id=message_id, headers=headers)


def publish_prepared(
    envelope: MessageEnvelope,
    *,
    queue: str | None = None,
) -> MessageEnvelope | None:
    """Publish an exact prepared envelope after the current transaction commits."""
    bus = get_bus()
    bus._validate_prepared_envelope(envelope)
    prepared = MessageEnvelope.from_dict(envelope.to_dict())
    target_queue = bus._resolve_prepared_queue(queue)

    def callback() -> MessageEnvelope:
        return bus.publish_prepared(prepared, queue=target_queue)

    transaction = _load_transaction_module()
    if transaction.get_connection().in_atomic_block:
        transaction.on_commit(callback)
        return None
    return callback()
