from __future__ import annotations

from importlib import import_module
from typing import Any, Callable

from pybus.queues import DEFAULT_QUEUE_NAME
from pybus.worker import Worker, WorkerHook


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
        self._transaction = transaction_module or self._load_transaction_module()
        self.default_queue = default_queue

    @staticmethod
    def _load_transaction_module() -> Any:
        try:
            return import_module("django.db.transaction")
        except ModuleNotFoundError as exc:  # pragma: no cover - import safety
            raise RuntimeError(
                "Django is required to use DjangoBusAdapter without injection"
            ) from exc

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
