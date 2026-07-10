from __future__ import annotations

from importlib import import_module
from typing import Any, Callable

from pybus.queues import DEFAULT_QUEUE_NAME


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
