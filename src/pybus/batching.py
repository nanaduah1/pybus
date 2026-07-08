from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Generic, TypeVar

DEFAULT_BATCH_SIZE = 100
DEFAULT_MAX_WAIT = 10

T = TypeVar("T")


def batched_buffer_key(event_type: str) -> str:
    return f"batched:{event_type}"


@dataclass
class BatchBuffer(Generic[T]):
    event_type: str
    batch_size: int = DEFAULT_BATCH_SIZE
    max_wait: int = DEFAULT_MAX_WAIT
    items: list[T] = field(default_factory=list)
    first_item_at: datetime | None = None

    def add(self, item: T, *, now: datetime | None = None) -> bool:
        now = now or datetime.now(timezone.utc)
        if self.first_item_at is None:
            self.first_item_at = now
        self.items.append(item)
        return self.ready(now=now)

    def ready(self, *, now: datetime | None = None) -> bool:
        if len(self.items) >= self.batch_size:
            return True
        if self.first_item_at is None:
            return False
        now = now or datetime.now(timezone.utc)
        return now - self.first_item_at >= timedelta(seconds=self.max_wait)

    def flush(self) -> list[T]:
        items = list(self.items)
        self.items.clear()
        self.first_item_at = None
        return items
