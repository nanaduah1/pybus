from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Deque
from uuid import uuid4

from pybus.contracts import OutboxStore


@dataclass
class OutboxRecord:
    receipt: str
    message: bytes
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    claimed_at: datetime | None = None
    dispatched_at: datetime | None = None


class InMemoryOutboxStore(OutboxStore):
    def __init__(self) -> None:
        self._queue: Deque[OutboxRecord] = deque()
        self._records: dict[str, OutboxRecord] = {}

    def add(self, message: bytes) -> str:
        receipt = str(uuid4())
        record = OutboxRecord(receipt=receipt, message=message)
        self._queue.append(record)
        self._records[receipt] = record
        return receipt

    def claim(self, limit: int = 100) -> list[bytes]:
        claimed: list[bytes] = []
        for _ in range(min(limit, len(self._queue))):
            record = self._queue.popleft()
            record.claimed_at = datetime.now(timezone.utc)
            claimed.append(record.message)
        return claimed

    def complete(self, receipt: str) -> None:
        record = self._records.get(receipt)
        if record is not None:
            record.dispatched_at = datetime.now(timezone.utc)
