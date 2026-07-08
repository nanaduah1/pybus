from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from pybus.contracts import InboxStore
from pybus.exceptions import DuplicateMessageError


@dataclass
class InboxEntry:
    message_id: str
    seen_at: datetime


class InMemoryInboxStore(InboxStore):
    def __init__(self, *, ttl_seconds: int | None = None) -> None:
        self.ttl_seconds = ttl_seconds
        self._entries: dict[str, InboxEntry] = {}

    def _prune(self) -> None:
        if self.ttl_seconds is None:
            return
        expired_before = datetime.now(timezone.utc) - timedelta(
            seconds=self.ttl_seconds
        )
        self._entries = {
            message_id: entry
            for message_id, entry in self._entries.items()
            if entry.seen_at >= expired_before
        }

    def seen(self, message_id: str) -> bool:
        self._prune()
        return message_id in self._entries

    def record(self, message_id: str) -> None:
        self._prune()
        if message_id in self._entries:
            raise DuplicateMessageError(f"Message already processed: {message_id}")
        self._entries[message_id] = InboxEntry(
            message_id=message_id,
            seen_at=datetime.now(timezone.utc),
        )
