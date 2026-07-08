from __future__ import annotations

from collections import defaultdict, deque
from typing import Deque


class MemoryTransport:
    def __init__(self) -> None:
        self._queues: dict[str, Deque[bytes]] = defaultdict(deque)

    def publish(self, channel: str, message: bytes) -> None:
        self._queues[channel].append(message)

    def consume(self, channel: str, timeout: int = 5) -> bytes | None:
        queue = self._queues[channel]
        if not queue:
            return None
        return queue.popleft()

    def ack(self, receipt: str) -> None:
        return None

    def nack(self, receipt: str, *, requeue: bool = True) -> None:
        return None

    def size(self, channel: str) -> int:
        return len(self._queues[channel])
