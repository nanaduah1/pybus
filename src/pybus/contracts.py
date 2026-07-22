from __future__ import annotations

from typing import Protocol


class Transport(Protocol):
    def publish(self, channel: str, message: bytes) -> None: ...

    def consume(self, channel: str, timeout: int = 5) -> bytes | None:
        """Claim and return one message, or `None` on timeout.

        A backend that supports recoverable claims (e.g. `RedisTransport`) should
        return a `bytes` value that also carries whatever the backend needs to
        build a receipt for `ack`/`nack`, without changing its behavior as plain
        bytes for callers that only read the payload.
        """
        ...

    def ack(self, receipt: str) -> None:
        """Settle a claimed message. An unknown or already-settled receipt is a
        no-op, not an error; a failed settlement attempt may still raise."""
        ...

    def nack(self, receipt: str, *, requeue: bool = True) -> None:
        """Settle a claimed message; if `requeue`, make it available for
        redelivery. An unknown or already-settled receipt is a no-op, not an
        error; a failed settlement attempt may still raise."""
        ...


class OutboxStore(Protocol):
    def add(self, message: bytes) -> str: ...

    def claim(self, limit: int = 100) -> list[bytes]: ...

    def complete(self, receipt: str) -> None: ...


class InboxStore(Protocol):
    def seen(self, message_id: str) -> bool: ...

    def record(self, message_id: str) -> None: ...
