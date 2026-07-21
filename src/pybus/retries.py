from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

DEFAULT_RETRY_LIMIT = 10
DEFAULT_RETRY_DELAY = 0.0


@dataclass(frozen=True)
class RetryPolicy:
    """Controls retry behaviour in Listener.

    max_retries: maximum requeue attempts before dead-lettering.
    delay: base sleep in seconds before each requeue; 0 = immediate (default).
    backoff_factor: multiplier applied per attempt — sleep = delay * (backoff_factor ** attempt).
    """

    max_retries: int = DEFAULT_RETRY_LIMIT
    delay: float = DEFAULT_RETRY_DELAY
    backoff_factor: float = 1.0

    def should_retry(self, retries: int) -> bool:
        return retries < self.max_retries

    def next_delay(self, retries: int) -> float:
        return self.delay * (self.backoff_factor**retries)


@dataclass(frozen=True)
class RetryState:
    retries: int = 0
    last_attempt: datetime | None = None
    first_attempt: datetime | None = None

    def bump(self, *, attempted_at: datetime | None = None) -> "RetryState":
        attempted_at = attempted_at or datetime.now(timezone.utc)
        return RetryState(
            retries=self.retries + 1,
            last_attempt=attempted_at,
            first_attempt=self.first_attempt or attempted_at,
        )


def next_retry_payload(
    payload: dict[str, Any],
    *,
    attempted_at: datetime | None = None,
) -> dict[str, Any]:
    attempted_at = attempted_at or datetime.now(timezone.utc)
    updated = dict(payload)
    updated["retries"] = int(updated.get("retries", 0)) + 1
    updated["last_attempt"] = attempted_at.isoformat()
    return updated
