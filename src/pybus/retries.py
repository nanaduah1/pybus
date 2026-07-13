from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import math
from typing import Any

DEFAULT_RETRY_LIMIT = 10
DEFAULT_RETRY_DELAY = 0


@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int = DEFAULT_RETRY_LIMIT
    delay: int = DEFAULT_RETRY_DELAY
    backoff_factor: float = 1.0

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_retries, bool)
            or not isinstance(self.max_retries, int)
            or self.max_retries < 0
        ):
            raise ValueError("max_retries must be a non-negative integer")
        if isinstance(self.delay, bool) or not isinstance(self.delay, (int, float)):
            raise ValueError("delay must be a non-negative number")
        if not math.isfinite(self.delay) or self.delay < 0:
            raise ValueError("delay must be a non-negative number")
        if isinstance(self.backoff_factor, bool) or not isinstance(
            self.backoff_factor, (int, float)
        ):
            raise ValueError("backoff_factor must be a non-negative number")
        if not math.isfinite(self.backoff_factor) or self.backoff_factor < 0:
            raise ValueError("backoff_factor must be a non-negative number")

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
    retries: int | None = None,
) -> dict[str, Any]:
    attempted_at = attempted_at or datetime.now(timezone.utc)
    updated = dict(payload)
    if retries is not None:
        current_retries = retries
    else:
        current_retries = _valid_retry_count(updated.get("retries")) or 0
    updated["retries"] = (current_retries or 0) + 1
    updated["last_attempt"] = attempted_at.isoformat()
    return updated


def resolve_retry_count(
    payload: object,
    headers: Mapping[str, object],
) -> int:
    """Resolve framework retry state without allowing stale metadata to reset it."""

    header_retries = _valid_retry_count(headers.get("retries"))
    if header_retries is not None:
        return header_retries
    if isinstance(payload, Mapping):
        return _valid_retry_count(payload.get("retries")) or 0
    return 0


def resolve_last_attempt(
    payload: object,
    headers: Mapping[str, object],
) -> datetime | None:
    header_attempt = _valid_datetime(headers.get("last_attempt"))
    if header_attempt is not None:
        return header_attempt
    if isinstance(payload, Mapping):
        return _valid_datetime(payload.get("last_attempt"))
    return None


def _valid_retry_count(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = int(value)
        except (TypeError, ValueError, OverflowError):
            return None
    else:
        return None
    if parsed < 0:
        return None
    return parsed


def _valid_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed
