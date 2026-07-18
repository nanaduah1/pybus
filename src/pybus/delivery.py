from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum


class CommandDeliveryStatus(StrEnum):
    STARTED = "started"
    SUCCEEDED = "succeeded"
    CONTINUED = "continued"
    RETRY_SCHEDULED = "retry_scheduled"
    DEAD_LETTERED = "dead_lettered"


@dataclass(frozen=True, slots=True)
class CommandDeliveryOutcome:
    status: CommandDeliveryStatus
    message_id: str
    message_type: str
    version: int
    source_queue: str
    destination_queue: str | None
    retry_count: int
    max_retries: int
    durable_record_id: str | None = None
    durable_generation: int | None = None


CommandDeliveryObserver = Callable[[CommandDeliveryOutcome], None]


__all__ = [
    "CommandDeliveryObserver",
    "CommandDeliveryOutcome",
    "CommandDeliveryStatus",
]
