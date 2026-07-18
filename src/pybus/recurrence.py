from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol, runtime_checkable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

if TYPE_CHECKING:
    from pybus.delivery import CommandDeliveryOutcome
    from pybus.durable import DurableCommandDraft, DurableCommandRecord


class RecurrenceCadence(StrEnum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class RecurringCommandSeriesState(StrEnum):
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class Recurrence:
    cadence: RecurrenceCadence
    timezone: str = "UTC"
    ends_at: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.cadence, RecurrenceCadence):
            raise TypeError("cadence must be a RecurrenceCadence")
        _zone(self.timezone)
        if self.ends_at is not None:
            _require_aware(self.ends_at, label="ends_at")


@dataclass(frozen=True, slots=True)
class ScheduleNextOccurrence:
    at: datetime

    def __post_init__(self) -> None:
        _require_aware(self.at, label="next occurrence")


@dataclass(frozen=True, slots=True)
class EndRecurrence:
    pass


@dataclass(frozen=True, slots=True)
class RecurringCommandSeriesDraft:
    first_occurrence: DurableCommandDraft
    recurrence: Recurrence
    starts_at: datetime
    fingerprint: str
    idempotency_key: str | None = None


@dataclass(frozen=True, slots=True)
class RecurringCommandSeriesRecord:
    id: str
    state: RecurringCommandSeriesState
    cadence: RecurrenceCadence
    timezone: str
    starts_at: datetime
    ends_at: datetime | None
    fingerprint: str
    idempotency_key: str | None
    latest_occurrence_number: int
    latest_occurrence_id: str
    latest_message_id: str
    latest_run_at: datetime
    created_at: datetime
    finished_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class RecurringCommandSeriesHandle:
    id: str
    state: RecurringCommandSeriesState
    occurrence_id: str
    message_id: str
    occurrence_number: int
    run_at: datetime
    created_at: datetime

    @classmethod
    def from_record(
        cls, record: RecurringCommandSeriesRecord
    ) -> RecurringCommandSeriesHandle:
        return cls(
            id=record.id,
            state=record.state,
            occurrence_id=record.latest_occurrence_id,
            message_id=record.latest_message_id,
            occurrence_number=record.latest_occurrence_number,
            run_at=record.latest_run_at,
            created_at=record.created_at,
        )


@runtime_checkable
class RecurringCommandStore(Protocol):
    def schedule_recurring(
        self, draft: RecurringCommandSeriesDraft
    ) -> tuple[RecurringCommandSeriesRecord, DurableCommandRecord]: ...

    def cancel_recurring(
        self, *, series_id: str, cancelled_at: datetime
    ) -> RecurringCommandSeriesRecord: ...

    def complete_recurring_success(
        self,
        outcome: CommandDeliveryOutcome,
        handler_result: object,
        *,
        completed_at: datetime,
    ) -> bool: ...


def next_cadence_at(
    recurrence: Recurrence,
    *,
    starts_at: datetime,
    after: datetime,
) -> datetime | None:
    _require_aware(starts_at, label="starts_at")
    _require_aware(after, label="after")
    zone = _zone(recurrence.timezone)
    anchor = starts_at.astimezone(zone)
    after_utc = after.astimezone(timezone.utc)
    index = _initial_index(recurrence.cadence, anchor, after.astimezone(zone))

    while True:
        local_candidate = _candidate_local(anchor, recurrence.cadence, index)
        candidate = _resolve_local(local_candidate, zone).astimezone(timezone.utc)
        if candidate > after_utc:
            if (
                recurrence.ends_at is not None
                and candidate >= recurrence.ends_at.astimezone(timezone.utc)
            ):
                return None
            return candidate
        index += 1


def _initial_index(
    cadence: RecurrenceCadence,
    anchor: datetime,
    after: datetime,
) -> int:
    if cadence == RecurrenceCadence.DAILY:
        return max(1, (after.date() - anchor.date()).days)
    if cadence == RecurrenceCadence.WEEKLY:
        days = (after.date() - anchor.date()).days
        return max(1, days // 7)
    months = (after.year - anchor.year) * 12 + after.month - anchor.month
    return max(1, months)


def _candidate_local(
    anchor: datetime,
    cadence: RecurrenceCadence,
    index: int,
) -> datetime:
    local_anchor = anchor.replace(tzinfo=None)
    if cadence == RecurrenceCadence.DAILY:
        return local_anchor + timedelta(days=index)
    if cadence == RecurrenceCadence.WEEKLY:
        return local_anchor + timedelta(weeks=index)
    month_index = anchor.year * 12 + anchor.month - 1 + index
    year, zero_based_month = divmod(month_index, 12)
    month = zero_based_month + 1
    day = min(anchor.day, calendar.monthrange(year, month)[1])
    return local_anchor.replace(year=year, month=month, day=day)


def _resolve_local(value: datetime, zone: ZoneInfo) -> datetime:
    first = value.replace(tzinfo=zone, fold=0)
    second = value.replace(tzinfo=zone, fold=1)
    first_round_trip = first.astimezone(timezone.utc).astimezone(zone)
    second_round_trip = second.astimezone(timezone.utc).astimezone(zone)
    first_valid = first_round_trip.replace(tzinfo=None) == value
    second_valid = second_round_trip.replace(tzinfo=None) == value

    if first_valid:
        return first
    if second_valid:
        return second
    candidates = [
        candidate
        for candidate in (first_round_trip, second_round_trip)
        if candidate.replace(tzinfo=None) > value
    ]
    if not candidates:
        raise ValueError("unable to resolve recurrence time in timezone")
    return min(candidates, key=lambda candidate: candidate.astimezone(timezone.utc))


def _zone(name: str) -> ZoneInfo:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("timezone must be a non-empty IANA timezone name")
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"unknown IANA timezone: {name}") from exc


def _require_aware(value: datetime, *, label: str) -> None:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError(f"{label} must be timezone-aware")


__all__ = [
    "EndRecurrence",
    "Recurrence",
    "RecurrenceCadence",
    "RecurringCommandSeriesDraft",
    "RecurringCommandSeriesHandle",
    "RecurringCommandSeriesRecord",
    "RecurringCommandSeriesState",
    "RecurringCommandStore",
    "ScheduleNextOccurrence",
    "next_cadence_at",
]
