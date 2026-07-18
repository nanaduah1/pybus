from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pybus.recurrence import (
    EndRecurrence,
    Recurrence,
    RecurrenceCadence,
    ScheduleNextOccurrence,
    next_cadence_at,
)


def test_monthly_cadence_keeps_the_original_anchor_after_clamping() -> None:
    recurrence = Recurrence(
        cadence=RecurrenceCadence.MONTHLY,
        timezone="UTC",
    )
    starts_at = datetime(2027, 1, 31, 9, tzinfo=timezone.utc)

    february = next_cadence_at(
        recurrence,
        starts_at=starts_at,
        after=datetime(2027, 1, 31, 10, tzinfo=timezone.utc),
    )
    march = next_cadence_at(
        recurrence,
        starts_at=starts_at,
        after=datetime(2027, 2, 28, 10, tzinfo=timezone.utc),
    )

    assert february == datetime(2027, 2, 28, 9, tzinfo=timezone.utc)
    assert march == datetime(2027, 3, 31, 9, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("cadence", "expected"),
    [
        (RecurrenceCadence.DAILY, datetime(2027, 2, 2, 9, tzinfo=timezone.utc)),
        (RecurrenceCadence.WEEKLY, datetime(2027, 2, 8, 9, tzinfo=timezone.utc)),
    ],
)
def test_daily_and_weekly_cadence_skip_to_the_first_future_anchor(
    cadence: RecurrenceCadence,
    expected: datetime,
) -> None:
    starts_at = datetime(2027, 2, 1, 9, tzinfo=timezone.utc)

    assert (
        next_cadence_at(
            Recurrence(cadence),
            starts_at=starts_at,
            after=datetime(2027, 2, 1, 12, tzinfo=timezone.utc),
        )
        == expected
    )


def test_calendar_cadence_uses_named_timezone_and_resolves_dst() -> None:
    recurrence = Recurrence(RecurrenceCadence.DAILY, timezone="America/New_York")
    spring_anchor = datetime(2027, 3, 13, 7, 30, tzinfo=timezone.utc)

    spring = next_cadence_at(
        recurrence,
        starts_at=spring_anchor,
        after=spring_anchor,
    )

    assert spring == datetime(2027, 3, 14, 7, 30, tzinfo=timezone.utc)

    fold_anchor = datetime(2027, 11, 6, 5, 30, tzinfo=timezone.utc)
    fold = next_cadence_at(
        recurrence,
        starts_at=fold_anchor,
        after=fold_anchor,
    )
    assert fold == datetime(2027, 11, 7, 5, 30, tzinfo=timezone.utc)


def test_exclusive_end_boundary_completes_the_series() -> None:
    starts_at = datetime(2027, 1, 1, 9, tzinfo=timezone.utc)
    recurrence = Recurrence(
        RecurrenceCadence.DAILY,
        ends_at=datetime(2027, 1, 2, 9, tzinfo=timezone.utc),
    )

    assert (
        next_cadence_at(
            recurrence,
            starts_at=starts_at,
            after=starts_at,
        )
        is None
    )


def test_recurrence_contract_rejects_invalid_time_context() -> None:
    with pytest.raises(ValueError, match="timezone"):
        Recurrence(RecurrenceCadence.DAILY, timezone="missing/zone")
    with pytest.raises(ValueError, match="timezone-aware"):
        ScheduleNextOccurrence(at=datetime(2027, 1, 1, 9))


def test_end_recurrence_is_an_explicit_terminal_result() -> None:
    assert EndRecurrence() == EndRecurrence()
