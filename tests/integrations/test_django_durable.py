# ruff: noqa: E402

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from importlib import import_module
import json
from pathlib import Path
import subprocess
import sys
import textwrap
from types import SimpleNamespace
from uuid import UUID

import pytest


django = pytest.importorskip("django")

from django.conf import settings


if not settings.configured:
    settings.configure(
        DATABASES={
            "default": {
                "ENGINE": "django.db.backends.sqlite3",
                "NAME": ":memory:",
            }
        },
        INSTALLED_APPS=["pybus.integrations.django_durable"],
        SECRET_KEY="pybus-tests",
    )
    django.setup()

from django.db import connection, transaction

from pybus import (
    ContinueProcessing,
    EndRecurrence,
    Pybus,
    Recurrence,
    RecurrenceCadence,
    ScheduleNextOccurrence,
    command,
    command_handler,
)
from pybus.delivery import CommandDeliveryOutcome, CommandDeliveryStatus
from pybus.durable import (
    DurableJobPolicy,
    DurableJobState,
    DurableDeliveryAdmission,
    DurableSettlement,
)
from pybus.envelope import MessageEnvelope
from pybus.exceptions import (
    DeliveryObservationError,
    DurableJobConflictError,
    IndeterminateDeliveryError,
    WorkerAbortError,
)
from pybus.integrations.django_durable.models import (
    DurableJob,
    JobSeries,
)
from pybus.integrations.django_durable.store import DjangoDurableJobStore
from pybus.listener import DEFAULT_FAILED_QUEUE_NAME, DEFAULT_QUEUE_NAME
from pybus.queues import DEFAULT_SLOW_QUEUE_NAME
from pybus.retries import RetryPolicy
from pybus.recurrence import JobSeriesState
from pybus.serializer import JsonSerializer
from pybus.transports.memory import MemoryTransport


@command("reports.build")
class BuildReport:
    report_id: int


@command("reports.build_with_supported_values")
class BuildReportWithSupportedValues:
    occurred_at: datetime
    occurred_on: date
    local_time: time
    reference_id: UUID
    nested: dict[str, object]


class FailOnPublication(MemoryTransport):
    def __init__(self, publication: int) -> None:
        super().__init__()
        self.publication = publication
        self.publish_count = 0

    def publish(self, channel: str, message: bytes) -> None:
        self.publish_count += 1
        if self.publish_count == self.publication:
            raise RuntimeError("transport acknowledgement unavailable")
        super().publish(channel, message)


class EnqueueThenFail(MemoryTransport):
    def publish(self, channel: str, message: bytes) -> None:
        super().publish(channel, message)
        raise RuntimeError("acknowledgement lost")


@pytest.fixture(autouse=True)
def durable_table():
    tables = connection.introspection.table_names()
    if JobSeries._meta.db_table not in tables:
        with connection.schema_editor() as editor:
            editor.create_model(JobSeries)
    if DurableJob._meta.db_table not in tables:
        with connection.schema_editor() as editor:
            editor.create_model(DurableJob)
    DurableJob.objects.all().delete()
    JobSeries.objects.all().delete()
    yield
    DurableJob.objects.all().delete()
    JobSeries.objects.all().delete()


def _complete_recurring_occurrence(
    store: DjangoDurableJobStore,
    *,
    at: datetime,
    result: object = None,
) -> CommandDeliveryOutcome:
    claim = store.claim(
        worker_id="recurrence-test",
        now=at,
        lease_expires_at=at + timedelta(seconds=30),
    )
    assert claim is not None
    admission = DurableDeliveryAdmission(
        record_id=claim.id,
        generation=claim.generation,
        source_queue=claim.queue,
        retry_count=claim.retry_count,
        max_retries=1,
        message_id=claim.message_id,
        message_type=claim.message_type,
        version=claim.version,
        payload=claim.payload,
        application_headers=claim.headers,
    )
    assert store.admit_delivery(admission).value == "proceed"
    outcome = CommandDeliveryOutcome(
        status=CommandDeliveryStatus.SUCCEEDED,
        message_id=claim.message_id,
        message_type=claim.message_type,
        version=claim.version,
        source_queue=claim.queue,
        destination_queue=None,
        retry_count=claim.retry_count,
        max_retries=1,
        durable_record_id=claim.id,
        durable_generation=claim.generation,
    )
    assert store.complete_recurring_success(
        outcome,
        result,
        completed_at=at,
    )
    return outcome


def _historical_apps(**models):
    return SimpleNamespace(get_model=lambda app_label, name: models[name])


def test_django_command_storage_names_are_exact_job_aliases() -> None:
    from pybus.integrations.django_durable.models import (
        DurableCommand,
        RecurringCommandSeries,
    )
    from pybus.integrations.django_durable.store import DjangoDurableCommandStore

    assert DurableCommand is DurableJob
    assert RecurringCommandSeries is JobSeries
    assert DjangoDurableCommandStore is DjangoDurableJobStore


def test_schedule_participates_in_the_callers_database_transaction() -> None:
    bus = Pybus(MemoryTransport(), durable_job_store=DjangoDurableJobStore())

    with pytest.raises(RuntimeError, match="rollback"):
        with transaction.atomic():
            bus.schedule_command(BuildReport(report_id=1))
            raise RuntimeError("rollback")

    assert DurableJob.objects.count() == 0


def test_recurring_schedule_and_first_occurrence_are_atomic() -> None:
    bus = Pybus(MemoryTransport(), durable_job_store=DjangoDurableJobStore())
    starts_at = datetime(2026, 7, 20, 9, tzinfo=timezone.utc)

    with pytest.raises(RuntimeError, match="rollback"):
        with transaction.atomic():
            bus.schedule_command(
                BuildReport(report_id=101),
                run_at=starts_at,
                recurrence=Recurrence(RecurrenceCadence.DAILY),
            )
            raise RuntimeError("rollback")

    assert JobSeries.objects.count() == 0
    assert DurableJob.objects.count() == 0


def test_recurring_success_creates_one_anchored_successor() -> None:
    store = DjangoDurableJobStore()
    bus = Pybus(MemoryTransport(), durable_job_store=store)
    starts_at = datetime(2026, 1, 31, 9, tzinfo=timezone.utc)
    handle = bus.schedule_command(
        BuildReport(report_id=102),
        run_at=starts_at,
        recurrence=Recurrence(RecurrenceCadence.MONTHLY),
    )

    _complete_recurring_occurrence(
        store,
        at=starts_at + timedelta(hours=1),
        result=ScheduleNextOccurrence(datetime(2026, 2, 15, 9, tzinfo=timezone.utc)),
    )
    _complete_recurring_occurrence(
        store,
        at=datetime(2026, 2, 15, 10, tzinfo=timezone.utc),
    )

    occurrences = list(
        DurableJob.objects.filter(series_id=handle.series_id).order_by(
            "occurrence_number"
        )
    )
    assert [item.occurrence_number for item in occurrences] == [1, 2, 3]
    assert occurrences[1].available_at == datetime(2026, 2, 15, 9, tzinfo=timezone.utc)
    assert occurrences[2].available_at == datetime(2026, 2, 28, 9, tzinfo=timezone.utc)


def test_recurring_handler_can_end_series_without_a_successor() -> None:
    store = DjangoDurableJobStore()
    bus = Pybus(MemoryTransport(), durable_job_store=store)
    starts_at = datetime(2026, 7, 20, 9, tzinfo=timezone.utc)
    handle = bus.schedule_command(
        BuildReport(report_id=103),
        run_at=starts_at,
        recurrence=Recurrence(RecurrenceCadence.WEEKLY),
    )

    _complete_recurring_occurrence(
        store,
        at=starts_at + timedelta(minutes=1),
        result=EndRecurrence(),
    )

    series = JobSeries.objects.get(id=handle.series_id)
    assert series.state == JobSeriesState.COMPLETED
    assert series.occurrences.count() == 1


def test_cancelling_a_pending_series_cancels_its_occurrence() -> None:
    store = DjangoDurableJobStore()
    bus = Pybus(MemoryTransport(), durable_job_store=store)
    handle = bus.schedule_command(
        BuildReport(report_id=104),
        run_at=datetime(2026, 8, 1, 9, tzinfo=timezone.utc),
        recurrence=Recurrence(RecurrenceCadence.DAILY),
    )

    cancelled = bus.cancel_recurring_command(handle.series_id)

    assert cancelled.state == JobSeriesState.CANCELLED
    assert DurableJob.objects.get(id=handle.id).state == DurableJobState.CANCELLED


def test_recurring_schedule_idempotency_is_owned_by_the_series() -> None:
    bus = Pybus(MemoryTransport(), durable_job_store=DjangoDurableJobStore())
    starts_at = datetime(2026, 8, 1, 9, tzinfo=timezone.utc)
    recurrence = Recurrence(RecurrenceCadence.DAILY)

    first = bus.schedule_command(
        BuildReport(report_id=105),
        run_at=starts_at,
        recurrence=recurrence,
        idempotency_key="daily-report",
    )
    second = bus.schedule_command(
        BuildReport(report_id=105),
        run_at=starts_at,
        recurrence=recurrence,
        idempotency_key="daily-report",
    )

    assert second == first
    assert JobSeries.objects.count() == 1
    assert DurableJob.objects.count() == 1


@pytest.mark.parametrize("recurring_first", [False, True])
def test_idempotency_key_cannot_cross_one_off_and_recurring_modes(
    recurring_first: bool,
) -> None:
    bus = Pybus(MemoryTransport(), durable_job_store=DjangoDurableJobStore())

    def one_off():
        return bus.schedule_command(
            BuildReport(report_id=116),
            idempotency_key="report-mode:116",
        )

    def recurring():
        return bus.schedule_command(
            BuildReport(report_id=116),
            recurrence=Recurrence(RecurrenceCadence.DAILY),
            idempotency_key="report-mode:116",
        )

    first, second = (recurring, one_off) if recurring_first else (one_off, recurring)
    first()
    with pytest.raises(DurableJobConflictError):
        second()


def test_dead_lettering_an_occurrence_fails_the_series() -> None:
    @command_handler(BuildReport)
    def fail(message: BuildReport) -> None:
        raise RuntimeError("reporting unavailable")

    transport = MemoryTransport()
    store = DjangoDurableJobStore()
    bus = Pybus(
        transport,
        durable_job_store=store,
        handler_targets=[fail],
    )
    bus.listener.retry_policy = RetryPolicy(max_retries=0)
    handle = bus.schedule_command(
        BuildReport(report_id=106),
        run_at=django.utils.timezone.now() - timedelta(seconds=1),
        recurrence=Recurrence(RecurrenceCadence.DAILY),
    )

    bus.create_durable_job_worker(error_delay=0).run(max_iterations=1)
    bus.listen_once(DEFAULT_QUEUE_NAME)

    series = JobSeries.objects.get(id=handle.series_id)
    assert series.state == JobSeriesState.FAILED
    assert DurableJob.objects.get(id=handle.id).state == DurableJobState.DEAD_LETTERED


def test_listener_success_advances_a_recurring_series() -> None:
    @command_handler(BuildReport)
    def succeed(message: BuildReport) -> None:
        return None

    transport = MemoryTransport()
    store = DjangoDurableJobStore()
    bus = Pybus(
        transport,
        durable_job_store=store,
        handler_targets=[succeed],
    )
    handle = bus.schedule_command(
        BuildReport(report_id=107),
        run_at=django.utils.timezone.now() - timedelta(seconds=1),
        recurrence=Recurrence(RecurrenceCadence.DAILY),
    )

    bus.create_durable_job_worker(error_delay=0).run(max_iterations=1)
    bus.listen_once(DEFAULT_QUEUE_NAME)

    occurrences = DurableJob.objects.filter(series_id=handle.series_id)
    assert occurrences.count() == 2
    assert occurrences.get(occurrence_number=1).state == DurableJobState.SUCCEEDED
    assert occurrences.get(occurrence_number=2).state == DurableJobState.PENDING


def test_recurrence_result_on_one_off_command_is_a_handler_failure() -> None:
    @command_handler(BuildReport)
    def invalid(message: BuildReport) -> EndRecurrence:
        return EndRecurrence()

    transport = MemoryTransport()
    store = DjangoDurableJobStore()
    bus = Pybus(
        transport,
        durable_job_store=store,
        handler_targets=[invalid],
    )
    bus.listener.retry_policy = RetryPolicy(max_retries=0)
    handle = bus.schedule_command(BuildReport(report_id=108))

    bus.create_durable_job_worker(error_delay=0).run(max_iterations=1)
    bus.listen_once(DEFAULT_QUEUE_NAME)

    assert DurableJob.objects.get(id=handle.id).state == DurableJobState.DEAD_LETTERED


def test_invalid_recurring_override_retries_the_same_occurrence() -> None:
    @command_handler(BuildReport)
    def invalid(message: BuildReport) -> ScheduleNextOccurrence:
        return ScheduleNextOccurrence(
            django.utils.timezone.now() - timedelta(minutes=1)
        )

    transport = MemoryTransport()
    store = DjangoDurableJobStore()
    bus = Pybus(
        transport,
        durable_job_store=store,
        handler_targets=[invalid],
    )
    bus.listener.retry_policy = RetryPolicy(max_retries=1)
    handle = bus.schedule_command(
        BuildReport(report_id=109),
        run_at=django.utils.timezone.now() - timedelta(seconds=1),
        recurrence=Recurrence(RecurrenceCadence.DAILY),
    )

    bus.create_durable_job_worker(error_delay=0).run(max_iterations=1)
    bus.listen_once(DEFAULT_QUEUE_NAME)

    occurrence = DurableJob.objects.get(id=handle.id)
    assert occurrence.state == DurableJobState.PUBLISHED
    assert occurrence.retry_count == 1
    assert occurrence.occurrence_number == 1
    assert DurableJob.objects.filter(series_id=handle.series_id).count() == 1


def test_duplicate_recurring_success_cannot_create_a_second_successor() -> None:
    store = DjangoDurableJobStore()
    bus = Pybus(MemoryTransport(), durable_job_store=store)
    starts_at = datetime(2026, 8, 1, 9, tzinfo=timezone.utc)
    handle = bus.schedule_command(
        BuildReport(report_id=110),
        run_at=starts_at,
        recurrence=Recurrence(RecurrenceCadence.DAILY),
    )
    completed_at = starts_at + timedelta(minutes=1)

    outcome = _complete_recurring_occurrence(store, at=completed_at)
    assert store.complete_recurring_success(
        outcome,
        None,
        completed_at=completed_at,
    )

    assert DurableJob.objects.filter(series_id=handle.series_id).count() == 2


def test_stale_recurring_success_cannot_advance_the_series() -> None:
    store = DjangoDurableJobStore()
    bus = Pybus(MemoryTransport(), durable_job_store=store)
    handle = bus.schedule_command(
        BuildReport(report_id=121),
        recurrence=Recurrence(RecurrenceCadence.DAILY),
    )
    occurrence = DurableJob.objects.get(id=handle.id)
    DurableJob.objects.filter(id=handle.id).update(
        state=DurableJobState.RUNNING,
        generation=2,
    )
    stale = CommandDeliveryOutcome(
        status=CommandDeliveryStatus.SUCCEEDED,
        message_id=occurrence.message_id,
        message_type=occurrence.message_type,
        version=occurrence.version,
        source_queue=occurrence.queue,
        destination_queue=None,
        retry_count=0,
        max_retries=1,
        durable_record_id=str(occurrence.id),
        durable_generation=1,
    )

    assert not store.complete_recurring_success(
        stale,
        None,
        completed_at=django.utils.timezone.now(),
    )
    assert DurableJob.objects.filter(series_id=handle.series_id).count() == 1


def test_continuation_keeps_the_current_recurring_occurrence() -> None:
    attempts = 0

    @command_handler(BuildReport)
    def continue_once(message: BuildReport) -> ContinueProcessing | None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return ContinueProcessing()
        return None

    transport = MemoryTransport()
    store = DjangoDurableJobStore()
    bus = Pybus(
        transport,
        durable_job_store=store,
        handler_targets=[continue_once],
    )
    handle = bus.schedule_command(
        BuildReport(report_id=111),
        recurrence=Recurrence(RecurrenceCadence.DAILY),
    )
    bus.create_durable_job_worker(error_delay=0).run(max_iterations=1)

    bus.listen_once(DEFAULT_QUEUE_NAME)
    assert DurableJob.objects.filter(series_id=handle.series_id).count() == 1
    assert DurableJob.objects.get(id=handle.id).state == DurableJobState.PUBLISHED

    bus.listen_once(DEFAULT_QUEUE_NAME)
    assert DurableJob.objects.filter(series_id=handle.series_id).count() == 2


def test_cancellation_after_publication_drops_the_transport_copy() -> None:
    handled: list[BuildReport] = []

    @command_handler(BuildReport)
    def handle(message: BuildReport) -> None:
        handled.append(message)

    transport = MemoryTransport()
    store = DjangoDurableJobStore()
    bus = Pybus(
        transport,
        durable_job_store=store,
        handler_targets=[handle],
    )
    scheduled = bus.schedule_command(
        BuildReport(report_id=112),
        recurrence=Recurrence(RecurrenceCadence.DAILY),
    )
    bus.create_durable_job_worker(error_delay=0).run(max_iterations=1)

    bus.cancel_recurring_command(scheduled.series_id)
    bus.listen_once(DEFAULT_QUEUE_NAME)

    assert handled == []
    assert DurableJob.objects.get(id=scheduled.id).state == DurableJobState.CANCELLED
    bus.listen_once(DEFAULT_QUEUE_NAME)
    assert DurableJob.objects.get(id=scheduled.id).state == DurableJobState.CANCELLED


def test_cancellation_during_execution_allows_success_but_no_successor() -> None:
    store = DjangoDurableJobStore()
    bus = Pybus(MemoryTransport(), durable_job_store=store)
    starts_at = datetime(2026, 8, 1, 9, tzinfo=timezone.utc)
    handle = bus.schedule_command(
        BuildReport(report_id=113),
        run_at=starts_at,
        recurrence=Recurrence(RecurrenceCadence.DAILY),
    )
    claim = store.claim(
        worker_id="recurrence-test",
        now=starts_at,
        lease_expires_at=starts_at + timedelta(seconds=30),
    )
    assert claim is not None
    admission = DurableDeliveryAdmission(
        record_id=claim.id,
        generation=claim.generation,
        source_queue=claim.queue,
        retry_count=0,
        max_retries=1,
        message_id=claim.message_id,
        message_type=claim.message_type,
        version=claim.version,
        payload=claim.payload,
        application_headers=claim.headers,
    )
    assert store.admit_delivery(admission).value == "proceed"
    bus.cancel_recurring_command(handle.series_id)
    outcome = CommandDeliveryOutcome(
        status=CommandDeliveryStatus.SUCCEEDED,
        message_id=claim.message_id,
        message_type=claim.message_type,
        version=claim.version,
        source_queue=claim.queue,
        destination_queue=None,
        retry_count=0,
        max_retries=1,
        durable_record_id=claim.id,
        durable_generation=claim.generation,
    )

    assert store.complete_recurring_success(
        outcome,
        None,
        completed_at=starts_at + timedelta(minutes=1),
    )
    assert DurableJob.objects.filter(series_id=handle.series_id).count() == 1
    assert DurableJob.objects.get(id=handle.id).state == DurableJobState.SUCCEEDED


def test_cancellation_during_a_failed_attempt_drops_its_retry() -> None:
    transport = MemoryTransport()
    store = DjangoDurableJobStore()
    scheduled = None

    @command_handler(BuildReport)
    def cancel_then_fail(message: BuildReport) -> None:
        assert scheduled is not None
        bus.cancel_recurring_command(scheduled.series_id)
        raise RuntimeError("cancelled while running")

    bus = Pybus(
        transport,
        durable_job_store=store,
        handler_targets=[cancel_then_fail],
    )
    bus.listener.retry_policy = RetryPolicy(max_retries=1)
    scheduled = bus.schedule_command(
        BuildReport(report_id=114),
        recurrence=Recurrence(RecurrenceCadence.DAILY),
    )
    bus.create_durable_job_worker(error_delay=0).run(max_iterations=1)

    bus.listen_once(DEFAULT_QUEUE_NAME)
    assert DurableJob.objects.get(id=scheduled.id).state == DurableJobState.CANCELLED


def test_cancellation_terminalizes_an_abandoned_running_occurrence() -> None:
    store = DjangoDurableJobStore()
    bus = Pybus(MemoryTransport(), durable_job_store=store)
    starts_at = datetime(2026, 8, 1, 9, tzinfo=timezone.utc)
    handle = bus.schedule_command(
        BuildReport(report_id=117),
        run_at=starts_at,
        recurrence=Recurrence(RecurrenceCadence.DAILY),
    )
    claim = store.claim(
        worker_id="lost-worker",
        now=starts_at,
        lease_expires_at=starts_at + timedelta(seconds=30),
    )
    assert claim is not None
    admission = DurableDeliveryAdmission(
        record_id=claim.id,
        generation=claim.generation,
        source_queue=claim.queue,
        retry_count=0,
        max_retries=1,
        message_id=claim.message_id,
        message_type=claim.message_type,
        version=claim.version,
        payload=claim.payload,
        application_headers=claim.headers,
        reconciliation_due_at=starts_at + timedelta(seconds=30),
    )
    assert store.admit_delivery(admission).value == "proceed"
    bus.cancel_recurring_command(handle.series_id)

    assert (
        store.claim(
            worker_id="recovery",
            now=starts_at + timedelta(minutes=1),
            lease_expires_at=starts_at + timedelta(minutes=2),
        )
        is None
    )
    assert DurableJob.objects.get(id=handle.id).state == DurableJobState.CANCELLED


def test_cancellation_terminalizes_an_occurrence_during_settlement() -> None:
    store = DjangoDurableJobStore()
    bus = Pybus(MemoryTransport(), durable_job_store=store)
    starts_at = datetime(2026, 8, 1, 9, tzinfo=timezone.utc)
    handle = bus.schedule_command(
        BuildReport(report_id=118),
        run_at=starts_at,
        recurrence=Recurrence(RecurrenceCadence.DAILY),
    )
    claim = store.claim(
        worker_id="settling-worker",
        now=starts_at,
        lease_expires_at=starts_at + timedelta(seconds=30),
    )
    assert claim is not None
    DurableJob.objects.filter(id=handle.id).update(
        state=DurableJobState.RUNNING,
        max_retries=1,
    )
    store.checkpoint_settlement(
        DurableSettlement(
            record_id=claim.id,
            generation=claim.generation,
            status=CommandDeliveryStatus.CONTINUED,
            source_queue=claim.queue,
            destination_queue=claim.queue,
            retry_count=0,
            max_retries=1,
            reconciliation_due_at=starts_at + timedelta(minutes=1),
        )
    )

    bus.cancel_recurring_command(handle.series_id)

    assert DurableJob.objects.get(id=handle.id).state == DurableJobState.CANCELLED


def test_dead_letter_acknowledgement_recovery_fails_the_series() -> None:
    store = DjangoDurableJobStore()
    bus = Pybus(MemoryTransport(), durable_job_store=store)
    starts_at = datetime(2026, 8, 1, 9, tzinfo=timezone.utc)
    handle = bus.schedule_command(
        BuildReport(report_id=119),
        run_at=starts_at,
        recurrence=Recurrence(RecurrenceCadence.DAILY),
    )
    claim = store.claim(
        worker_id="dead-letter-worker",
        now=starts_at,
        lease_expires_at=starts_at + timedelta(seconds=30),
    )
    assert claim is not None
    DurableJob.objects.filter(id=handle.id).update(
        state=DurableJobState.RUNNING,
        max_retries=0,
    )
    store.checkpoint_settlement(
        DurableSettlement(
            record_id=claim.id,
            generation=claim.generation,
            status=CommandDeliveryStatus.DEAD_LETTERED,
            source_queue=claim.queue,
            destination_queue=DEFAULT_FAILED_QUEUE_NAME,
            retry_count=0,
            max_retries=0,
            reconciliation_due_at=starts_at - timedelta(seconds=1),
        )
    )
    recovered = store.claim(
        worker_id="recovery",
        now=starts_at,
        lease_expires_at=starts_at + timedelta(seconds=30),
    )
    assert recovered is not None

    store.mark_published(
        recovered,
        now=starts_at,
        reconciliation_due_at=starts_at + timedelta(minutes=1),
    )

    assert DurableJob.objects.get(id=handle.id).state == DurableJobState.DEAD_LETTERED
    assert JobSeries.objects.get(id=handle.series_id).state == JobSeriesState.FAILED


def test_nested_savepoint_rollback_discards_only_inner_schedule() -> None:
    bus = Pybus(MemoryTransport(), durable_job_store=DjangoDurableJobStore())

    with transaction.atomic():
        bus.schedule_command(BuildReport(report_id=20))
        with pytest.raises(RuntimeError, match="inner"):
            with transaction.atomic():
                bus.schedule_command(BuildReport(report_id=21))
                raise RuntimeError("inner")

    assert list(DurableJob.objects.values_list("payload", flat=True)) == [
        {"report_id": 20}
    ]


def test_idempotency_key_conflicts_with_a_different_command() -> None:
    bus = Pybus(MemoryTransport(), durable_job_store=DjangoDurableJobStore())
    bus.schedule_command(BuildReport(report_id=1), idempotency_key="report:1")

    with pytest.raises(DurableJobConflictError):
        bus.schedule_command(BuildReport(report_id=2), idempotency_key="report:1")


def test_same_idempotency_key_and_command_returns_existing_handle() -> None:
    bus = Pybus(MemoryTransport(), durable_job_store=DjangoDurableJobStore())

    first = bus.schedule_command(BuildReport(report_id=1), idempotency_key="report:1")
    second = bus.schedule_command(BuildReport(report_id=1), idempotency_key="report:1")

    assert second == first
    assert DurableJob.objects.count() == 1


def test_reverse_migration_refuses_to_drop_nonempty_table() -> None:
    bus = Pybus(MemoryTransport(), durable_job_store=DjangoDurableJobStore())
    bus.schedule_command(BuildReport(report_id=2))
    migration = import_module(
        "pybus.integrations.django_durable.migrations.0001_initial"
    )

    with pytest.raises(RuntimeError, match="contains data"):
        migration.refuse_nonempty_reverse(
            _historical_apps(DurableCommand=DurableJob),
            SimpleNamespace(connection=connection),
        )


def test_recurrence_migration_refuses_to_drop_series_data() -> None:
    bus = Pybus(MemoryTransport(), durable_job_store=DjangoDurableJobStore())
    bus.schedule_command(
        BuildReport(report_id=115),
        recurrence=Recurrence(RecurrenceCadence.DAILY),
    )
    migration = import_module(
        "pybus.integrations.django_durable.migrations.0002_recurring_commands"
    )

    with pytest.raises(RuntimeError, match="recurring-command storage"):
        migration.refuse_recurrence_reverse(
            _historical_apps(
                RecurringCommandSeries=JobSeries,
                DurableCommand=DurableJob,
            ),
            SimpleNamespace(connection=connection),
        )


def test_recurrence_migration_can_reverse_with_only_legacy_one_off_rows() -> None:
    bus = Pybus(MemoryTransport(), durable_job_store=DjangoDurableJobStore())
    bus.schedule_command(BuildReport(report_id=120))
    migration = import_module(
        "pybus.integrations.django_durable.migrations.0002_recurring_commands"
    )

    migration.refuse_recurrence_reverse(
        _historical_apps(
            RecurringCommandSeries=JobSeries,
            DurableCommand=DurableJob,
        ),
        SimpleNamespace(connection=connection),
    )


def test_django_store_runs_the_command_to_an_irreversible_terminal_state() -> None:
    handled = []

    @command_handler(BuildReport)
    def handle(message: BuildReport) -> None:
        handled.append(message)

    transport = MemoryTransport()
    bus = Pybus(
        transport,
        durable_job_store=DjangoDurableJobStore(),
        handler_targets=[handle],
    )
    handle_ref = bus.schedule_command(BuildReport(report_id=3))

    bus.create_durable_job_worker(error_delay=0).run(max_iterations=1)
    bus.listen_once(DEFAULT_QUEUE_NAME)

    record = DurableJob.objects.get(id=handle_ref.id)
    assert handled == [BuildReport(report_id=3)]
    assert record.state == DurableJobState.SUCCEEDED
    assert record.generation == 1


def test_durable_command_admits_supported_codec_values_end_to_end() -> None:
    handled = []

    @command_handler(BuildReportWithSupportedValues)
    def handle(message: BuildReportWithSupportedValues) -> None:
        handled.append(message)

    occurred_at = datetime(2026, 7, 18, 14, 30, tzinfo=timezone.utc)
    occurred_on = date(2026, 7, 18)
    local_time = time(14, 30, tzinfo=timezone.utc)
    reference_id = UUID("2e75d610-8f16-4a89-a875-c34353ad411d")
    command = BuildReportWithSupportedValues(
        occurred_at=occurred_at,
        occurred_on=occurred_on,
        local_time=local_time,
        reference_id=reference_id,
        nested={
            "occurred_at": occurred_at,
            "occurred_on": occurred_on,
            "local_time": local_time,
            "reference_id": reference_id,
        },
    )
    transport = MemoryTransport()
    bus = Pybus(
        transport,
        durable_job_store=DjangoDurableJobStore(),
        handler_targets=[handle],
    )
    handle_ref = bus.schedule_command(command)

    bus.create_durable_job_worker(error_delay=0).run(max_iterations=1)
    bus.listen_once(DEFAULT_QUEUE_NAME)

    assert handled == [command]
    assert DurableJob.objects.get(id=handle_ref.id).state == DurableJobState.SUCCEEDED


def test_late_mark_published_cannot_regress_a_fast_successful_handler() -> None:
    store = DjangoDurableJobStore()
    bus = Pybus(MemoryTransport(), durable_job_store=store)
    handle_ref = bus.schedule_command(BuildReport(report_id=4))
    now = django.utils.timezone.now()
    claim = store.claim(
        worker_id="publisher",
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert claim is not None
    admission = DurableDeliveryAdmission(
        record_id=claim.id,
        generation=claim.generation,
        source_queue=DEFAULT_QUEUE_NAME,
        retry_count=0,
        max_retries=10,
        message_id=claim.message_id,
        message_type=claim.message_type,
        version=claim.version,
        payload=claim.payload,
        application_headers=claim.headers,
    )
    assert store.admit_delivery(admission).value == "proceed"

    store.mark_published(
        claim,
        now=now,
        reconciliation_due_at=now + timedelta(minutes=5),
    )

    assert DurableJob.objects.get(id=handle_ref.id).state == DurableJobState.RUNNING


def test_reconciliation_reclaims_with_a_new_generation_and_fences_old_copy() -> None:
    store = DjangoDurableJobStore()
    bus = Pybus(MemoryTransport(), durable_job_store=store)
    bus.schedule_command(BuildReport(report_id=5))
    now = django.utils.timezone.now()
    first = store.claim(
        worker_id="first",
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert first is not None
    store.mark_published(
        first,
        now=now,
        reconciliation_due_at=now - timedelta(seconds=1),
    )

    second = store.claim(
        worker_id="second",
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )

    assert second is not None
    assert second.generation == first.generation + 1
    stale = DurableDeliveryAdmission(
        record_id=first.id,
        generation=first.generation,
        source_queue=DEFAULT_QUEUE_NAME,
        retry_count=0,
        max_retries=10,
        message_id=first.message_id,
        message_type=first.message_type,
        version=first.version,
        payload=first.payload,
        application_headers=first.headers,
    )
    assert store.admit_delivery(stale).value == "drop"


def test_unknown_running_outcome_advances_retry_floor_or_stays_indeterminate() -> None:
    store = DjangoDurableJobStore()
    bus = Pybus(MemoryTransport(), durable_job_store=store)
    handle_ref = bus.schedule_command(BuildReport(report_id=6))
    now = django.utils.timezone.now()
    DurableJob.objects.filter(id=handle_ref.id).update(
        state=DurableJobState.RUNNING,
        generation=1,
        retry_count=0,
        max_retries=1,
        reconciliation_due_at=now - timedelta(seconds=1),
    )

    recovered = store.claim(
        worker_id="recovery",
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert recovered is not None
    assert recovered.generation == 2
    assert recovered.retry_count == 1
    assert recovered.delivery_envelope().headers["retries"] == 1

    DurableJob.objects.filter(id=handle_ref.id).update(
        state=DurableJobState.RUNNING,
        retry_count=1,
        max_retries=1,
        reconciliation_due_at=now - timedelta(seconds=1),
    )
    assert (
        store.claim(
            worker_id="recovery",
            now=now,
            lease_expires_at=now + timedelta(seconds=30),
        )
        is None
    )
    assert (
        DurableJob.objects.get(id=handle_ref.id).state == DurableJobState.INDETERMINATE
    )


def test_crash_after_admission_respects_zero_retry_budget() -> None:
    store = DjangoDurableJobStore()
    bus = Pybus(MemoryTransport(), durable_job_store=store)
    bus.schedule_command(BuildReport(report_id=22))
    now = django.utils.timezone.now()
    claim = store.claim(
        worker_id="publisher",
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert claim is not None
    admission = DurableDeliveryAdmission(
        record_id=claim.id,
        generation=claim.generation,
        source_queue=claim.queue,
        retry_count=0,
        max_retries=0,
        message_id=claim.message_id,
        message_type=claim.message_type,
        version=claim.version,
        payload=claim.payload,
        application_headers=claim.headers,
        reconciliation_due_at=now - timedelta(seconds=1),
    )
    assert store.admit_delivery(admission).value == "proceed"

    assert (
        store.claim(
            worker_id="recovery",
            now=now,
            lease_expires_at=now + timedelta(seconds=30),
        )
        is None
    )
    assert DurableJob.objects.get().state == DurableJobState.INDETERMINATE
    assert store.admit_delivery(admission).value == "drop"


def test_restart_cannot_increase_the_persisted_retry_budget() -> None:
    store = DjangoDurableJobStore()
    bus = Pybus(MemoryTransport(), durable_job_store=store)
    bus.schedule_command(BuildReport(report_id=24))
    now = django.utils.timezone.now()
    claim = store.claim(
        worker_id="publisher",
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert claim is not None
    admission = DurableDeliveryAdmission(
        record_id=claim.id,
        generation=claim.generation,
        source_queue=claim.queue,
        retry_count=0,
        max_retries=0,
        message_id=claim.message_id,
        message_type=claim.message_type,
        version=claim.version,
        payload=claim.payload,
        application_headers=claim.headers,
        reconciliation_due_at=now + timedelta(minutes=5),
    )
    assert store.admit_delivery(admission).value == "proceed"
    store.release_delivery(
        record_id=claim.id,
        generation=claim.generation,
        reconciliation_due_at=now + timedelta(minutes=5),
    )

    assert store.admit_delivery(replace(admission, max_retries=5)).value == "abort"
    record = DurableJob.objects.get(id=claim.id)
    assert record.state == DurableJobState.PUBLISHED
    assert record.max_retries == 0


def test_retry_then_dead_letter_updates_state_without_reviving_terminal_work() -> None:
    @command_handler(BuildReport)
    def fail(message: BuildReport) -> None:
        raise RuntimeError("reporting unavailable")

    transport = MemoryTransport()
    store = DjangoDurableJobStore()
    bus = Pybus(transport, durable_job_store=store, handler_targets=[fail])
    bus.listener.retry_policy = RetryPolicy(max_retries=1)
    handle_ref = bus.schedule_command(BuildReport(report_id=7))
    bus.create_durable_job_worker(error_delay=0).run(max_iterations=1)

    bus.listen_once(DEFAULT_QUEUE_NAME)
    assert DurableJob.objects.get(id=handle_ref.id).state == DurableJobState.PUBLISHED
    bus.listen_once(DEFAULT_QUEUE_NAME)

    terminal = DurableJob.objects.get(id=handle_ref.id)
    assert terminal.state == DurableJobState.DEAD_LETTERED
    assert terminal.retry_count == 1
    assert terminal.finished_at is not None
    assert transport.size(DEFAULT_FAILED_QUEUE_NAME) == 1


def test_checkpointed_continuation_resumes_after_restart() -> None:
    @command_handler(BuildReport)
    def continue_later(message: BuildReport) -> ContinueProcessing:
        return ContinueProcessing(queue=DEFAULT_SLOW_QUEUE_NAME)

    transport = FailOnPublication(publication=2)
    store = DjangoDurableJobStore()
    bus = Pybus(transport, durable_job_store=store, handler_targets=[continue_later])
    handle_ref = bus.schedule_command(BuildReport(report_id=8))
    bus.create_durable_job_worker(error_delay=0).run(max_iterations=1)

    with pytest.raises(IndeterminateDeliveryError):
        bus.listen_once(DEFAULT_QUEUE_NAME)
    DurableJob.objects.filter(id=handle_ref.id).update(
        reconciliation_due_at=django.utils.timezone.now() - timedelta(seconds=1)
    )

    bus.create_durable_job_worker(error_delay=0).run(max_iterations=1)

    recovered = DurableJob.objects.get(id=handle_ref.id)
    assert recovered.state == DurableJobState.PUBLISHED
    assert recovered.queue == DEFAULT_SLOW_QUEUE_NAME
    assert recovered.generation == 2
    assert transport.size(DEFAULT_SLOW_QUEUE_NAME) == 1


def test_duplicate_or_stale_outcomes_cannot_regress_terminal_state() -> None:
    store = DjangoDurableJobStore()
    bus = Pybus(MemoryTransport(), durable_job_store=store)
    handle_ref = bus.schedule_command(BuildReport(report_id=9))
    DurableJob.objects.filter(id=handle_ref.id).update(
        state=DurableJobState.SUCCEEDED,
        generation=2,
        finished_at=django.utils.timezone.now(),
    )
    stale = CommandDeliveryOutcome(
        status=CommandDeliveryStatus.RETRY_SCHEDULED,
        message_id=handle_ref.message_id,
        message_type="reports.build",
        version=1,
        source_queue=DEFAULT_QUEUE_NAME,
        destination_queue=DEFAULT_QUEUE_NAME,
        retry_count=99,
        max_retries=100,
        durable_record_id=handle_ref.id,
        durable_generation=1,
    )

    deadline = django.utils.timezone.now() + timedelta(minutes=5)
    store.apply_outcome(stale, reconciliation_due_at=deadline)
    store.apply_outcome(stale, reconciliation_due_at=deadline)

    terminal = DurableJob.objects.get(id=handle_ref.id)
    assert terminal.state == DurableJobState.SUCCEEDED
    assert terminal.retry_count == 0


def test_only_one_claim_owns_a_pending_generation() -> None:
    store = DjangoDurableJobStore()
    bus = Pybus(MemoryTransport(), durable_job_store=store)
    bus.schedule_command(BuildReport(report_id=10))
    now = django.utils.timezone.now()

    first = store.claim(
        worker_id="one",
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )
    second = store.claim(
        worker_id="two",
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )

    assert first is not None
    assert second is None


@pytest.mark.parametrize(
    "change",
    [
        {"message_id": "tampered"},
        {"message_type": "reports.other"},
        {"version": 2},
        {"payload": {"report_id": 999}},
        {"application_headers": {"tenant": "other"}},
        {"source_queue": DEFAULT_SLOW_QUEUE_NAME},
        {"retry_count": 1},
        {"last_attempt_at": django.utils.timezone.now()},
    ],
)
def test_admission_rejects_tampered_logical_command(change) -> None:
    store = DjangoDurableJobStore()
    bus = Pybus(MemoryTransport(), durable_job_store=store)
    bus.schedule_command(BuildReport(report_id=11))
    now = django.utils.timezone.now()
    claim = store.claim(
        worker_id="publisher",
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert claim is not None
    admission = DurableDeliveryAdmission(
        record_id=claim.id,
        generation=claim.generation,
        source_queue=claim.queue,
        retry_count=claim.retry_count,
        max_retries=10,
        message_id=claim.message_id,
        message_type=claim.message_type,
        version=claim.version,
        payload=claim.payload,
        application_headers=claim.headers,
    )

    assert store.admit_delivery(replace(admission, **change)).value == "abort"


@pytest.mark.parametrize(
    "supported_value",
    [
        datetime(2026, 7, 18, 15, 45, tzinfo=timezone.utc),
        date(2026, 7, 18),
        time(15, 45, tzinfo=timezone.utc),
        UUID("7387d9ce-1de7-4e4a-81b5-54c6d02c2630"),
    ],
)
def test_admission_canonicalizes_supported_application_header_values(
    supported_value,
) -> None:
    store = DjangoDurableJobStore()
    bus = Pybus(MemoryTransport(), durable_job_store=store)
    handle_ref = bus.schedule_command(BuildReport(report_id=111))
    DurableJob.objects.filter(id=handle_ref.id).update(
        headers=json.loads(JsonSerializer().dumps({"value": supported_value}))
    )
    now = django.utils.timezone.now()
    claim = store.claim(
        worker_id="publisher",
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert claim is not None
    delivered_headers = JsonSerializer().loads(JsonSerializer().dumps(claim.headers))
    admission = DurableDeliveryAdmission(
        record_id=claim.id,
        generation=claim.generation,
        source_queue=claim.queue,
        retry_count=claim.retry_count,
        max_retries=10,
        message_id=claim.message_id,
        message_type=claim.message_type,
        version=claim.version,
        payload=claim.payload,
        application_headers=delivered_headers,
    )

    assert store.admit_delivery(admission).value == "proceed"


def test_admission_rejects_a_modified_supported_codec_value() -> None:
    store = DjangoDurableJobStore()
    bus = Pybus(MemoryTransport(), durable_job_store=store)
    handle_ref = bus.schedule_command(BuildReport(report_id=113))
    stored_value = datetime(2026, 7, 18, 15, 45, tzinfo=timezone.utc)
    DurableJob.objects.filter(id=handle_ref.id).update(
        headers=json.loads(JsonSerializer().dumps({"value": stored_value}))
    )
    now = django.utils.timezone.now()
    claim = store.claim(
        worker_id="publisher",
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert claim is not None
    admission = DurableDeliveryAdmission(
        record_id=claim.id,
        generation=claim.generation,
        source_queue=claim.queue,
        retry_count=claim.retry_count,
        max_retries=10,
        message_id=claim.message_id,
        message_type=claim.message_type,
        version=claim.version,
        payload=claim.payload,
        application_headers={"value": stored_value + timedelta(seconds=1)},
    )

    assert store.admit_delivery(admission).value == "abort"


def test_admission_distinguishes_json_booleans_from_integers() -> None:
    store = DjangoDurableJobStore()
    bus = Pybus(MemoryTransport(), durable_job_store=store)
    handle_ref = bus.schedule_command(BuildReport(report_id=112))
    DurableJob.objects.filter(id=handle_ref.id).update(headers={"enabled": True})
    now = django.utils.timezone.now()
    claim = store.claim(
        worker_id="publisher",
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert claim is not None
    admission = DurableDeliveryAdmission(
        record_id=claim.id,
        generation=claim.generation,
        source_queue=claim.queue,
        retry_count=claim.retry_count,
        max_retries=10,
        message_id=claim.message_id,
        message_type=claim.message_type,
        version=claim.version,
        payload=claim.payload,
        application_headers={"enabled": 1},
    )

    assert store.admit_delivery(admission).value == "abort"


def test_enqueue_then_lost_ack_is_admitted_without_waiting_for_reconciliation() -> None:
    handled = []

    @command_handler(BuildReport)
    def handle(message: BuildReport) -> None:
        handled.append(message)

    transport = EnqueueThenFail()
    store = DjangoDurableJobStore()
    bus = Pybus(transport, durable_job_store=store, handler_targets=[handle])
    handle_ref = bus.schedule_command(BuildReport(report_id=12))

    with pytest.raises(IndeterminateDeliveryError):
        bus.create_durable_job_worker(error_delay=0).run(max_iterations=1)
    assert (
        DurableJob.objects.get(id=handle_ref.id).state == DurableJobState.INDETERMINATE
    )

    bus.listen_once(DEFAULT_QUEUE_NAME)

    assert handled == [BuildReport(report_id=12)]
    assert DurableJob.objects.get(id=handle_ref.id).state == DurableJobState.SUCCEEDED


def test_started_observer_failure_releases_admission_before_restoring() -> None:
    handled = []

    @command_handler(BuildReport)
    def handle(message: BuildReport) -> None:
        handled.append(message)

    def fail_started(outcome: CommandDeliveryOutcome) -> None:
        if outcome.status == CommandDeliveryStatus.STARTED:
            raise RuntimeError("observer unavailable")

    transport = MemoryTransport()
    store = DjangoDurableJobStore()
    failing_bus = Pybus(
        transport,
        durable_job_store=store,
        handler_targets=[handle],
        command_delivery_observers=[fail_started],
    )
    handle_ref = failing_bus.schedule_command(BuildReport(report_id=13))
    failing_bus.create_durable_job_worker(error_delay=0).run(max_iterations=1)

    with pytest.raises(DeliveryObservationError):
        failing_bus.listen_once(DEFAULT_QUEUE_NAME)

    released = DurableJob.objects.get(id=handle_ref.id)
    assert released.state == DurableJobState.PUBLISHED
    assert released.retry_count == 0
    assert transport.size(DEFAULT_QUEUE_NAME) == 1

    recovery_bus = Pybus(transport, durable_job_store=store, handler_targets=[handle])
    recovery_bus.listen_once(DEFAULT_QUEUE_NAME)
    assert handled == [BuildReport(report_id=13)]


def test_worker_claim_rejects_an_outer_application_transaction() -> None:
    bus = Pybus(MemoryTransport(), durable_job_store=DjangoDurableJobStore())
    bus.schedule_command(BuildReport(report_id=14))

    with transaction.atomic(), pytest.raises(WorkerAbortError, match="outside"):
        bus.create_durable_job_worker(error_delay=0).run(max_iterations=1)

    assert DurableJob.objects.get().state == DurableJobState.PENDING


def test_retry_recovery_preserves_count_and_last_attempt_timestamp() -> None:
    @command_handler(BuildReport)
    def fail(message: BuildReport) -> None:
        raise RuntimeError("retry")

    transport = FailOnPublication(publication=2)
    store = DjangoDurableJobStore()
    bus = Pybus(transport, durable_job_store=store, handler_targets=[fail])
    handle_ref = bus.schedule_command(BuildReport(report_id=15))
    bus.create_durable_job_worker(error_delay=0).run(max_iterations=1)

    with pytest.raises(IndeterminateDeliveryError):
        bus.listen_once(DEFAULT_QUEUE_NAME)
    DurableJob.objects.filter(id=handle_ref.id).update(
        reconciliation_due_at=django.utils.timezone.now() - timedelta(seconds=1)
    )
    bus.create_durable_job_worker(error_delay=0).run(max_iterations=1)

    raw = transport.consume(DEFAULT_QUEUE_NAME)
    envelope = MessageEnvelope.from_dict(bus.serializer.loads(raw))
    assert envelope.headers["retries"] == 1
    assert isinstance(envelope.headers["last_attempt"], str)


def test_retry_timestamp_survives_ambiguous_continuation_and_restart() -> None:
    attempts = 0

    @command_handler(BuildReport)
    def retry_then_continue(message: BuildReport) -> ContinueProcessing | None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("retry")
        return ContinueProcessing(queue=DEFAULT_SLOW_QUEUE_NAME)

    transport = FailOnPublication(publication=3)
    store = DjangoDurableJobStore()
    bus = Pybus(
        transport,
        durable_job_store=store,
        handler_targets=[retry_then_continue],
    )
    handle_ref = bus.schedule_command(BuildReport(report_id=25))
    bus.create_durable_job_worker(error_delay=0).run(max_iterations=1)
    bus.listen_once(DEFAULT_QUEUE_NAME)

    with pytest.raises(IndeterminateDeliveryError):
        bus.listen_once(DEFAULT_QUEUE_NAME)
    DurableJob.objects.filter(id=handle_ref.id).update(
        reconciliation_due_at=django.utils.timezone.now() - timedelta(seconds=1)
    )
    bus.create_durable_job_worker(error_delay=0).run(max_iterations=1)

    raw = transport.consume(DEFAULT_SLOW_QUEUE_NAME)
    envelope = MessageEnvelope.from_dict(bus.serializer.loads(raw))
    assert envelope.headers["retries"] == 1
    assert isinstance(envelope.headers["last_attempt"], str)


def test_confirmed_retry_receives_a_fresh_configured_reconciliation_deadline() -> None:
    @command_handler(BuildReport)
    def fail(message: BuildReport) -> None:
        raise RuntimeError("retry")

    policy = DurableJobPolicy(
        lease_duration=timedelta(minutes=1),
        reconciliation_delay=timedelta(minutes=40),
    )
    store = DjangoDurableJobStore()
    bus = Pybus(
        MemoryTransport(),
        durable_job_store=store,
        durable_job_policy=policy,
        handler_targets=[fail],
    )
    handle_ref = bus.schedule_command(BuildReport(report_id=23))
    bus.create_durable_job_worker(error_delay=0).run(max_iterations=1)
    first_deadline = DurableJob.objects.get(id=handle_ref.id).reconciliation_due_at

    bus.listen_once(DEFAULT_QUEUE_NAME)

    retry_deadline = DurableJob.objects.get(id=handle_ref.id).reconciliation_due_at
    assert retry_deadline > first_deadline
    assert retry_deadline > django.utils.timezone.now() + timedelta(minutes=39)


def test_shipped_migration_applies_and_reverses_from_zero(tmp_path: Path) -> None:
    database = tmp_path / "migration.sqlite3"
    script = textwrap.dedent(
        f"""
        from django.conf import settings
        settings.configure(
            INSTALLED_APPS=['pybus.integrations.django_durable'],
            DATABASES={{'default': {{
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': {str(database)!r},
            }}}},
            SECRET_KEY='migration-test',
        )
        import django
        django.setup()
        from django.apps import apps
        from django.core.management import call_command
        from django.db import connection
        call_command('migrate', 'pybus_durable', verbosity=0)
        assert 'pybus_durable_command' in connection.introspection.table_names()
        assert 'pybus_recurring_command_series' in connection.introspection.table_names()
        assert apps.get_model('pybus_durable', 'DurableJob') is not None
        assert apps.get_model('pybus_durable', 'JobSeries') is not None
        try:
            apps.get_model('pybus_durable', 'DurableCommand')
        except LookupError:
            pass
        else:
            raise AssertionError('legacy app-registry model label unexpectedly survived')
        call_command('migrate', 'pybus_durable', 'zero', verbosity=0)
        assert 'pybus_durable_command' not in connection.introspection.table_names()
        """
    )

    subprocess.run([sys.executable, "-c", script], check=True)


def test_job_ontology_migration_preserves_seeded_rows_forward_and_backward(
    tmp_path: Path,
) -> None:
    database = tmp_path / "job-ontology-migration.sqlite3"
    source_root = Path(__file__).resolve().parents[2] / "src"
    script = textwrap.dedent(
        f"""
        import sys
        from datetime import datetime, timezone
        from uuid import UUID

        sys.path.insert(0, {str(source_root)!r})
        from django.conf import settings
        settings.configure(
            INSTALLED_APPS=['pybus.integrations.django_durable'],
            DATABASES={{'default': {{
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': {str(database)!r},
            }}}},
            SECRET_KEY='job-ontology-migration-test',
        )
        import django
        django.setup()
        from django.db import connection
        from django.db.migrations.executor import MigrationExecutor

        old_target = ('pybus_durable', '0002_recurring_commands')
        new_target = ('pybus_durable', '0003_job_ontology')
        job_id = UUID('00000000-0000-0000-0000-000000000041')
        series_id = UUID('00000000-0000-0000-0000-000000000042')
        now = datetime(2026, 7, 18, tzinfo=timezone.utc)

        executor = MigrationExecutor(connection)
        executor.migrate([old_target])
        old_apps = executor.loader.project_state([old_target]).apps
        OldSeries = old_apps.get_model('pybus_durable', 'RecurringCommandSeries')
        OldJob = old_apps.get_model('pybus_durable', 'DurableCommand')
        OldSeries.objects.create(
            id=series_id,
            message_type='tests.RefreshIndex',
            version=1,
            payload={{'index_id': 41}},
            headers={{'actor_id': 'migration-test'}},
            queue='pybus.jobs',
            cadence='daily',
            timezone='UTC',
            starts_at=now,
            anchor_local='09:00:00',
            fingerprint='series-fingerprint',
            state='active',
            latest_occurrence_number=1,
            created_at=now,
        )
        OldJob.objects.create(
            id=job_id,
            message_id='job-ontology-41',
            message_type='tests.RefreshIndex',
            version=1,
            payload={{'index_id': 41}},
            headers={{'actor_id': 'migration-test'}},
            created_at=now,
            available_at=now,
            queue='pybus.jobs',
            fingerprint='job-fingerprint',
            state='pending',
            series_id=series_id,
            occurrence_number=1,
        )

        executor = MigrationExecutor(connection)
        executor.migrate([new_target])
        new_apps = executor.loader.project_state([new_target]).apps
        NewSeries = new_apps.get_model('pybus_durable', 'JobSeries')
        NewJob = new_apps.get_model('pybus_durable', 'DurableJob')
        new_series = NewSeries.objects.get(id=series_id)
        new_job = NewJob.objects.get(id=job_id)
        assert new_series.payload == {{'index_id': 41}}
        assert new_series.headers == {{'actor_id': 'migration-test'}}
        assert new_job.series_id == series_id
        assert new_job.occurrence_number == 1
        assert new_job.message_id == 'job-ontology-41'
        assert set(connection.introspection.table_names()) >= {{
            'pybus_durable_command',
            'pybus_recurring_command_series',
        }}

        executor = MigrationExecutor(connection)
        executor.migrate([old_target])
        old_apps = executor.loader.project_state([old_target]).apps
        RestoredSeries = old_apps.get_model(
            'pybus_durable', 'RecurringCommandSeries'
        )
        RestoredJob = old_apps.get_model('pybus_durable', 'DurableCommand')
        restored_series = RestoredSeries.objects.get(id=series_id)
        restored_job = RestoredJob.objects.get(id=job_id)
        assert restored_series.fingerprint == 'series-fingerprint'
        assert restored_job.fingerprint == 'job-fingerprint'
        assert restored_job.series_id == series_id
        """
    )

    subprocess.run([sys.executable, "-c", script], check=True)


def test_different_database_alias_is_explicitly_not_caller_atomic(
    tmp_path: Path,
) -> None:
    default_database = tmp_path / "default.sqlite3"
    durable_database = tmp_path / "durable.sqlite3"
    source_root = Path(__file__).resolve().parents[2] / "src"
    script = textwrap.dedent(
        f"""
        import sys
        sys.path.insert(0, {str(source_root)!r})
        from django.conf import settings
        settings.configure(
            INSTALLED_APPS=['pybus.integrations.django_durable'],
            DATABASES={{
                'default': {{'ENGINE': 'django.db.backends.sqlite3', 'NAME': {str(default_database)!r}}},
                'durable': {{'ENGINE': 'django.db.backends.sqlite3', 'NAME': {str(durable_database)!r}}},
            }},
            SECRET_KEY='alias-test',
        )
        import django
        django.setup()
        from django.core.management import call_command
        from django.db import transaction
        from pybus import Pybus, command
        from pybus.integrations.django_durable.models import DurableJob
        from pybus.integrations.django_durable.store import DjangoDurableJobStore
        from pybus.transports.memory import MemoryTransport
        call_command('migrate', 'pybus_durable', database='durable', verbosity=0)
        @command('alias.test')
        class AliasCommand:
            value: int
        try:
            with transaction.atomic(using='default'):
                Pybus(MemoryTransport(), durable_job_store=DjangoDurableJobStore(using='durable')).schedule_command(AliasCommand(1))
                raise RuntimeError('rollback caller')
        except RuntimeError:
            pass
        assert DurableJob.objects.using('durable').count() == 1
        """
    )

    subprocess.run([sys.executable, "-I", "-c", script], check=True)


def test_independent_connections_race_for_only_one_claim(tmp_path: Path) -> None:
    database = tmp_path / "claim-race.sqlite3"
    source_root = Path(__file__).resolve().parents[2] / "src"
    script = textwrap.dedent(
        f"""
        import sys
        sys.path.insert(0, {str(source_root)!r})
        from concurrent.futures import ThreadPoolExecutor
        from datetime import timedelta
        from threading import Barrier
        from django.conf import settings
        settings.configure(
            INSTALLED_APPS=['pybus.integrations.django_durable'],
            DATABASES={{'default': {{
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': {str(database)!r},
                'OPTIONS': {{'timeout': 10}},
            }}}},
            SECRET_KEY='race-test',
        )
        import django
        django.setup()
        from django.core.management import call_command
        from django.db import close_old_connections
        from django.utils import timezone
        from pybus import Pybus, command
        from pybus.integrations.django_durable.store import DjangoDurableJobStore
        from pybus.transports.memory import MemoryTransport
        call_command('migrate', 'pybus_durable', verbosity=0)
        @command('race.test')
        class RaceCommand:
            value: int
        Pybus(MemoryTransport(), durable_job_store=DjangoDurableJobStore()).schedule_command(RaceCommand(1))
        barrier = Barrier(2)
        def claim(worker):
            close_old_connections()
            barrier.wait()
            now = timezone.now()
            result = DjangoDurableJobStore().claim(
                worker_id=worker,
                now=now,
                lease_expires_at=now + timedelta(seconds=30),
            )
            close_old_connections()
            return None if result is None else result.id
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(claim, ['one', 'two']))
        assert len([result for result in results if result is not None]) == 1, results
        """
    )

    subprocess.run([sys.executable, "-I", "-c", script], check=True)


def test_independent_connections_share_one_idempotent_schedule(
    tmp_path: Path,
) -> None:
    database = tmp_path / "schedule-race.sqlite3"
    source_root = Path(__file__).resolve().parents[2] / "src"
    script = textwrap.dedent(
        f"""
        import sys
        sys.path.insert(0, {str(source_root)!r})
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        from django.conf import settings
        settings.configure(
            INSTALLED_APPS=['pybus.integrations.django_durable'],
            DATABASES={{'default': {{
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': {str(database)!r},
                'OPTIONS': {{'timeout': 10}},
            }}}},
            SECRET_KEY='schedule-race-test',
        )
        import django
        django.setup()
        from django.core.management import call_command
        from django.db import close_old_connections
        from pybus import Pybus, command
        from pybus.integrations.django_durable.models import DurableJob
        from pybus.integrations.django_durable.store import DjangoDurableJobStore
        from pybus.transports.memory import MemoryTransport
        call_command('migrate', 'pybus_durable', verbosity=0)
        @command('schedule.race')
        class RaceCommand:
            value: int
        barrier = Barrier(2)
        def schedule(value):
            close_old_connections()
            bus = Pybus(MemoryTransport(), durable_job_store=DjangoDurableJobStore())
            barrier.wait()
            result = bus.schedule_command(RaceCommand(value), idempotency_key='shared')
            close_old_connections()
            return result.id
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(schedule, [1, 1]))
        assert len(set(results)) == 1, results
        assert DurableJob.objects.count() == 1
        """
    )

    subprocess.run([sys.executable, "-I", "-c", script], check=True)
