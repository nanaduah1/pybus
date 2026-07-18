from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from time import monotonic

import pytest

import pybus
from pybus import (
    BusConfiguration,
    Pybus,
    Recurrence,
    RecurrenceCadence,
    command,
    command_handler,
    event,
    event_handler,
)
from pybus.delivery import CommandDeliveryStatus
from pybus.durable import (
    DurableCommandClaim,
    DurableCommandDraft,
    DurableCommandHandle,
    DurableCommandPolicy,
    DurableCommandRecord,
    DurableCommandState,
    DurableDeliveryDecision,
)
from pybus.envelope import MessageEnvelope
from pybus.exceptions import DurableCommandsNotConfiguredError
from pybus.exceptions import (
    DurableRecurrenceNotSupportedError,
    IndeterminateDeliveryError,
    InvalidMessageDefinitionError,
    WorkerAbortError,
)
from pybus.recurrence import (
    RecurringCommandSeriesRecord,
    RecurringCommandSeriesState,
)
from pybus.listener import DEFAULT_QUEUE_NAME
from pybus.retries import RetryPolicy
from pybus.transports.memory import MemoryTransport


@command("billing.generate")
class GenerateBill:
    student_id: int


@event("billing.audited")
class BillingAudited:
    student_id: int


class RecordingDurableStore:
    def __init__(self) -> None:
        self.records: dict[str, DurableCommandRecord] = {}
        self.scheduled: list[DurableCommandDraft] = []
        self.claims: list[DurableCommandClaim] = []
        self.published: list[DurableCommandClaim] = []
        self.indeterminate: list[DurableCommandClaim] = []
        self.admission = DurableDeliveryDecision.PROCEED
        self.admissions = []
        self.outcomes = []
        self.settlements = []
        self.releases = []

    def schedule(self, draft: DurableCommandDraft) -> DurableCommandRecord:
        self.scheduled.append(draft)
        existing = next(
            (
                record
                for record in self.records.values()
                if draft.idempotency_key is not None
                and record.idempotency_key == draft.idempotency_key
            ),
            None,
        )
        if existing is not None:
            return existing
        record = DurableCommandRecord.from_draft(draft)
        self.records[record.id] = record
        return record

    def claim(self, *, worker_id, now, lease_expires_at):
        record = next(
            (
                value
                for value in self.records.values()
                if value.state == DurableCommandState.PENDING
            ),
            None,
        )
        if record is None:
            return None
        claimed = replace(
            record,
            state=DurableCommandState.CLAIMED,
            generation=record.generation + 1,
            lease_owner=worker_id,
            lease_expires_at=lease_expires_at,
        )
        self.records[record.id] = claimed
        claim = DurableCommandClaim.from_record(claimed)
        self.claims.append(claim)
        return claim

    def mark_published(self, claim, *, now, reconciliation_due_at):
        self.published.append(claim)
        current = self.records[claim.id]
        self.records[claim.id] = replace(
            current,
            state=DurableCommandState.PUBLISHED,
            reconciliation_due_at=reconciliation_due_at,
        )

    def mark_publish_indeterminate(self, claim, *, now, reconciliation_due_at):
        self.indeterminate.append(claim)

    def admit_delivery(self, admission):
        self.admissions.append(admission)
        return self.admission

    def checkpoint_settlement(self, settlement):
        self.settlements.append(settlement)

    def apply_outcome(self, outcome, *, reconciliation_due_at):
        self.outcomes.append(outcome)

    def release_delivery(self, *, record_id, generation, reconciliation_due_at) -> None:
        self.releases.append((record_id, generation, reconciliation_due_at))


class RecordingRecurringStore(RecordingDurableStore):
    def __init__(self) -> None:
        super().__init__()
        self.series = {}

    def schedule_recurring(self, draft):
        record = DurableCommandRecord.from_draft(draft.first_occurrence)
        series_id = f"series-{len(self.series) + 1}"
        record = replace(record, series_id=series_id, occurrence_number=1)
        self.records[record.id] = record
        series = RecurringCommandSeriesRecord(
            id=series_id,
            state=RecurringCommandSeriesState.ACTIVE,
            cadence=draft.recurrence.cadence,
            timezone=draft.recurrence.timezone,
            starts_at=draft.starts_at,
            ends_at=draft.recurrence.ends_at,
            fingerprint=draft.fingerprint,
            idempotency_key=draft.idempotency_key,
            latest_occurrence_number=1,
            latest_occurrence_id=record.id,
            latest_message_id=record.message_id,
            latest_run_at=record.available_at,
            created_at=record.created_at,
        )
        self.series[series_id] = series
        return series, record

    def cancel_recurring(self, *, series_id, cancelled_at):
        return self.series[series_id]

    def complete_recurring_success(
        self, outcome, handler_result, *, completed_at
    ) -> bool:
        return outcome.durable_record_id in self.records and bool(
            self.records[outcome.durable_record_id].series_id
        )


def test_schedule_command_is_concise_and_performs_no_transport_io() -> None:
    transport = MemoryTransport()
    store = RecordingDurableStore()
    bus = Pybus(transport, durable_command_store=store)

    handle = bus.schedule_command(GenerateBill(student_id=7))

    assert isinstance(handle, DurableCommandHandle)
    assert handle.message_id == store.scheduled[0].message_id
    assert store.scheduled[0].queue == DEFAULT_QUEUE_NAME
    assert transport.size(DEFAULT_QUEUE_NAME) == 0


def test_schedule_command_accepts_future_and_recurring_context() -> None:
    store = RecordingRecurringStore()
    bus = Pybus(MemoryTransport(), durable_command_store=store)
    run_at = datetime(2027, 1, 31, 9, tzinfo=timezone.utc)

    handle = bus.schedule_command(
        GenerateBill(student_id=7),
        run_at=run_at,
        recurrence=Recurrence(RecurrenceCadence.MONTHLY),
    )

    assert handle.series_id == "series-1"
    assert handle.occurrence_number == 1
    assert store.records[handle.id].available_at == run_at


def test_future_one_off_uses_run_at_without_requiring_recurrence_support() -> None:
    store = RecordingDurableStore()
    bus = Pybus(MemoryTransport(), durable_command_store=store)
    run_at = datetime(2027, 1, 31, 9, tzinfo=timezone.utc)

    handle = bus.schedule_command(GenerateBill(student_id=7), run_at=run_at)

    assert store.records[handle.id].available_at == run_at


def test_recurrence_requires_an_explicit_store_capability() -> None:
    bus = Pybus(MemoryTransport(), durable_command_store=RecordingDurableStore())

    with pytest.raises(DurableRecurrenceNotSupportedError):
        bus.schedule_command(
            GenerateBill(student_id=7),
            recurrence=Recurrence(RecurrenceCadence.DAILY),
        )


def test_top_level_schedule_command_uses_the_configured_bus() -> None:
    store = RecordingDurableStore()
    configuration = BusConfiguration(
        transport_factory=MemoryTransport,
        durable_command_store_factory=lambda: store,
    )
    configuration.configure()

    handle = pybus.schedule_command(GenerateBill(student_id=8))

    assert handle.id in store.records


def test_durable_worker_uses_configured_policy_and_accepts_an_override() -> None:
    configured = DurableCommandPolicy(
        lease_duration=timedelta(minutes=2),
        reconciliation_delay=timedelta(minutes=20),
    )
    override = DurableCommandPolicy(
        lease_duration=timedelta(minutes=3),
        reconciliation_delay=timedelta(minutes=30),
    )
    bus = BusConfiguration(
        transport_factory=MemoryTransport,
        durable_command_store_factory=RecordingDurableStore,
        durable_command_policy=configured,
    ).create()

    worker = bus.create_durable_command_worker()
    overridden_worker = bus.create_durable_command_worker(policy=override)

    assert worker.listener.runner.policy == configured
    assert overridden_worker.listener.runner.policy == override


@pytest.mark.parametrize(
    "kwargs",
    [
        {"lease_duration": timedelta(0)},
        {"reconciliation_delay": timedelta(0)},
    ],
)
def test_durable_policy_requires_positive_intervals(kwargs) -> None:
    with pytest.raises(ValueError):
        DurableCommandPolicy(**kwargs)


def test_empty_durable_worker_paces_database_polling() -> None:
    bus = Pybus(MemoryTransport(), durable_command_store=RecordingDurableStore())
    started = monotonic()

    bus.create_durable_command_worker(idle_delay=0.01).run(max_iterations=1)

    assert monotonic() - started >= 0.008


def test_schedule_command_requires_explicit_durable_configuration() -> None:
    bus = Pybus(MemoryTransport())

    with pytest.raises(DurableCommandsNotConfiguredError):
        bus.schedule_command(GenerateBill(student_id=7))
    with pytest.raises(DurableCommandsNotConfiguredError):
        bus.create_durable_command_worker()


@pytest.mark.parametrize("idempotency_key", ["", " ", 42, "x" * 256])
def test_schedule_command_validates_idempotency_keys(idempotency_key) -> None:
    bus = Pybus(MemoryTransport(), durable_command_store=RecordingDurableStore())

    with pytest.raises(InvalidMessageDefinitionError, match="idempotency_key"):
        bus.schedule_command(
            GenerateBill(student_id=7), idempotency_key=idempotency_key
        )


def test_durable_worker_publishes_a_generation_fenced_delivery_copy() -> None:
    transport = MemoryTransport()
    store = RecordingDurableStore()
    bus = Pybus(transport, durable_command_store=store)
    handle = bus.schedule_command(GenerateBill(student_id=9))

    worker = bus.create_durable_command_worker(error_delay=0)
    worker.run(max_iterations=1)

    raw = transport.consume(DEFAULT_QUEUE_NAME)
    envelope = MessageEnvelope.from_dict(bus.serializer.loads(raw))
    assert envelope.message_id == handle.message_id
    assert envelope.payload == {"student_id": 9}
    assert envelope.headers["pybus_durable_record"] == handle.id
    assert envelope.headers["pybus_durable_generation"] == 1
    assert store.published[0].message_id == handle.message_id


def test_unknown_publish_acknowledgement_is_recorded_and_aborts_worker() -> None:
    class FailingTransport(MemoryTransport):
        def publish(self, channel: str, message: bytes) -> None:
            raise RuntimeError("unknown acknowledgement")

    store = RecordingDurableStore()
    bus = Pybus(FailingTransport(), durable_command_store=store)
    bus.schedule_command(GenerateBill(student_id=99))

    with pytest.raises(IndeterminateDeliveryError):
        bus.create_durable_command_worker(error_delay=0).run(max_iterations=1)

    assert len(store.indeterminate) == 1
    assert store.published == []


@pytest.mark.parametrize("failure", ["published", "indeterminate"])
def test_post_publication_state_failures_abort_as_indeterminate(failure) -> None:
    class FailingStore(RecordingDurableStore):
        def mark_published(self, claim, *, now, reconciliation_due_at):
            if failure == "published":
                raise RuntimeError("database unavailable")
            super().mark_published(
                claim, now=now, reconciliation_due_at=reconciliation_due_at
            )

        def mark_publish_indeterminate(self, claim, *, now, reconciliation_due_at):
            if failure == "indeterminate":
                raise RuntimeError("database unavailable")
            super().mark_publish_indeterminate(
                claim, now=now, reconciliation_due_at=reconciliation_due_at
            )

    class PublishBehavior(MemoryTransport):
        def publish(self, channel: str, message: bytes) -> None:
            if failure == "indeterminate":
                raise RuntimeError("unknown acknowledgement")
            super().publish(channel, message)

    bus = Pybus(PublishBehavior(), durable_command_store=FailingStore())
    bus.schedule_command(GenerateBill(student_id=100))

    with pytest.raises(IndeterminateDeliveryError):
        bus.create_durable_command_worker(error_delay=0).run(max_iterations=1)


def test_durable_envelope_requires_a_store_on_the_consuming_worker() -> None:
    handled = []

    @command_handler(GenerateBill)
    def handle(message: GenerateBill) -> None:
        handled.append(message)

    transport = MemoryTransport()
    publishing_bus = Pybus(transport, durable_command_store=RecordingDurableStore())
    publishing_bus.schedule_command(GenerateBill(student_id=101))
    publishing_bus.create_durable_command_worker(error_delay=0).run(max_iterations=1)
    consuming_bus = Pybus(transport, handler_targets=[handle])

    with pytest.raises(WorkerAbortError, match="not configured"):
        consuming_bus.listen_once(DEFAULT_QUEUE_NAME)

    assert handled == []
    assert transport.size(DEFAULT_QUEUE_NAME) == 1


def test_durable_configuration_does_not_change_ordinary_multi_handler_commands() -> (
    None
):
    handled = []

    @command_handler(GenerateBill, allow_multiple=True)
    def first(message: GenerateBill) -> None:
        handled.append("first")

    @command_handler(GenerateBill, allow_multiple=True)
    def second(message: GenerateBill) -> None:
        handled.append("second")

    bus = Pybus(
        MemoryTransport(),
        durable_command_store=RecordingDurableStore(),
        handler_targets=[first, second],
    )
    bus.send_command(GenerateBill(student_id=102))

    bus.listen_once(DEFAULT_QUEUE_NAME)

    assert handled == ["first", "second"]


@pytest.mark.parametrize("message_kind", ["event", "request", "response"])
def test_durable_identity_is_rejected_on_non_command_envelopes(message_kind) -> None:
    handled = []

    @event_handler(BillingAudited)
    def handle(message: BillingAudited) -> None:
        handled.append(message)

    transport = MemoryTransport()
    store = RecordingDurableStore()
    bus = Pybus(transport, durable_command_store=store, handler_targets=[handle])
    envelope = MessageEnvelope.create(
        message_id="not-a-command",
        message_type="billing.audited",
        message_kind=message_kind,
        version=1,
        payload={"student_id": 7},
        headers={
            "pybus_durable_record": "record-1",
            "pybus_durable_generation": 1,
        },
    )
    bus.publish_prepared(envelope)

    with pytest.raises(WorkerAbortError, match="only on commands"):
        bus.listen_once(DEFAULT_QUEUE_NAME)

    assert handled == []
    assert store.admissions == []
    assert transport.size(DEFAULT_QUEUE_NAME) == 1


def test_idempotency_key_returns_the_existing_logical_command() -> None:
    store = RecordingDurableStore()
    bus = Pybus(MemoryTransport(), durable_command_store=store)

    first = bus.schedule_command(GenerateBill(student_id=10), idempotency_key="bill:10")
    second = bus.schedule_command(
        GenerateBill(student_id=10), idempotency_key="bill:10"
    )

    assert second == first
    assert len(store.records) == 1


def test_record_and_claim_timestamps_require_aware_utc_values() -> None:
    bus = Pybus(MemoryTransport(), durable_command_store=RecordingDurableStore())
    handle = bus.schedule_command(GenerateBill(student_id=11))

    assert handle.created_at.tzinfo == timezone.utc
    assert isinstance(handle.created_at, datetime)


def test_current_generation_is_admitted_and_terminal_outcome_is_persisted() -> None:
    handled = []

    @command_handler(GenerateBill)
    def handle(message: GenerateBill) -> None:
        handled.append(message)

    store = RecordingDurableStore()
    bus = Pybus(
        MemoryTransport(), durable_command_store=store, handler_targets=[handle]
    )
    bus.schedule_command(GenerateBill(student_id=12))
    bus.create_durable_command_worker(error_delay=0).run(max_iterations=1)

    bus.listen_once(DEFAULT_QUEUE_NAME)

    assert handled == [GenerateBill(student_id=12)]
    assert [outcome.status for outcome in store.outcomes] == [
        CommandDeliveryStatus.STARTED,
        CommandDeliveryStatus.SUCCEEDED,
    ]
    assert store.outcomes[-1].durable_record_id is not None
    assert store.outcomes[-1].durable_generation == 1


def test_stale_or_terminal_delivery_is_consumed_without_running_handler() -> None:
    handled = []

    @command_handler(GenerateBill)
    def handle(message: GenerateBill) -> None:
        handled.append(message)

    store = RecordingDurableStore()
    store.admission = DurableDeliveryDecision.DROP
    transport = MemoryTransport()
    bus = Pybus(transport, durable_command_store=store, handler_targets=[handle])
    bus.schedule_command(GenerateBill(student_id=13))
    bus.create_durable_command_worker(error_delay=0).run(max_iterations=1)

    assert bus.listen_once(DEFAULT_QUEUE_NAME) is None
    assert handled == []
    assert transport.size(DEFAULT_QUEUE_NAME) == 0


def test_stale_delivery_is_dropped_before_typed_payload_decoding() -> None:
    handled = []

    @command_handler(GenerateBill)
    def handle(message: GenerateBill) -> None:
        handled.append(message)

    store = RecordingDurableStore()
    store.admission = DurableDeliveryDecision.DROP
    transport = MemoryTransport()
    bus = Pybus(transport, durable_command_store=store, handler_targets=[handle])
    bus.schedule_command(GenerateBill(student_id=13))
    bus.create_durable_command_worker(error_delay=0).run(max_iterations=1)
    raw = transport.consume(DEFAULT_QUEUE_NAME)
    envelope = MessageEnvelope.from_dict(bus.serializer.loads(raw))
    envelope.payload = {"not_student_id": 13}
    transport.publish(DEFAULT_QUEUE_NAME, bus.serializer.dump(envelope))

    assert bus.listen_once(DEFAULT_QUEUE_NAME) is None
    assert handled == []
    assert transport.size(DEFAULT_QUEUE_NAME) == 0


def test_retry_settlement_is_checkpointed_before_the_delivery_outcome() -> None:
    @command_handler(GenerateBill)
    def fail(message: GenerateBill) -> None:
        raise RuntimeError("try again")

    store = RecordingDurableStore()
    bus = Pybus(MemoryTransport(), durable_command_store=store, handler_targets=[fail])
    bus.listener.retry_policy = RetryPolicy(max_retries=1)
    bus.schedule_command(GenerateBill(student_id=14))
    bus.create_durable_command_worker(error_delay=0).run(max_iterations=1)

    bus.listen_once(DEFAULT_QUEUE_NAME)

    assert store.settlements[0].status == CommandDeliveryStatus.RETRY_SCHEDULED
    assert store.settlements[0].retry_count == 1
    assert store.outcomes[-1].status == CommandDeliveryStatus.RETRY_SCHEDULED
