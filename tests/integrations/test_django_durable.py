# ruff: noqa: E402

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from importlib import import_module
from pathlib import Path
import subprocess
import sys
import textwrap
from types import SimpleNamespace

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

from django.apps import apps
from django.db import connection, transaction

from pybus import ContinueProcessing, Pybus, command, command_handler
from pybus.delivery import CommandDeliveryOutcome, CommandDeliveryStatus
from pybus.durable import (
    DurableCommandPolicy,
    DurableCommandState,
    DurableDeliveryAdmission,
)
from pybus.envelope import MessageEnvelope
from pybus.exceptions import (
    DeliveryObservationError,
    DurableCommandConflictError,
    IndeterminateDeliveryError,
    WorkerAbortError,
)
from pybus.integrations.django_durable.models import DurableCommand
from pybus.integrations.django_durable.store import DjangoDurableCommandStore
from pybus.listener import DEFAULT_FAILED_QUEUE_NAME, DEFAULT_QUEUE_NAME
from pybus.queues import DEFAULT_SLOW_QUEUE_NAME
from pybus.retries import RetryPolicy
from pybus.transports.memory import MemoryTransport


@command("reports.build")
class BuildReport:
    report_id: int


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
    if DurableCommand._meta.db_table not in connection.introspection.table_names():
        with connection.schema_editor() as editor:
            editor.create_model(DurableCommand)
    DurableCommand.objects.all().delete()
    yield
    DurableCommand.objects.all().delete()


def test_schedule_participates_in_the_callers_database_transaction() -> None:
    bus = Pybus(MemoryTransport(), durable_command_store=DjangoDurableCommandStore())

    with pytest.raises(RuntimeError, match="rollback"):
        with transaction.atomic():
            bus.schedule_command(BuildReport(report_id=1))
            raise RuntimeError("rollback")

    assert DurableCommand.objects.count() == 0


def test_nested_savepoint_rollback_discards_only_inner_schedule() -> None:
    bus = Pybus(MemoryTransport(), durable_command_store=DjangoDurableCommandStore())

    with transaction.atomic():
        bus.schedule_command(BuildReport(report_id=20))
        with pytest.raises(RuntimeError, match="inner"):
            with transaction.atomic():
                bus.schedule_command(BuildReport(report_id=21))
                raise RuntimeError("inner")

    assert list(DurableCommand.objects.values_list("payload", flat=True)) == [
        {"report_id": 20}
    ]


def test_idempotency_key_conflicts_with_a_different_command() -> None:
    bus = Pybus(MemoryTransport(), durable_command_store=DjangoDurableCommandStore())
    bus.schedule_command(BuildReport(report_id=1), idempotency_key="report:1")

    with pytest.raises(DurableCommandConflictError):
        bus.schedule_command(BuildReport(report_id=2), idempotency_key="report:1")


def test_same_idempotency_key_and_command_returns_existing_handle() -> None:
    bus = Pybus(MemoryTransport(), durable_command_store=DjangoDurableCommandStore())

    first = bus.schedule_command(BuildReport(report_id=1), idempotency_key="report:1")
    second = bus.schedule_command(BuildReport(report_id=1), idempotency_key="report:1")

    assert second == first
    assert DurableCommand.objects.count() == 1


def test_reverse_migration_refuses_to_drop_nonempty_table() -> None:
    bus = Pybus(MemoryTransport(), durable_command_store=DjangoDurableCommandStore())
    bus.schedule_command(BuildReport(report_id=2))
    migration = import_module(
        "pybus.integrations.django_durable.migrations.0001_initial"
    )

    with pytest.raises(RuntimeError, match="contains data"):
        migration.refuse_nonempty_reverse(apps, SimpleNamespace(connection=connection))


def test_django_store_runs_the_command_to_an_irreversible_terminal_state() -> None:
    handled = []

    @command_handler(BuildReport)
    def handle(message: BuildReport) -> None:
        handled.append(message)

    transport = MemoryTransport()
    bus = Pybus(
        transport,
        durable_command_store=DjangoDurableCommandStore(),
        handler_targets=[handle],
    )
    handle_ref = bus.schedule_command(BuildReport(report_id=3))

    bus.create_durable_command_worker(error_delay=0).run(max_iterations=1)
    bus.listen_once(DEFAULT_QUEUE_NAME)

    record = DurableCommand.objects.get(id=handle_ref.id)
    assert handled == [BuildReport(report_id=3)]
    assert record.state == DurableCommandState.SUCCEEDED
    assert record.generation == 1


def test_late_mark_published_cannot_regress_a_fast_successful_handler() -> None:
    store = DjangoDurableCommandStore()
    bus = Pybus(MemoryTransport(), durable_command_store=store)
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

    assert (
        DurableCommand.objects.get(id=handle_ref.id).state
        == DurableCommandState.RUNNING
    )


def test_reconciliation_reclaims_with_a_new_generation_and_fences_old_copy() -> None:
    store = DjangoDurableCommandStore()
    bus = Pybus(MemoryTransport(), durable_command_store=store)
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
    store = DjangoDurableCommandStore()
    bus = Pybus(MemoryTransport(), durable_command_store=store)
    handle_ref = bus.schedule_command(BuildReport(report_id=6))
    now = django.utils.timezone.now()
    DurableCommand.objects.filter(id=handle_ref.id).update(
        state=DurableCommandState.RUNNING,
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

    DurableCommand.objects.filter(id=handle_ref.id).update(
        state=DurableCommandState.RUNNING,
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
        DurableCommand.objects.get(id=handle_ref.id).state
        == DurableCommandState.INDETERMINATE
    )


def test_crash_after_admission_respects_zero_retry_budget() -> None:
    store = DjangoDurableCommandStore()
    bus = Pybus(MemoryTransport(), durable_command_store=store)
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
    assert DurableCommand.objects.get().state == DurableCommandState.INDETERMINATE
    assert store.admit_delivery(admission).value == "drop"


def test_restart_cannot_increase_the_persisted_retry_budget() -> None:
    store = DjangoDurableCommandStore()
    bus = Pybus(MemoryTransport(), durable_command_store=store)
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
    record = DurableCommand.objects.get(id=claim.id)
    assert record.state == DurableCommandState.PUBLISHED
    assert record.max_retries == 0


def test_retry_then_dead_letter_updates_state_without_reviving_terminal_work() -> None:
    @command_handler(BuildReport)
    def fail(message: BuildReport) -> None:
        raise RuntimeError("reporting unavailable")

    transport = MemoryTransport()
    store = DjangoDurableCommandStore()
    bus = Pybus(transport, durable_command_store=store, handler_targets=[fail])
    bus.listener.retry_policy = RetryPolicy(max_retries=1)
    handle_ref = bus.schedule_command(BuildReport(report_id=7))
    bus.create_durable_command_worker(error_delay=0).run(max_iterations=1)

    bus.listen_once(DEFAULT_QUEUE_NAME)
    assert (
        DurableCommand.objects.get(id=handle_ref.id).state
        == DurableCommandState.PUBLISHED
    )
    bus.listen_once(DEFAULT_QUEUE_NAME)

    terminal = DurableCommand.objects.get(id=handle_ref.id)
    assert terminal.state == DurableCommandState.DEAD_LETTERED
    assert terminal.retry_count == 1
    assert terminal.finished_at is not None
    assert transport.size(DEFAULT_FAILED_QUEUE_NAME) == 1


def test_checkpointed_continuation_resumes_after_restart() -> None:
    @command_handler(BuildReport)
    def continue_later(message: BuildReport) -> ContinueProcessing:
        return ContinueProcessing(queue=DEFAULT_SLOW_QUEUE_NAME)

    transport = FailOnPublication(publication=2)
    store = DjangoDurableCommandStore()
    bus = Pybus(
        transport, durable_command_store=store, handler_targets=[continue_later]
    )
    handle_ref = bus.schedule_command(BuildReport(report_id=8))
    bus.create_durable_command_worker(error_delay=0).run(max_iterations=1)

    with pytest.raises(IndeterminateDeliveryError):
        bus.listen_once(DEFAULT_QUEUE_NAME)
    DurableCommand.objects.filter(id=handle_ref.id).update(
        reconciliation_due_at=django.utils.timezone.now() - timedelta(seconds=1)
    )

    bus.create_durable_command_worker(error_delay=0).run(max_iterations=1)

    recovered = DurableCommand.objects.get(id=handle_ref.id)
    assert recovered.state == DurableCommandState.PUBLISHED
    assert recovered.queue == DEFAULT_SLOW_QUEUE_NAME
    assert recovered.generation == 2
    assert transport.size(DEFAULT_SLOW_QUEUE_NAME) == 1


def test_duplicate_or_stale_outcomes_cannot_regress_terminal_state() -> None:
    store = DjangoDurableCommandStore()
    bus = Pybus(MemoryTransport(), durable_command_store=store)
    handle_ref = bus.schedule_command(BuildReport(report_id=9))
    DurableCommand.objects.filter(id=handle_ref.id).update(
        state=DurableCommandState.SUCCEEDED,
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

    terminal = DurableCommand.objects.get(id=handle_ref.id)
    assert terminal.state == DurableCommandState.SUCCEEDED
    assert terminal.retry_count == 0


def test_only_one_claim_owns_a_pending_generation() -> None:
    store = DjangoDurableCommandStore()
    bus = Pybus(MemoryTransport(), durable_command_store=store)
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
    store = DjangoDurableCommandStore()
    bus = Pybus(MemoryTransport(), durable_command_store=store)
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


def test_enqueue_then_lost_ack_is_admitted_without_waiting_for_reconciliation() -> None:
    handled = []

    @command_handler(BuildReport)
    def handle(message: BuildReport) -> None:
        handled.append(message)

    transport = EnqueueThenFail()
    store = DjangoDurableCommandStore()
    bus = Pybus(transport, durable_command_store=store, handler_targets=[handle])
    handle_ref = bus.schedule_command(BuildReport(report_id=12))

    with pytest.raises(IndeterminateDeliveryError):
        bus.create_durable_command_worker(error_delay=0).run(max_iterations=1)
    assert (
        DurableCommand.objects.get(id=handle_ref.id).state
        == DurableCommandState.INDETERMINATE
    )

    bus.listen_once(DEFAULT_QUEUE_NAME)

    assert handled == [BuildReport(report_id=12)]
    assert (
        DurableCommand.objects.get(id=handle_ref.id).state
        == DurableCommandState.SUCCEEDED
    )


def test_started_observer_failure_releases_admission_before_restoring() -> None:
    handled = []

    @command_handler(BuildReport)
    def handle(message: BuildReport) -> None:
        handled.append(message)

    def fail_started(outcome: CommandDeliveryOutcome) -> None:
        if outcome.status == CommandDeliveryStatus.STARTED:
            raise RuntimeError("observer unavailable")

    transport = MemoryTransport()
    store = DjangoDurableCommandStore()
    failing_bus = Pybus(
        transport,
        durable_command_store=store,
        handler_targets=[handle],
        command_delivery_observers=[fail_started],
    )
    handle_ref = failing_bus.schedule_command(BuildReport(report_id=13))
    failing_bus.create_durable_command_worker(error_delay=0).run(max_iterations=1)

    with pytest.raises(DeliveryObservationError):
        failing_bus.listen_once(DEFAULT_QUEUE_NAME)

    released = DurableCommand.objects.get(id=handle_ref.id)
    assert released.state == DurableCommandState.PUBLISHED
    assert released.retry_count == 0
    assert transport.size(DEFAULT_QUEUE_NAME) == 1

    recovery_bus = Pybus(
        transport, durable_command_store=store, handler_targets=[handle]
    )
    recovery_bus.listen_once(DEFAULT_QUEUE_NAME)
    assert handled == [BuildReport(report_id=13)]


def test_worker_claim_rejects_an_outer_application_transaction() -> None:
    bus = Pybus(MemoryTransport(), durable_command_store=DjangoDurableCommandStore())
    bus.schedule_command(BuildReport(report_id=14))

    with transaction.atomic(), pytest.raises(WorkerAbortError, match="outside"):
        bus.create_durable_command_worker(error_delay=0).run(max_iterations=1)

    assert DurableCommand.objects.get().state == DurableCommandState.PENDING


def test_retry_recovery_preserves_count_and_last_attempt_timestamp() -> None:
    @command_handler(BuildReport)
    def fail(message: BuildReport) -> None:
        raise RuntimeError("retry")

    transport = FailOnPublication(publication=2)
    store = DjangoDurableCommandStore()
    bus = Pybus(transport, durable_command_store=store, handler_targets=[fail])
    handle_ref = bus.schedule_command(BuildReport(report_id=15))
    bus.create_durable_command_worker(error_delay=0).run(max_iterations=1)

    with pytest.raises(IndeterminateDeliveryError):
        bus.listen_once(DEFAULT_QUEUE_NAME)
    DurableCommand.objects.filter(id=handle_ref.id).update(
        reconciliation_due_at=django.utils.timezone.now() - timedelta(seconds=1)
    )
    bus.create_durable_command_worker(error_delay=0).run(max_iterations=1)

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
    store = DjangoDurableCommandStore()
    bus = Pybus(
        transport,
        durable_command_store=store,
        handler_targets=[retry_then_continue],
    )
    handle_ref = bus.schedule_command(BuildReport(report_id=25))
    bus.create_durable_command_worker(error_delay=0).run(max_iterations=1)
    bus.listen_once(DEFAULT_QUEUE_NAME)

    with pytest.raises(IndeterminateDeliveryError):
        bus.listen_once(DEFAULT_QUEUE_NAME)
    DurableCommand.objects.filter(id=handle_ref.id).update(
        reconciliation_due_at=django.utils.timezone.now() - timedelta(seconds=1)
    )
    bus.create_durable_command_worker(error_delay=0).run(max_iterations=1)

    raw = transport.consume(DEFAULT_SLOW_QUEUE_NAME)
    envelope = MessageEnvelope.from_dict(bus.serializer.loads(raw))
    assert envelope.headers["retries"] == 1
    assert isinstance(envelope.headers["last_attempt"], str)


def test_confirmed_retry_receives_a_fresh_configured_reconciliation_deadline() -> None:
    @command_handler(BuildReport)
    def fail(message: BuildReport) -> None:
        raise RuntimeError("retry")

    policy = DurableCommandPolicy(
        lease_duration=timedelta(minutes=1),
        reconciliation_delay=timedelta(minutes=40),
    )
    store = DjangoDurableCommandStore()
    bus = Pybus(
        MemoryTransport(),
        durable_command_store=store,
        durable_command_policy=policy,
        handler_targets=[fail],
    )
    handle_ref = bus.schedule_command(BuildReport(report_id=23))
    bus.create_durable_command_worker(error_delay=0).run(max_iterations=1)
    first_deadline = DurableCommand.objects.get(id=handle_ref.id).reconciliation_due_at

    bus.listen_once(DEFAULT_QUEUE_NAME)

    retry_deadline = DurableCommand.objects.get(id=handle_ref.id).reconciliation_due_at
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
        from django.core.management import call_command
        from django.db import connection
        call_command('migrate', 'pybus_durable', verbosity=0)
        assert 'pybus_durable_command' in connection.introspection.table_names()
        call_command('migrate', 'pybus_durable', 'zero', verbosity=0)
        assert 'pybus_durable_command' not in connection.introspection.table_names()
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
        from pybus.integrations.django_durable.models import DurableCommand
        from pybus.integrations.django_durable.store import DjangoDurableCommandStore
        from pybus.transports.memory import MemoryTransport
        call_command('migrate', 'pybus_durable', database='durable', verbosity=0)
        @command('alias.test')
        class AliasCommand:
            value: int
        try:
            with transaction.atomic(using='default'):
                Pybus(MemoryTransport(), durable_command_store=DjangoDurableCommandStore(using='durable')).schedule_command(AliasCommand(1))
                raise RuntimeError('rollback caller')
        except RuntimeError:
            pass
        assert DurableCommand.objects.using('durable').count() == 1
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
        from pybus.integrations.django_durable.store import DjangoDurableCommandStore
        from pybus.transports.memory import MemoryTransport
        call_command('migrate', 'pybus_durable', verbosity=0)
        @command('race.test')
        class RaceCommand:
            value: int
        Pybus(MemoryTransport(), durable_command_store=DjangoDurableCommandStore()).schedule_command(RaceCommand(1))
        barrier = Barrier(2)
        def claim(worker):
            close_old_connections()
            barrier.wait()
            now = timezone.now()
            result = DjangoDurableCommandStore().claim(
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
        from pybus.integrations.django_durable.models import DurableCommand
        from pybus.integrations.django_durable.store import DjangoDurableCommandStore
        from pybus.transports.memory import MemoryTransport
        call_command('migrate', 'pybus_durable', verbosity=0)
        @command('schedule.race')
        class RaceCommand:
            value: int
        barrier = Barrier(2)
        def schedule(value):
            close_old_connections()
            bus = Pybus(MemoryTransport(), durable_command_store=DjangoDurableCommandStore())
            barrier.wait()
            result = bus.schedule_command(RaceCommand(value), idempotency_key='shared')
            close_old_connections()
            return result.id
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(schedule, [1, 1]))
        assert len(set(results)) == 1, results
        assert DurableCommand.objects.count() == 1
        """
    )

    subprocess.run([sys.executable, "-I", "-c", script], check=True)
