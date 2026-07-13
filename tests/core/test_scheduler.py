from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json

import pytest

import pybus
from pybus.scheduling import InMemoryScheduleStateStore, Scheduler, get_scheduler


def state_key(identity: str) -> str:
    return f"pybus.scheduler.state:{identity}"


def test_package_root_exposes_scheduler_primitives() -> None:
    assert pybus.Scheduler is Scheduler
    assert pybus.scheduled is not None
    assert pybus.configure_scheduler is not None
    assert pybus.get_scheduler is not None
    assert pybus.InMemoryScheduleStateStore is InMemoryScheduleStateStore


def test_configure_scheduler_replaces_global_scheduler() -> None:
    store = InMemoryScheduleStateStore()
    configured = pybus.configure_scheduler(state_store=store)

    assert configured is get_scheduler()
    assert configured.state_store is store


def test_scheduled_decorator_registers_interval_task() -> None:
    current = datetime(2026, 7, 12, 12, 0, tzinfo=timezone.utc)
    scheduler = Scheduler(
        state_store=InMemoryScheduleStateStore(),
        now_fn=lambda: current,
    )

    @scheduler.scheduled(interval=300, priority="low")
    def refresh_cache() -> None:
        return None

    task = scheduler.tasks()[
        f"{__name__}:test_scheduled_decorator_registers_interval_task.<locals>.refresh_cache"
    ]

    assert refresh_cache is not None
    assert task.name == "refresh_cache"
    assert task.identity.endswith(
        ":test_scheduled_decorator_registers_interval_task.<locals>.refresh_cache"
    )
    assert task.interval == 300
    assert task.options["priority"] == "low"
    assert task.due == current + timedelta(seconds=300)


def test_scheduled_decorator_uses_days_hour_and_minute_for_cron_tasks() -> None:
    current = datetime(2026, 7, 12, 21, 0, tzinfo=timezone.utc)  # Sunday
    scheduler = Scheduler(now_fn=lambda: current)

    @scheduler.scheduled(days=[0, 2, 4], hour=20, minute=15)
    def run_report() -> None:
        return None

    identity = f"{__name__}:test_scheduled_decorator_uses_days_hour_and_minute_for_cron_tasks.<locals>.run_report"
    task = scheduler.tasks()[identity]

    assert task.days == (0, 2, 4)
    assert task.due == datetime(2026, 7, 13, 20, 15, tzinfo=timezone.utc)


def test_run_due_tasks_executes_and_reschedules_interval_task() -> None:
    state_store = InMemoryScheduleStateStore()
    current = datetime(2026, 7, 12, 12, 0, tzinfo=timezone.utc)
    calls: list[str] = []
    scheduler = Scheduler(state_store=state_store, now_fn=lambda: current)

    @scheduler.scheduled(interval=120)
    def send_digest() -> None:
        calls.append("ran")

    identity = f"{__name__}:test_run_due_tasks_executes_and_reschedules_interval_task.<locals>.send_digest"
    task = scheduler.tasks()[identity]
    task.due = current

    scheduler.run_due_tasks(now=current)

    assert calls == ["ran"]
    assert task.last_run == current
    assert task.due == current + timedelta(seconds=120)
    stored = json.loads(state_store.get(state_key(identity)))
    assert stored == {
        "due": (current + timedelta(seconds=120)).isoformat(),
        "failures": 0,
        "last_failure": None,
        "last_run": current.isoformat(),
        "version": 1,
    }


def test_run_due_tasks_applies_backoff_after_failure() -> None:
    current = datetime(2026, 7, 12, 12, 0, tzinfo=timezone.utc)
    scheduler = Scheduler(now_fn=lambda: current)

    @scheduler.scheduled(interval=60)
    def flaky_job() -> None:
        raise RuntimeError("boom")

    identity = f"{__name__}:test_run_due_tasks_applies_backoff_after_failure.<locals>.flaky_job"
    task = scheduler.tasks()[identity]
    task.due = current

    with pytest.raises(RuntimeError, match="boom"):
        scheduler.run_due_tasks(now=current)

    assert task.failures == 1
    assert task.last_failure == current
    assert task.due == current + timedelta(seconds=120)


def test_transient_exceptions_retry_before_backoff() -> None:
    current = datetime(2026, 7, 12, 12, 0, tzinfo=timezone.utc)
    scheduler = Scheduler(now_fn=lambda: current)
    attempts = {"count": 0}

    @scheduler.scheduled(interval=60)
    def flaky_once() -> None:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("retry me")

    identity = (
        f"{__name__}:test_transient_exceptions_retry_before_backoff.<locals>.flaky_once"
    )
    task = scheduler.tasks()[identity]
    task.due = current

    scheduler.run_due_tasks(
        now=current,
        retries=1,
        transient_exceptions=(RuntimeError,),
    )

    assert attempts["count"] == 2
    assert task.failures == 0
    assert task.last_run == current


def test_root_scheduled_uses_configured_global_scheduler() -> None:
    current = datetime(2026, 7, 12, 12, 0, tzinfo=timezone.utc)
    scheduler = pybus.configure_scheduler(
        state_store=InMemoryScheduleStateStore(),
        now_fn=lambda: current,
    )

    @pybus.scheduled(interval=90)
    def global_task() -> None:
        return None

    identity = f"{__name__}:test_root_scheduled_uses_configured_global_scheduler.<locals>.global_task"
    assert identity in scheduler.tasks()
    assert scheduler.tasks()[identity].due == current + timedelta(seconds=90)


def test_same_named_functions_in_different_modules_have_distinct_identities() -> None:
    current = datetime(2026, 7, 13, 12, 0, tzinfo=timezone.utc)
    scheduler = Scheduler(now_fn=lambda: current)

    def make_task(module: str):
        def refresh() -> None:
            return None

        refresh.__module__ = module
        return refresh

    first = scheduler.scheduled(interval=60)(make_task("reports.daily"))
    second = scheduler.scheduled(interval=60)(make_task("billing.daily"))

    assert first.__name__ == second.__name__ == "refresh"
    assert set(scheduler.tasks()) == {
        "reports.daily:test_same_named_functions_in_different_modules_have_distinct_identities.<locals>.make_task.<locals>.refresh",
        "billing.daily:test_same_named_functions_in_different_modules_have_distinct_identities.<locals>.make_task.<locals>.refresh",
    }


def test_explicit_identity_controls_registry_and_state_key() -> None:
    current = datetime(2026, 7, 13, 12, 0, tzinfo=timezone.utc)
    store = InMemoryScheduleStateStore()
    scheduler = Scheduler(state_store=store, now_fn=lambda: current)

    @scheduler.scheduled(interval=60, identity="billing.refresh")
    def refresh() -> None:
        return None

    task = scheduler.tasks()["billing.refresh"]
    task.due = current
    scheduler.run_due_tasks(now=current)

    assert task.identity == "billing.refresh"
    assert store.get(state_key("billing.refresh")) is not None


@pytest.mark.parametrize("identity", ["", "   "])
def test_explicit_identity_must_not_be_empty(identity: str) -> None:
    scheduler = Scheduler()

    with pytest.raises(ValueError, match="identity must not be empty"):

        @scheduler.scheduled(interval=60, identity=identity)
        def refresh() -> None:
            return None


def test_duplicate_identity_is_rejected_before_state_read() -> None:
    class CountingStore(InMemoryScheduleStateStore):
        def __init__(self) -> None:
            super().__init__()
            self.get_calls = 0

        def get(self, key: str) -> str | None:
            self.get_calls += 1
            return super().get(key)

    store = CountingStore()
    scheduler = Scheduler(state_store=store)

    @scheduler.scheduled(interval=60, identity="shared")
    def first() -> None:
        return None

    with pytest.raises(ValueError, match="Duplicate scheduled task identity: shared"):

        @scheduler.scheduled(interval=60, identity="shared")
        def second() -> None:
            return None

    assert store.get_calls == 1


def test_duplicate_default_identity_is_rejected() -> None:
    scheduler = Scheduler()

    def refresh() -> None:
        return None

    scheduler.scheduled(interval=60)(refresh)

    with pytest.raises(ValueError, match="Duplicate scheduled task identity"):
        scheduler.scheduled(interval=120)(refresh)


def test_due_task_failure_does_not_prevent_later_due_task() -> None:
    current = datetime(2026, 7, 13, 12, 0, tzinfo=timezone.utc)
    calls: list[str] = []
    scheduler = Scheduler(now_fn=lambda: current)

    @scheduler.scheduled(interval=60, identity="first")
    def first() -> None:
        calls.append("first")
        raise RuntimeError("boom")

    @scheduler.scheduled(interval=60, identity="second")
    def second() -> None:
        calls.append("second")

    scheduler.tasks()["first"].due = current
    scheduler.tasks()["second"].due = current

    with pytest.raises(RuntimeError, match="boom"):
        scheduler.run_due_tasks(now=current)

    assert calls == ["first", "second"]
    assert scheduler.tasks()["first"].due == current + timedelta(seconds=120)


def test_completed_cron_state_prevents_repeat_after_restart() -> None:
    first_now = datetime(2026, 7, 13, 10, 0, tzinfo=timezone.utc)
    store = InMemoryScheduleStateStore()
    first_calls: list[str] = []
    first_scheduler = Scheduler(state_store=store, now_fn=lambda: first_now)

    @first_scheduler.scheduled(hour=23, identity="reports.nightly")
    def first_run() -> None:
        first_calls.append("ran")

    first_scheduler.tasks()["reports.nightly"].due = first_now
    first_scheduler.run_due_tasks(now=first_now)

    restart_now = datetime(2026, 7, 13, 11, 0, tzinfo=timezone.utc)
    restart_calls: list[str] = []
    restarted = Scheduler(state_store=store, now_fn=lambda: restart_now)

    @restarted.scheduled(hour=23, identity="reports.nightly")
    def second_run() -> None:
        restart_calls.append("repeated")

    restarted.run_due_tasks(now=datetime(2026, 7, 13, 23, 0, tzinfo=timezone.utc))

    assert first_calls == ["ran"]
    assert restart_calls == []
    assert restarted.tasks()["reports.nightly"].due == datetime(
        2026, 7, 14, 23, 0, tzinfo=timezone.utc
    )


def test_success_state_uses_current_cron_configuration_after_restart() -> None:
    first_now = datetime(2026, 7, 13, 10, 0, tzinfo=timezone.utc)
    store = InMemoryScheduleStateStore()
    first = Scheduler(state_store=store, now_fn=lambda: first_now)

    @first.scheduled(hour=23, identity="reports.rescheduled")
    def initial_schedule() -> None:
        return None

    first.tasks()["reports.rescheduled"].due = first_now
    first.run_due_tasks(now=first_now)

    restart_now = datetime(2026, 7, 14, 8, 0, tzinfo=timezone.utc)
    restarted = Scheduler(state_store=store, now_fn=lambda: restart_now)

    @restarted.scheduled(hour=12, identity="reports.rescheduled")
    def changed_schedule() -> None:
        return None

    assert restarted.tasks()["reports.rescheduled"].due == datetime(
        2026, 7, 14, 12, 0, tzinfo=timezone.utc
    )


def test_success_state_uses_current_interval_after_restart() -> None:
    first_now = datetime(2026, 7, 13, 10, 0, tzinfo=timezone.utc)
    store = InMemoryScheduleStateStore()
    first = Scheduler(state_store=store, now_fn=lambda: first_now)

    @first.scheduled(interval=3600, identity="reports.interval")
    def initial_schedule() -> None:
        return None

    first.tasks()["reports.interval"].due = first_now
    first.run_due_tasks(now=first_now)

    restart_now = first_now + timedelta(seconds=60)
    restarted = Scheduler(state_store=store, now_fn=lambda: restart_now)

    @restarted.scheduled(interval=120, identity="reports.interval")
    def changed_schedule() -> None:
        return None

    assert restarted.tasks()["reports.interval"].due == first_now + timedelta(
        seconds=120
    )


def test_failed_task_restores_backoff_and_failure_count_after_restart() -> None:
    first_now = datetime(2026, 7, 13, 12, 0, tzinfo=timezone.utc)
    store = InMemoryScheduleStateStore()
    first = Scheduler(state_store=store, now_fn=lambda: first_now)

    @first.scheduled(interval=60, identity="reports.flaky")
    def fail_first() -> None:
        raise RuntimeError("first")

    first.tasks()["reports.flaky"].due = first_now
    with pytest.raises(RuntimeError, match="first"):
        first.run_due_tasks(now=first_now)

    restart_now = first_now + timedelta(seconds=60)
    attempts: list[str] = []
    restarted = Scheduler(state_store=store, now_fn=lambda: restart_now)

    @restarted.scheduled(interval=60, identity="reports.flaky")
    def fail_again() -> None:
        attempts.append("ran")
        raise RuntimeError("second")

    restored = restarted.tasks()["reports.flaky"]
    assert restored.failures == 1
    assert restored.due == first_now + timedelta(seconds=120)

    restarted.run_due_tasks(now=restart_now)
    assert attempts == []

    with pytest.raises(RuntimeError, match="second"):
        restarted.run_due_tasks(now=restored.due)

    assert attempts == ["ran"]
    assert restored.failures == 2
    assert restored.due == first_now + timedelta(seconds=360)


def test_successful_retry_clears_persisted_failure_state() -> None:
    current = datetime(2026, 7, 13, 12, 0, tzinfo=timezone.utc)
    store = InMemoryScheduleStateStore()
    scheduler = Scheduler(state_store=store, now_fn=lambda: current)
    attempts = 0

    @scheduler.scheduled(interval=60, identity="reports.recovers")
    def task() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ConnectionError("temporary")

    scheduled_task = scheduler.tasks()["reports.recovers"]
    scheduled_task.due = current
    scheduler.run_due_tasks(
        now=current,
        retries=1,
        transient_exceptions=(ConnectionError,),
    )

    stored = json.loads(store.get(state_key("reports.recovers")))
    assert attempts == 2
    assert stored["failures"] == 0
    assert stored["last_failure"] is None
    assert stored["last_run"] == current.isoformat()


@pytest.mark.parametrize(
    "raw",
    [
        "not-json",
        '{"version": 2}',
        '{"version": 1, "last_run": "not-a-time", "due": null, "failures": 0, "last_failure": null}',
        '{"version": 1, "last_run": "2026-07-13T12:00:00", "due": null, "failures": 0, "last_failure": null}',
    ],
)
def test_malformed_durable_state_fails_registration_closed(raw: str) -> None:
    store = InMemoryScheduleStateStore()
    store.set(state_key("reports.corrupt"), raw)
    scheduler = Scheduler(state_store=store)

    with pytest.raises(ValueError, match="Invalid scheduler state for reports.corrupt"):

        @scheduler.scheduled(interval=60, identity="reports.corrupt")
        def task() -> None:
            return None


def test_state_store_read_failure_fails_registration_closed() -> None:
    class BrokenReadStore(InMemoryScheduleStateStore):
        def get(self, key: str) -> str | None:
            raise ConnectionError("redis unavailable")

    scheduler = Scheduler(state_store=BrokenReadStore())

    with pytest.raises(
        RuntimeError, match="Unable to read scheduler state for reports"
    ):

        @scheduler.scheduled(interval=60, identity="reports")
        def task() -> None:
            return None


def test_checkpoint_failure_does_not_repeat_callback_or_stop_sibling(caplog) -> None:
    class SelectiveWriteFailureStore(InMemoryScheduleStateStore):
        def set(self, key: str, value: str) -> None:
            if key == state_key("first"):
                raise ConnectionError("checkpoint unavailable")
            super().set(key, value)

    current = datetime(2026, 7, 13, 12, 0, tzinfo=timezone.utc)
    calls: list[str] = []
    scheduler = Scheduler(
        state_store=SelectiveWriteFailureStore(),
        now_fn=lambda: current,
    )

    @scheduler.scheduled(interval=60, identity="first")
    def first() -> None:
        calls.append("first")

    @scheduler.scheduled(interval=60, identity="second")
    def second() -> None:
        calls.append("second")

    scheduler.tasks()["first"].due = current
    scheduler.tasks()["second"].due = current

    with pytest.raises(ConnectionError, match="checkpoint unavailable"):
        scheduler.run_due_tasks(now=current)

    assert calls == ["first", "second"]
    assert scheduler.tasks()["first"].failures == 1
    assert "Unable to persist failure state for first" in caplog.text


def test_naive_scheduler_clock_fails_registration() -> None:
    scheduler = Scheduler(now_fn=lambda: datetime(2026, 7, 13, 12, 0))

    with pytest.raises(ValueError, match="Scheduler clock must include a UTC offset"):

        @scheduler.scheduled(interval=60, identity="naive")
        def task() -> None:
            return None

    assert scheduler.tasks() == {}


def test_naive_run_time_is_rejected_before_callback_or_checkpoint() -> None:
    aware_now = datetime(2026, 7, 13, 12, 0, tzinfo=timezone.utc)
    store = InMemoryScheduleStateStore()
    calls: list[str] = []
    scheduler = Scheduler(state_store=store, now_fn=lambda: aware_now)

    @scheduler.scheduled(interval=60, identity="naive-run")
    def task() -> None:
        calls.append("ran")

    scheduler.tasks()["naive-run"].due = aware_now

    with pytest.raises(ValueError, match="Scheduler clock must include a UTC offset"):
        scheduler.run_due_tasks(now=datetime(2026, 7, 13, 12, 0))

    assert calls == []
    assert store.get(state_key("naive-run")) is None
