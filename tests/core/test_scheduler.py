from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import pybus
from pybus.scheduling import InMemoryScheduleStateStore, Scheduler, get_scheduler


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

    task = scheduler.tasks()["refresh_cache"]

    assert refresh_cache is not None
    assert task.interval == 300
    assert task.options["priority"] == "low"
    assert task.due == current + timedelta(seconds=300)


def test_scheduled_decorator_uses_days_hour_and_minute_for_cron_tasks() -> None:
    current = datetime(2026, 7, 12, 21, 0, tzinfo=timezone.utc)  # Sunday
    scheduler = Scheduler(now_fn=lambda: current)

    @scheduler.scheduled(days=[0, 2, 4], hour=20, minute=15)
    def run_report() -> None:
        return None

    task = scheduler.tasks()["run_report"]

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

    task = scheduler.tasks()["send_digest"]
    task.due = current

    scheduler.run_due_tasks(now=current)

    assert calls == ["ran"]
    assert task.last_run == current
    assert task.due == current + timedelta(seconds=120)
    assert state_store.get("pybus.scheduler.last_run:send_digest") == current.isoformat()


def test_run_due_tasks_applies_backoff_after_failure() -> None:
    current = datetime(2026, 7, 12, 12, 0, tzinfo=timezone.utc)
    scheduler = Scheduler(now_fn=lambda: current)

    @scheduler.scheduled(interval=60)
    def flaky_job() -> None:
        raise RuntimeError("boom")

    task = scheduler.tasks()["flaky_job"]
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

    task = scheduler.tasks()["flaky_once"]
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

    assert "global_task" in scheduler.tasks()
    assert scheduler.tasks()["global_task"].due == current + timedelta(seconds=90)
