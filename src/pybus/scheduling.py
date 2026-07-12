from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import logging
import time
from typing import Any, Protocol


class ScheduleStateStore(Protocol):
    def get(self, key: str) -> str | None: ...

    def set(self, key: str, value: str) -> None: ...


class InMemoryScheduleStateStore:
    def __init__(self) -> None:
        self._values: dict[str, str] = {}

    def get(self, key: str) -> str | None:
        return self._values.get(key)

    def set(self, key: str, value: str) -> None:
        self._values[key] = value


@dataclass
class ScheduledTask:
    name: str
    func: Callable[[], object]
    due: datetime
    interval: int | None = None
    days: tuple[int, ...] | None = None
    hour: int | None = None
    minute: int | None = None
    options: dict[str, Any] = field(default_factory=dict)
    failures: int = 0
    last_failure: datetime | None = None
    last_run: datetime | None = None


class Scheduler:
    def __init__(
        self,
        *,
        state_store: ScheduleStateStore | None = None,
        logger: logging.Logger | None = None,
        now_fn: Callable[[], datetime] | None = None,
        sleep_fn: Callable[[float], None] | None = None,
    ) -> None:
        self.state_store = state_store or InMemoryScheduleStateStore()
        self.logger = logger or logging.getLogger("pybus.scheduler")
        self._now_fn = now_fn or self._default_now
        self._sleep_fn = sleep_fn or time.sleep
        self._tasks: dict[str, ScheduledTask] = {}
        self._disabled = False

    def scheduled(
        self,
        *,
        interval: int | None = None,
        days: Sequence[int] | None = None,
        hour: int | None = None,
        minute: int | None = None,
        **options: Any,
    ) -> Callable[[Callable[[], object]], Callable[[], object]]:
        def decorator(func: Callable[[], object]) -> Callable[[], object]:
            task = ScheduledTask(
                name=func.__name__,
                func=func,
                interval=interval,
                days=tuple(days) if days is not None else None,
                hour=hour,
                minute=minute,
                options=dict(options),
                due=self._initial_due_time(
                    func,
                    interval=interval,
                    days=days,
                    hour=hour,
                    minute=minute,
                ),
            )
            task.last_run = self._read_last_run(task.name)
            self._tasks[func.__name__] = task
            return func

        return decorator

    def tasks(self) -> dict[str, ScheduledTask]:
        return dict(self._tasks)

    def get_due_tasks(self, *, now: datetime | None = None) -> list[ScheduledTask]:
        current = now or self._now_fn()
        return [task for task in self._tasks.values() if current >= task.due]

    def run_due_tasks(
        self,
        *,
        now: datetime | None = None,
        retries: int = 0,
        transient_exceptions: tuple[type[Exception], ...] = (),
    ) -> None:
        current = now or self._now_fn()
        for task in self.get_due_tasks(now=current):
            self._run_task_with_retry(
                task,
                retries=retries,
                transient_exceptions=transient_exceptions,
                now=current,
            )

    def start(
        self,
        *,
        poll_interval: int = 30,
        retries: int = 0,
        transient_exceptions: tuple[type[Exception], ...] = (),
    ) -> None:
        self.logger.info("Scheduler started")
        self._disabled = False
        while self._disabled is False:
            try:
                self.run_due_tasks(
                    retries=retries,
                    transient_exceptions=transient_exceptions,
                )
            except Exception as exc:  # pragma: no cover - defensive loop guard
                self.logger.exception(exc)
            self._sleep_fn(poll_interval)
        self.logger.info("Scheduler stopping")

    def stop(self) -> None:
        self._disabled = True

    def _run_task_with_retry(
        self,
        task: ScheduledTask,
        *,
        retries: int,
        transient_exceptions: tuple[type[Exception], ...],
        now: datetime,
    ) -> None:
        attempt = 0
        while True:
            try:
                task.func()
                task.failures = 0
                task.last_failure = None
                task.last_run = now
                self.state_store.set(self._state_key(task.name), now.isoformat())
                task.due = self._next_due_time(
                    task,
                    now=now,
                )
                return
            except transient_exceptions as exc:
                if attempt < retries:
                    attempt += 1
                    self.logger.warning(
                        "Transient error during task %s: %s. Retrying %s/%s",
                        task.name,
                        exc,
                        attempt,
                        retries,
                    )
                    continue
                self._handle_task_failure(task, exc, now=now)
                raise
            except Exception as exc:
                self._handle_task_failure(task, exc, now=now)
                raise

    def _handle_task_failure(
        self,
        task: ScheduledTask,
        exception: Exception,
        *,
        now: datetime,
    ) -> None:
        task.failures += 1
        task.last_failure = now
        backoff_delay = 60 * (2 ** min(task.failures, 10))
        task.due = now + timedelta(seconds=backoff_delay)
        self.logger.error(
            "Task %s failed (attempt %s). Retrying in %ss at %s. Error: %s",
            task.name,
            task.failures,
            backoff_delay,
            task.due,
            exception,
        )

    def _initial_due_time(
        self,
        func: Callable[[], object],
        *,
        interval: int | None,
        days: Sequence[int] | None,
        hour: int | None,
        minute: int | None,
    ) -> datetime:
        current = self._now_fn()
        if interval is not None:
            return current + timedelta(seconds=interval)
        last_run = self._read_last_run(func.__name__)
        return self._cron_due_time(
            current,
            last_run=last_run,
            days=days,
            hour=hour,
            minute=minute,
        )

    def _next_due_time(self, task: ScheduledTask, *, now: datetime) -> datetime:
        if task.interval is not None:
            return now + timedelta(seconds=task.interval)
        return self._cron_due_time(
            now,
            last_run=task.last_run,
            days=task.days,
            hour=task.hour,
            minute=task.minute,
        )

    def _cron_due_time(
        self,
        current: datetime,
        *,
        last_run: datetime | None,
        days: Sequence[int] | None,
        hour: int | None,
        minute: int | None,
    ) -> datetime:
        target_hour = hour or 0
        target_minute = minute or 0
        scheduled_today = current.replace(
            hour=target_hour,
            minute=target_minute,
            second=0,
            microsecond=0,
        )
        has_run_today = last_run is not None and last_run.date() >= current.date()
        if has_run_today or current >= scheduled_today:
            start_from = scheduled_today + timedelta(days=1)
        else:
            start_from = scheduled_today

        if not days:
            return start_from

        candidate = start_from
        for _ in range(8):
            if candidate.weekday() in days:
                return candidate
            candidate += timedelta(days=1)
        return start_from

    def _read_last_run(self, task_name: str) -> datetime | None:
        raw = self.state_store.get(self._state_key(task_name))
        if raw is None:
            return None
        return datetime.fromisoformat(raw)

    @staticmethod
    def _state_key(task_name: str) -> str:
        return f"pybus.scheduler.last_run:{task_name}"

    @staticmethod
    def _default_now() -> datetime:
        return datetime.now(timezone.utc)


_default_scheduler = Scheduler()


def configure_scheduler(
    *,
    state_store: ScheduleStateStore | None = None,
    logger: logging.Logger | None = None,
    now_fn: Callable[[], datetime] | None = None,
    sleep_fn: Callable[[float], None] | None = None,
) -> Scheduler:
    global _default_scheduler
    _default_scheduler = Scheduler(
        state_store=state_store,
        logger=logger,
        now_fn=now_fn,
        sleep_fn=sleep_fn,
    )
    return _default_scheduler


def get_scheduler() -> Scheduler:
    return _default_scheduler


def scheduled(
    *,
    interval: int | None = None,
    days: Sequence[int] | None = None,
    hour: int | None = None,
    minute: int | None = None,
    **options: Any,
) -> Callable[[Callable[[], object]], Callable[[], object]]:
    return get_scheduler().scheduled(
        interval=interval,
        days=days,
        hour=hour,
        minute=minute,
        **options,
    )


__all__ = [
    "InMemoryScheduleStateStore",
    "ScheduleStateStore",
    "ScheduledTask",
    "Scheduler",
    "configure_scheduler",
    "get_scheduler",
    "scheduled",
]
