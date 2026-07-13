from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
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


_SCHEDULE_STATE_VERSION = 1


@dataclass(frozen=True)
class _PersistedScheduleState:
    last_run: datetime | None
    due: datetime | None
    failures: int
    last_failure: datetime | None


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
    identity: str | None = None


class Scheduler:
    def __init__(
        self,
        *,
        state_store: ScheduleStateStore | None = None,
        logger: logging.Logger | None = None,
        now_fn: Callable[[], datetime] | None = None,
        sleep_fn: Callable[[float], None] | None = None,
    ) -> None:
        self.state_store = (
            state_store if state_store is not None else InMemoryScheduleStateStore()
        )
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
        identity: str | None = None,
        **options: Any,
    ) -> Callable[[Callable[[], object]], Callable[[], object]]:
        def decorator(func: Callable[[], object]) -> Callable[[], object]:
            task_identity = self._resolve_identity(func, identity)
            if task_identity in self._tasks:
                raise ValueError(f"Duplicate scheduled task identity: {task_identity}")

            persisted = self._read_state(task_identity)
            current = self._require_aware_datetime(
                self._now_fn(),
                label="Scheduler clock",
            )
            last_run = persisted.last_run if persisted is not None else None
            due = (
                persisted.due
                if (
                    persisted is not None
                    and persisted.failures > 0
                    and persisted.due is not None
                )
                else self._initial_due_time(
                    current=current,
                    interval=interval,
                    days=days,
                    hour=hour,
                    minute=minute,
                    last_run=last_run,
                )
            )
            task = ScheduledTask(
                name=func.__name__,
                func=func,
                identity=task_identity,
                interval=interval,
                days=tuple(days) if days is not None else None,
                hour=hour,
                minute=minute,
                options=dict(options),
                due=due,
                failures=persisted.failures if persisted is not None else 0,
                last_failure=(
                    persisted.last_failure if persisted is not None else None
                ),
                last_run=last_run,
            )
            self._tasks[task_identity] = task
            return func

        return decorator

    def tasks(self) -> dict[str, ScheduledTask]:
        return dict(self._tasks)

    def get_due_tasks(self, *, now: datetime | None = None) -> list[ScheduledTask]:
        current = self._require_aware_datetime(
            now or self._now_fn(),
            label="Scheduler clock",
        )
        return [task for task in self._tasks.values() if current >= task.due]

    def run_due_tasks(
        self,
        *,
        now: datetime | None = None,
        retries: int = 0,
        transient_exceptions: tuple[type[Exception], ...] = (),
    ) -> None:
        current = self._require_aware_datetime(
            now or self._now_fn(),
            label="Scheduler clock",
        )
        first_error: Exception | None = None
        for task in self.get_due_tasks(now=current):
            try:
                self._run_task_with_retry(
                    task,
                    retries=retries,
                    transient_exceptions=transient_exceptions,
                    now=current,
                )
            except Exception as exc:
                self.logger.exception(
                    "Scheduled task %s failed",
                    self._task_identity(task),
                )
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

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
            except transient_exceptions as exc:
                if attempt < retries:
                    attempt += 1
                    self.logger.warning(
                        "Transient error during task %s: %s. Retrying %s/%s",
                        self._task_identity(task),
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
            else:
                try:
                    self._handle_task_success(task, now=now)
                except Exception as exc:
                    self._handle_task_failure(task, exc, now=now)
                    raise
                return

    def _handle_task_success(
        self,
        task: ScheduledTask,
        *,
        now: datetime,
    ) -> None:
        next_due = self._next_due_time(task, now=now, last_run=now)
        self._write_state(
            self._task_identity(task),
            _PersistedScheduleState(
                last_run=now,
                due=next_due,
                failures=0,
                last_failure=None,
            ),
        )
        task.failures = 0
        task.last_failure = None
        task.last_run = now
        task.due = next_due

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
        identity = self._task_identity(task)
        try:
            self._write_state(
                identity,
                _PersistedScheduleState(
                    last_run=task.last_run,
                    due=task.due,
                    failures=task.failures,
                    last_failure=task.last_failure,
                ),
            )
        except Exception:
            self.logger.exception(
                "Unable to persist failure state for %s",
                identity,
            )
        self.logger.error(
            "Task %s failed (attempt %s). Retrying in %ss at %s. Error: %s",
            identity,
            task.failures,
            backoff_delay,
            task.due,
            exception,
        )

    def _initial_due_time(
        self,
        *,
        current: datetime,
        interval: int | None,
        days: Sequence[int] | None,
        hour: int | None,
        minute: int | None,
        last_run: datetime | None,
    ) -> datetime:
        if interval is not None:
            base = last_run if last_run is not None else current
            return base + timedelta(seconds=interval)
        return self._cron_due_time(
            current,
            last_run=last_run,
            days=days,
            hour=hour,
            minute=minute,
        )

    def _next_due_time(
        self,
        task: ScheduledTask,
        *,
        now: datetime,
        last_run: datetime,
    ) -> datetime:
        if task.interval is not None:
            return now + timedelta(seconds=task.interval)
        return self._cron_due_time(
            now,
            last_run=last_run,
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

    def _read_state(self, identity: str) -> _PersistedScheduleState | None:
        try:
            raw = self.state_store.get(self._state_key(identity))
        except Exception as exc:
            raise RuntimeError(
                f"Unable to read scheduler state for {identity}"
            ) from exc
        if raw is None:
            return None

        try:
            data = json.loads(raw)
            if (
                not isinstance(data, dict)
                or data.get("version") != _SCHEDULE_STATE_VERSION
            ):
                raise ValueError("unsupported state version")
            failures = data["failures"]
            if isinstance(failures, bool) or not isinstance(failures, int):
                raise ValueError("failures must be an integer")
            if failures < 0:
                raise ValueError("failures must not be negative")
            state = _PersistedScheduleState(
                last_run=self._parse_datetime(data["last_run"]),
                due=self._parse_datetime(data["due"]),
                failures=failures,
                last_failure=self._parse_datetime(data["last_failure"]),
            )
            if state.failures == 0 and state.last_failure is not None:
                raise ValueError("successful state cannot have last_failure")
            if state.failures > 0 and (state.last_failure is None or state.due is None):
                raise ValueError("failed state requires last_failure and due")
            return state
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError(f"Invalid scheduler state for {identity}") from exc

    def _write_state(
        self,
        identity: str,
        state: _PersistedScheduleState,
    ) -> None:
        value = json.dumps(
            {
                "due": self._format_datetime(state.due),
                "failures": state.failures,
                "last_failure": self._format_datetime(state.last_failure),
                "last_run": self._format_datetime(state.last_run),
                "version": _SCHEDULE_STATE_VERSION,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        self.state_store.set(self._state_key(identity), value)

    @staticmethod
    def _parse_datetime(value: object) -> datetime | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("timestamp must be a string or null")
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("timestamp must include a UTC offset")
        return parsed

    @staticmethod
    def _format_datetime(value: datetime | None) -> str | None:
        if value is None:
            return None
        Scheduler._require_aware_datetime(value, label="Scheduler state timestamp")
        return value.isoformat()

    @staticmethod
    def _require_aware_datetime(value: datetime, *, label: str) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{label} must include a UTC offset")
        return value

    @staticmethod
    def _resolve_identity(
        func: Callable[[], object],
        explicit_identity: str | None,
    ) -> str:
        if explicit_identity is not None:
            if not explicit_identity.strip():
                raise ValueError("Scheduled task identity must not be empty")
            return explicit_identity
        return f"{func.__module__}:{func.__qualname__}"

    @staticmethod
    def _task_identity(task: ScheduledTask) -> str:
        if task.identity is None:
            raise ValueError("ScheduledTask identity is required")
        return task.identity

    @staticmethod
    def _state_key(identity: str) -> str:
        return f"pybus.scheduler.state:{identity}"

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
    identity: str | None = None,
    **options: Any,
) -> Callable[[Callable[[], object]], Callable[[], object]]:
    return get_scheduler().scheduled(
        interval=interval,
        days=days,
        hour=hour,
        minute=minute,
        identity=identity,
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
