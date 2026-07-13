from __future__ import annotations

from collections.abc import Sequence
import logging
import math
import sys
from threading import Event, Lock
from typing import Protocol

from pybus.exceptions import IndeterminateDeliveryError


class ListenerLike(Protocol):
    dead_letter_channel: str

    def listen_once(self, channel: str | Sequence[str]) -> object | None: ...


class WorkerHook:
    """No-op lifecycle callbacks for a blocking worker."""

    def on_start(self, worker: Worker) -> None:
        return None

    def before_poll(self, worker: Worker) -> None:
        return None

    def after_poll(self, worker: Worker, result: object | None) -> None:
        return None

    def on_error(self, worker: Worker, exception: Exception) -> None:
        return None

    def on_stop(self, worker: Worker) -> None:
        return None


class Worker:
    """Run a Listener with cooperative stopping and lifecycle callbacks."""

    def __init__(
        self,
        listener: ListenerLike,
        channel: str | Sequence[str],
        *,
        hooks: Sequence[WorkerHook] = (),
        error_delay: float = 1.0,
        logger: logging.Logger | None = None,
        stop_event: Event | None = None,
    ) -> None:
        if (
            isinstance(error_delay, bool)
            or not isinstance(error_delay, (int, float))
            or not math.isfinite(error_delay)
            or error_delay < 0
        ):
            raise ValueError("error_delay must be a non-negative finite number")

        normalized_channel: str | tuple[str, ...]
        if isinstance(channel, str):
            normalized_channel = channel
            channels = (channel,)
        else:
            normalized_channel = tuple(channel)
            channels = normalized_channel
        if not channels:
            raise ValueError("worker requires at least one channel")
        if listener.dead_letter_channel in channels:
            raise ValueError("dead-letter channel is terminal and cannot be worked")

        self.listener = listener
        self.channel = normalized_channel
        self.hooks = tuple(hooks)
        self.error_delay = float(error_delay)
        self.logger = logger or logging.getLogger("pybus.worker")
        self._stop_event = Event() if stop_event is None else stop_event
        self._state_lock = Lock()
        self._running = False

    @property
    def is_running(self) -> bool:
        with self._state_lock:
            return self._running

    def stop(self) -> None:
        with self._state_lock:
            self._stop_event.set()

    def run(self, *, max_iterations: int | None = None) -> None:
        self._validate_max_iterations(max_iterations)
        with self._state_lock:
            if self._running:
                raise RuntimeError("worker is already running")
            self._running = True

        started_hooks: list[WorkerHook] = []
        try:
            for hook in self.hooks:
                hook.on_start(self)
                started_hooks.append(hook)

            iterations = 0
            while not self._stop_event.is_set() and (
                max_iterations is None or iterations < max_iterations
            ):
                iterations += 1
                cycle_error = self._run_cycle()
                if cycle_error is None:
                    continue
                self._report_error(cycle_error)
                if isinstance(cycle_error, IndeterminateDeliveryError):
                    raise cycle_error
                self._stop_event.wait(self.error_delay)
        finally:
            active_exception = sys.exc_info()[0] is not None
            stop_errors: list[Exception] = []
            for hook in reversed(started_hooks):
                try:
                    hook.on_stop(self)
                except Exception as exc:
                    stop_errors.append(exc)
                    self._log_exception("Worker stop hook failed", exc)
            with self._state_lock:
                self._running = False
            if stop_errors and not active_exception:
                raise stop_errors[0]

    def _run_cycle(self) -> Exception | None:
        poll_started = False
        result: object | None = None
        cycle_error: Exception | None = None
        try:
            for hook in self.hooks:
                hook.before_poll(self)
            with self._state_lock:
                if self._stop_event.is_set():
                    return None
                poll_started = True
            result = self.listener.listen_once(self.channel)
        except Exception as exc:
            cycle_error = exc
        finally:
            if poll_started:
                for hook in self.hooks:
                    try:
                        hook.after_poll(self, result)
                    except Exception as exc:
                        if cycle_error is None:
                            cycle_error = exc
                        else:
                            self._log_exception("Worker after-poll hook failed", exc)
        return cycle_error

    def _report_error(self, exception: Exception) -> None:
        self._log_exception("Worker cycle failed", exception)
        for hook in self.hooks:
            try:
                hook.on_error(self, exception)
            except Exception as exc:
                self._log_exception("Worker error hook failed", exc)

    def _log_exception(self, message: str, exception: Exception) -> None:
        self.logger.error(
            "%s: %s",
            message,
            exception,
            exc_info=(type(exception), exception, exception.__traceback__),
        )

    @staticmethod
    def _validate_max_iterations(max_iterations: int | None) -> None:
        if max_iterations is None:
            return
        if (
            isinstance(max_iterations, bool)
            or not isinstance(max_iterations, int)
            or max_iterations < 0
        ):
            raise ValueError("max_iterations must be a non-negative integer")


__all__ = ["Worker", "WorkerHook"]
