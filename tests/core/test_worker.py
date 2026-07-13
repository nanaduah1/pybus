from __future__ import annotations

from collections.abc import Sequence
from threading import Event, Thread

import pytest

from pybus.exceptions import IndeterminateDeliveryError
from pybus.worker import Worker, WorkerHook


class ScriptedListener:
    dead_letter_channel = "pybus.jobs.failed"

    def __init__(self, actions: Sequence[object], events: list[str] | None = None):
        self.actions = list(actions)
        self.events = events
        self.calls: list[object] = []

    def listen_once(self, channel):
        self.calls.append(channel)
        if self.events is not None:
            self.events.append("listen")
        action = self.actions.pop(0) if self.actions else None
        if isinstance(action, BaseException):
            raise action
        return action


class RecordingHook(WorkerHook):
    def __init__(self, name: str, events: list[str], *, stop_after_poll=False):
        self.name = name
        self.events = events
        self.stop_after_poll = stop_after_poll

    def on_start(self, worker):
        self.events.append(f"{self.name}:start")

    def before_poll(self, worker):
        self.events.append(f"{self.name}:before")

    def after_poll(self, worker, result):
        self.events.append(f"{self.name}:after:{result}")
        if self.stop_after_poll:
            worker.stop()

    def on_error(self, worker, exception):
        self.events.append(f"{self.name}:error:{type(exception).__name__}")

    def on_stop(self, worker):
        self.events.append(f"{self.name}:stop")


class RecordingStopEvent:
    def __init__(self):
        self.stopped = False
        self.waits: list[float] = []

    def is_set(self):
        return self.stopped

    def set(self):
        self.stopped = True

    def wait(self, timeout):
        self.waits.append(timeout)
        return self.stopped


def test_worker_runs_hooks_in_deterministic_order() -> None:
    events: list[str] = []
    listener = ScriptedListener(["handled", None], events)
    first = RecordingHook("first", events)
    second = RecordingHook("second", events)
    worker = Worker(listener, "pybus.jobs", hooks=(first, second))

    worker.run(max_iterations=2)

    assert events == [
        "first:start",
        "second:start",
        "first:before",
        "second:before",
        "listen",
        "first:after:handled",
        "second:after:handled",
        "first:before",
        "second:before",
        "listen",
        "first:after:None",
        "second:after:None",
        "second:stop",
        "first:stop",
    ]
    assert worker.is_running is False


def test_stop_after_poll_prevents_another_delivery_and_is_idempotent() -> None:
    events: list[str] = []
    listener = ScriptedListener(["one", "two"])
    worker = Worker(
        listener,
        "pybus.jobs",
        hooks=(RecordingHook("hook", events, stop_after_poll=True),),
    )

    worker.run()
    worker.stop()

    assert listener.calls == ["pybus.jobs"]
    assert events[-1] == "hook:stop"


def test_stop_before_run_executes_lifecycle_without_polling() -> None:
    events: list[str] = []
    listener = ScriptedListener(["never"])
    worker = Worker(
        listener,
        "pybus.jobs",
        hooks=(RecordingHook("hook", events),),
    )

    worker.stop()
    worker.run()

    assert listener.calls == []
    assert events == ["hook:start", "hook:stop"]


def test_stop_during_in_flight_delivery_allows_it_to_finish_once() -> None:
    events: list[str] = []
    worker = None

    class StoppingListener(ScriptedListener):
        def listen_once(self, channel):
            events.append("delivery:start")
            worker.stop()
            events.append("delivery:finish")
            return "handled"

    worker = Worker(StoppingListener([]), "pybus.jobs")

    worker.run()

    assert events == ["delivery:start", "delivery:finish"]


def test_stop_from_before_poll_prevents_listener_admission() -> None:
    class StoppingHook(WorkerHook):
        def before_poll(self, worker):
            worker.stop()

    listener = ScriptedListener(["never"])
    worker = Worker(listener, "pybus.jobs", hooks=(StoppingHook(),))

    worker.run()

    assert listener.calls == []


def test_stop_wins_while_before_poll_is_blocked() -> None:
    entered = Event()
    release = Event()

    class BlockingHook(WorkerHook):
        def before_poll(self, worker):
            entered.set()
            release.wait(timeout=2)

    listener = ScriptedListener(["never"])
    worker = Worker(listener, "pybus.jobs", hooks=(BlockingHook(),))
    thread = Thread(target=worker.run)
    thread.start()
    assert entered.wait(timeout=1)

    worker.stop()
    release.set()
    thread.join(timeout=2)

    assert thread.is_alive() is False
    assert listener.calls == []


def test_worker_recovers_from_poll_error_with_fixed_interruptible_delay() -> None:
    events: list[str] = []
    stop_event = RecordingStopEvent()
    listener = ScriptedListener([ConnectionError("redis down"), "handled"])
    worker = Worker(
        listener,
        ("pybus.jobs", "pybus.jobs.slow"),
        hooks=(RecordingHook("hook", events),),
        error_delay=0.25,
        stop_event=stop_event,
    )

    worker.run(max_iterations=2)

    assert len(listener.calls) == 2
    assert stop_event.waits == [0.25]
    assert "hook:error:ConnectionError" in events
    assert "hook:after:handled" in events


def test_indeterminate_delivery_is_reported_and_aborts_worker() -> None:
    events: list[str] = []
    listener = ScriptedListener(
        [IndeterminateDeliveryError("settlement failed"), "must-not-run"]
    )
    worker = Worker(
        listener,
        "pybus.jobs",
        hooks=(RecordingHook("hook", events),),
        error_delay=0,
    )

    with pytest.raises(IndeterminateDeliveryError, match="settlement failed"):
        worker.run()

    assert listener.calls == ["pybus.jobs"]
    assert "hook:error:IndeterminateDeliveryError" in events
    assert events[-1] == "hook:stop"


def test_failed_error_hook_is_logged_without_killing_recovery(caplog) -> None:
    class BrokenErrorHook(WorkerHook):
        def on_error(self, worker, exception):
            raise RuntimeError("observer broke")

    events: list[str] = []
    listener = ScriptedListener([ConnectionError("redis down"), "handled"])
    worker = Worker(
        listener,
        "pybus.jobs",
        hooks=(BrokenErrorHook(), RecordingHook("healthy", events)),
        error_delay=0,
    )

    worker.run(max_iterations=2)

    assert len(listener.calls) == 2
    assert "healthy:error:ConnectionError" in events
    assert "observer broke" in caplog.text


def test_before_poll_failure_skips_consumption_then_recovers() -> None:
    events: list[str] = []

    class FailOnceBeforeHook(RecordingHook):
        def __init__(self):
            super().__init__("hook", events)
            self.failed = False

        def before_poll(self, worker):
            super().before_poll(worker)
            if not self.failed:
                self.failed = True
                raise RuntimeError("cleanup failed")

    listener = ScriptedListener(["handled"])
    worker = Worker(
        listener,
        "pybus.jobs",
        hooks=(FailOnceBeforeHook(),),
        error_delay=0,
    )

    worker.run(max_iterations=2)

    assert listener.calls == ["pybus.jobs"]
    assert "hook:error:RuntimeError" in events
    assert "hook:after:handled" in events


def test_after_poll_failure_does_not_repeat_completed_delivery() -> None:
    events: list[str] = []

    class FailOnceAfterHook(RecordingHook):
        def __init__(self):
            super().__init__("hook", events)
            self.failed = False

        def after_poll(self, worker, result):
            super().after_poll(worker, result)
            if not self.failed:
                self.failed = True
                raise RuntimeError("after failed")

    listener = ScriptedListener(["first", "second"])
    worker = Worker(
        listener,
        "pybus.jobs",
        hooks=(FailOnceAfterHook(),),
        error_delay=0,
    )

    worker.run(max_iterations=2)

    assert listener.calls == ["pybus.jobs", "pybus.jobs"]
    assert events.count("hook:after:first") == 1
    assert "hook:error:RuntimeError" in events


def test_start_failure_aborts_before_poll_and_runs_started_cleanup() -> None:
    events: list[str] = []

    class FailingStartHook(RecordingHook):
        def on_start(self, worker):
            super().on_start(worker)
            raise RuntimeError("start failed")

    listener = ScriptedListener(["never"])
    first = RecordingHook("first", events)
    failing = FailingStartHook("failing", events)
    worker = Worker(listener, "pybus.jobs", hooks=(first, failing))

    with pytest.raises(RuntimeError, match="start failed"):
        worker.run()

    assert listener.calls == []
    assert events == ["first:start", "failing:start", "first:stop"]


def test_all_stop_hooks_run_and_first_failure_is_raised(caplog) -> None:
    events: list[str] = []

    class FailingStopHook(RecordingHook):
        def on_stop(self, worker):
            super().on_stop(worker)
            raise RuntimeError(f"{self.name} stop failed")

    worker = Worker(
        ScriptedListener([None]),
        "pybus.jobs",
        hooks=(
            FailingStopHook("first", events),
            FailingStopHook("second", events),
        ),
    )

    with pytest.raises(RuntimeError, match="second stop failed"):
        worker.run(max_iterations=1)

    assert events[-2:] == ["second:stop", "first:stop"]
    assert "first stop failed" in caplog.text


def test_worker_does_not_swallow_base_exceptions() -> None:
    events: list[str] = []
    worker = Worker(
        ScriptedListener([KeyboardInterrupt()]),
        "pybus.jobs",
        hooks=(RecordingHook("hook", events),),
    )

    with pytest.raises(KeyboardInterrupt):
        worker.run()

    assert events[-1] == "hook:stop"


def test_worker_rejects_terminal_dead_letter_channel() -> None:
    listener = ScriptedListener([])

    with pytest.raises(ValueError, match="dead-letter channel is terminal"):
        Worker(listener, listener.dead_letter_channel)

    with pytest.raises(ValueError, match="dead-letter channel is terminal"):
        Worker(listener, ("pybus.jobs", listener.dead_letter_channel))


def test_worker_rejects_concurrent_run() -> None:
    entered = Event()
    release = Event()

    class BlockingListener(ScriptedListener):
        def listen_once(self, channel):
            entered.set()
            release.wait(timeout=2)
            return None

    worker = Worker(BlockingListener([]), "pybus.jobs")
    thread = Thread(target=worker.run, kwargs={"max_iterations": 1})
    thread.start()
    assert entered.wait(timeout=1)

    with pytest.raises(RuntimeError, match="already running"):
        worker.run(max_iterations=1)

    release.set()
    thread.join(timeout=2)
    assert thread.is_alive() is False


@pytest.mark.parametrize("error_delay", [-1, True, float("nan")])
def test_worker_rejects_invalid_error_delay(error_delay) -> None:
    with pytest.raises(ValueError):
        Worker(ScriptedListener([]), "pybus.jobs", error_delay=error_delay)


@pytest.mark.parametrize("max_iterations", [-1, True, 1.5])
def test_worker_rejects_invalid_max_iterations(max_iterations) -> None:
    worker = Worker(ScriptedListener([]), "pybus.jobs")

    with pytest.raises(ValueError):
        worker.run(max_iterations=max_iterations)
