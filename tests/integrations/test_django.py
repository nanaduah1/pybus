from __future__ import annotations

from dataclasses import dataclass

import pytest

from pybus.integrations.django import (
    DjangoBusAdapter,
    DjangoConnectionCleanupHook,
)
from pybus.worker import Worker


@dataclass
class FakeConnection:
    in_atomic_block: bool


class FakeTransactionModule:
    def __init__(self, *, in_atomic_block: bool) -> None:
        self._connection = FakeConnection(in_atomic_block=in_atomic_block)
        self.callbacks: list[callable] = []

    def get_connection(self) -> FakeConnection:
        return self._connection

    def on_commit(self, callback) -> None:
        self.callbacks.append(callback)


def test_django_bus_adapter_defers_within_atomic_block() -> None:
    published: list[tuple[str, dict, str]] = []
    transaction = FakeTransactionModule(in_atomic_block=True)
    adapter = DjangoBusAdapter(
        lambda event_type, data, queue=None: published.append(
            (event_type, data, queue)
        ),
        transaction_module=transaction,
    )

    adapter.schedule("student.enrolled", {"student_id": "S-1"})

    assert published == []
    assert len(transaction.callbacks) == 1

    transaction.callbacks[0]()

    assert published == [("student.enrolled", {"student_id": "S-1"}, "pybus.jobs")]


def test_django_bus_adapter_publishes_immediately_outside_transaction() -> None:
    published: list[tuple[str, dict, str]] = []
    transaction = FakeTransactionModule(in_atomic_block=False)
    adapter = DjangoBusAdapter(
        lambda event_type, data, queue=None: published.append(
            (event_type, data, queue)
        ),
        transaction_module=transaction,
        default_queue="custom.queue",
    )

    adapter.schedule("student.enrolled", {"student_id": "S-2"})

    assert transaction.callbacks == []
    assert published == [("student.enrolled", {"student_id": "S-2"}, "custom.queue")]


def test_django_cleanup_hook_wraps_poll_and_shutdown() -> None:
    calls: list[str] = []

    class Listener:
        dead_letter_channel = "pybus.jobs.failed"

        def listen_once(self, channel):
            calls.append("listen")
            return None

    hook = DjangoConnectionCleanupHook(
        close_connections_fn=lambda: calls.append("cleanup")
    )

    Worker(Listener(), "pybus.jobs", hooks=(hook,)).run(max_iterations=1)

    assert calls == ["cleanup", "listen", "cleanup", "cleanup"]


def test_django_cleanup_hook_loads_django_lazily(monkeypatch) -> None:
    def missing_django(name):
        raise ModuleNotFoundError(name)

    monkeypatch.setattr("pybus.integrations.django.import_module", missing_django)

    with pytest.raises(RuntimeError, match="Django is required"):
        DjangoConnectionCleanupHook()


def test_django_cleanup_runs_after_listener_error_before_recovery() -> None:
    calls: list[str] = []

    class Listener:
        dead_letter_channel = "pybus.jobs.failed"

        def __init__(self):
            self.attempts = 0

        def listen_once(self, channel):
            self.attempts += 1
            calls.append(f"listen:{self.attempts}")
            if self.attempts == 1:
                raise ConnectionError("redis down")
            return None

    hook = DjangoConnectionCleanupHook(
        close_connections_fn=lambda: calls.append("cleanup")
    )

    Worker(Listener(), "pybus.jobs", hooks=(hook,), error_delay=0).run(max_iterations=2)

    assert calls == [
        "cleanup",
        "listen:1",
        "cleanup",
        "cleanup",
        "listen:2",
        "cleanup",
        "cleanup",
    ]
