from __future__ import annotations

from dataclasses import dataclass
from inspect import signature
from unittest.mock import patch

import pytest

from pybus import (
    Pybus,
    command,
    event,
    publish_event as core_publish_event,
    send_command as core_send_command,
)
from pybus.envelope import MessageEnvelope
from pybus.exceptions import InvalidMessageDefinitionError
from pybus.integrations.django import (
    DjangoBusAdapter,
    DjangoConnectionCleanupHook,
    publish_event,
    send_command,
)
from pybus.queues import QueueTopology
from pybus.transports.memory import MemoryTransport
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


@event("student.enrolled")
class StudentEnrolled:
    student_id: str


@event("student.tags_changed")
class StudentTagsChanged:
    tags: list[str]


@command("billing.generate")
class GenerateBill:
    student_id: str


@event("student.archived", queue="student.lifecycle")
class StudentArchived:
    student_id: str


MESSAGE_TOPOLOGY = QueueTopology().declare_queues(
    "student.lifecycle", "student.priority", "billing.commands"
)


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


def test_django_module_override_uses_the_same_publish_event_input(monkeypatch) -> None:
    transport = MemoryTransport()
    bus = Pybus(transport, topology=MESSAGE_TOPOLOGY)
    transaction = FakeTransactionModule(in_atomic_block=False)
    monkeypatch.setattr("pybus.integrations.django.get_bus", lambda: bus)
    monkeypatch.setattr(
        "pybus.integrations.django._load_transaction_module", lambda: transaction
    )

    envelope = publish_event(StudentEnrolled(student_id="S-4"))

    assert envelope.message_type == "student.enrolled"
    assert transport.size(bus.topology.default_queue) == 1


@pytest.mark.parametrize(
    ("in_atomic_block", "queue", "expected_queue"),
    [
        (False, None, "student.lifecycle"),
        (False, "student.priority", "student.priority"),
        (True, None, "student.lifecycle"),
        (True, "student.priority", "student.priority"),
    ],
)
def test_django_override_honors_declared_queue_and_call_site_override(
    monkeypatch, in_atomic_block, queue, expected_queue
) -> None:
    transport = MemoryTransport()
    bus = Pybus(transport, topology=MESSAGE_TOPOLOGY)
    transaction = FakeTransactionModule(in_atomic_block=in_atomic_block)
    monkeypatch.setattr("pybus.integrations.django.get_bus", lambda: bus)
    monkeypatch.setattr(
        "pybus.integrations.django._load_transaction_module", lambda: transaction
    )

    publish_event(StudentArchived(student_id="S-4"), queue=queue)
    if in_atomic_block:
        assert transport.size(expected_queue) == 0
        transaction.callbacks[0]()

    assert transport.size(expected_queue) == 1
    other_queue = (
        "student.priority"
        if expected_queue == "student.lifecycle"
        else "student.lifecycle"
    )
    assert transport.size(other_queue) == 0


@pytest.mark.parametrize("invalid_queue", ["", "   ", 7, True])
def test_django_override_rejects_invalid_queue_before_registering_callback(
    monkeypatch, invalid_queue
) -> None:
    transport = MemoryTransport()
    bus = Pybus(transport, topology=MESSAGE_TOPOLOGY)
    transaction = FakeTransactionModule(in_atomic_block=True)
    monkeypatch.setattr("pybus.integrations.django.get_bus", lambda: bus)
    monkeypatch.setattr(
        "pybus.integrations.django._load_transaction_module", lambda: transaction
    )

    with pytest.raises(InvalidMessageDefinitionError, match="queue"):
        publish_event(StudentArchived(student_id="S-4"), queue=invalid_queue)

    assert transaction.callbacks == []


def test_django_override_rejects_invalid_bus_default_before_callback(
    monkeypatch,
) -> None:
    transport = MemoryTransport()
    bus = Pybus(transport, topology=QueueTopology(default_queue="   "))
    transaction = FakeTransactionModule(in_atomic_block=True)
    monkeypatch.setattr("pybus.integrations.django.get_bus", lambda: bus)
    monkeypatch.setattr(
        "pybus.integrations.django._load_transaction_module", lambda: transaction
    )

    with pytest.raises(InvalidMessageDefinitionError, match="queue"):
        publish_event(StudentEnrolled(student_id="S-4"))

    assert transaction.callbacks == []
    assert transport.size("   ") == 0


def test_django_deferred_publish_snapshots_the_resolved_queue(monkeypatch) -> None:
    transport = MemoryTransport()
    bus = Pybus(transport, topology=MESSAGE_TOPOLOGY)
    transaction = FakeTransactionModule(in_atomic_block=True)
    monkeypatch.setattr("pybus.integrations.django.get_bus", lambda: bus)
    monkeypatch.setattr(
        "pybus.integrations.django._load_transaction_module", lambda: transaction
    )

    publish_event(StudentArchived(student_id="S-4"))
    monkeypatch.setattr(StudentArchived, "__pybus_default_queue__", "student.priority")
    transaction.callbacks[0]()

    assert transport.size("student.lifecycle") == 1
    assert transport.size("student.priority") == 0


def test_django_override_parameters_match_core_publication_functions() -> None:
    assert (
        signature(publish_event).parameters == signature(core_publish_event).parameters
    )
    assert signature(send_command).parameters == signature(core_send_command).parameters


def test_django_module_exports_matching_command_name() -> None:
    transport = MemoryTransport()
    bus = Pybus(transport)
    transaction = FakeTransactionModule(in_atomic_block=False)

    with (
        patch("pybus.integrations.django.get_bus", return_value=bus),
        patch(
            "pybus.integrations.django._load_transaction_module",
            return_value=transaction,
        ),
    ):
        envelope = send_command(GenerateBill(student_id="S-5"))

    assert envelope.message_type == "billing.generate"
    assert envelope.payload == {"student_id": "S-5"}


def test_django_deferred_publish_snapshots_payload_at_call_time() -> None:
    transport = MemoryTransport()
    bus = Pybus(transport)
    transaction = FakeTransactionModule(in_atomic_block=True)
    tags = ["new"]

    with (
        patch("pybus.integrations.django.get_bus", return_value=bus),
        patch(
            "pybus.integrations.django._load_transaction_module",
            return_value=transaction,
        ),
    ):
        publish_event(StudentTagsChanged(tags=tags))
    tags.append("mutated-after-schedule")
    transaction.callbacks[0]()

    raw = transport.consume(bus.topology.default_queue)
    envelope = MessageEnvelope.from_dict(bus.serializer.loads(raw))
    assert envelope.payload == {"tags": ["new"]}


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
