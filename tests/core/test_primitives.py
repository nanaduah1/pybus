from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

import pybus
from pybus.dispatcher import Dispatcher
from pybus.envelope import MessageEnvelope
from pybus.messages import EventMessage
from pybus.registry import Registry
from pybus.serializer import JsonSerializer
from pybus.transports.memory import MemoryTransport


def test_import_surface_exposes_core_primitives() -> None:
    assert pybus.MessageEnvelope is MessageEnvelope
    assert pybus.EventMessage is EventMessage
    assert pybus.Registry is Registry
    assert pybus.Dispatcher is Dispatcher
    assert pybus.JsonSerializer is JsonSerializer


def test_imports_do_not_pull_optional_integrations() -> None:
    import sys

    import pybus as _pybus  # noqa: F401
    import pybus.integrations.django as _django  # noqa: F401
    import pybus.integrations.redis as _redis  # noqa: F401

    assert "redis" not in sys.modules
    assert "django" not in sys.modules


def test_message_envelope_round_trip_with_datetime_and_uuid() -> None:
    serializer = JsonSerializer()
    created_at = datetime(2026, 7, 7, 15, 30, tzinfo=timezone.utc)
    expires_at = datetime(2026, 7, 8, 15, 30, tzinfo=timezone.utc)
    payload = {
        "event_id": uuid4(),
        "scheduled_for": created_at,
        "nested": ["ready", {"expires_at": expires_at}],
    }

    envelope = MessageEnvelope.create(
        message_type="student.enrolled",
        message_kind="event",
        version=1,
        payload=payload,
        headers={"trace_id": UUID("12345678-1234-5678-1234-567812345678")},
        correlation_id="corr-1",
        causation_id="cause-1",
        reply_to="queue.replies",
        expires_at=expires_at,
        message_id="msg-1",
        created_at=created_at,
    )

    decoded = MessageEnvelope.from_dict(serializer.loads(serializer.dump(envelope)))

    assert decoded == envelope
    assert decoded.payload["scheduled_for"] == created_at
    assert decoded.payload["nested"][1]["expires_at"] == expires_at
    assert decoded.headers["trace_id"] == UUID("12345678-1234-5678-1234-567812345678")


def test_message_round_trip_and_envelope_conversion() -> None:
    message = EventMessage(
        message_type="student.enrolled",
        payload={"student_id": "S-1", "year": 2026},
        headers={"source": "tests"},
        correlation_id="corr-2",
        causation_id="cause-2",
        reply_to="queue.replies",
    )

    decoded = EventMessage.from_dict(message.to_dict())
    envelope = message.to_envelope(message_id="msg-2")
    round_tripped = EventMessage.from_envelope(envelope)

    assert decoded == message
    assert round_tripped == message
    assert envelope.message_type == "student.enrolled"
    assert envelope.message_kind == "event"
    assert envelope.message_id == "msg-2"


def test_registry_dispatch_and_listener_with_memory_transport() -> None:
    registry = Registry()
    transport = MemoryTransport()
    serializer = JsonSerializer()
    dispatcher = Dispatcher(registry=registry, serializer=serializer)

    received: list[EventMessage] = []

    def handle_event(message: EventMessage) -> str:
        received.append(message)
        return "handled"

    registry.register("event", "student.enrolled", handle_event)

    listener = pybus.Listener(
        transport=transport,
        dispatcher=dispatcher,
        serializer=serializer,
    )

    envelope = EventMessage(
        message_type="student.enrolled",
        payload={"student_id": "S-1"},
        headers={"source": "tests"},
        correlation_id="corr-3",
    ).to_envelope(message_id="msg-3")

    transport.publish("pybus.events", serializer.dump(envelope))

    result = listener.listen_once("pybus.events")

    assert result == "handled"
    assert received[0].message_type == "student.enrolled"
    assert received[0].payload == {"student_id": "S-1"}
    assert received[0].correlation_id == "corr-3"
