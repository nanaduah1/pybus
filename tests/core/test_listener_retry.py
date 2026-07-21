from __future__ import annotations

from unittest.mock import patch

from pybus.dispatcher import Dispatcher
from pybus.envelope import MessageEnvelope
from pybus.listener import DEFAULT_FAILED_QUEUE_NAME, DEFAULT_QUEUE_NAME, Listener
from pybus.messages import EventMessage
from pybus.registry import Registry
from pybus.serializer import JsonSerializer
from pybus.transports.memory import MemoryTransport
from pybus.retries import RetryPolicy


def test_listener_retries_failed_message_before_dead_letter() -> None:
    registry = Registry()
    transport = MemoryTransport()
    serializer = JsonSerializer()
    dispatcher = Dispatcher(registry=registry, serializer=serializer)
    listener = Listener(
        transport=transport,
        dispatcher=dispatcher,
        serializer=serializer,
        retry_policy=RetryPolicy(max_retries=1),
        dead_letter_channel=DEFAULT_FAILED_QUEUE_NAME,
    )

    def handle_event(message: EventMessage) -> None:
        raise RuntimeError(f"boom: {message.payload['student_id']}")

    registry.register("event", "student.enrolled", handle_event)

    envelope = EventMessage(
        message_type="student.enrolled",
        payload={"student_id": "S-1"},
        headers={"source": "tests"},
    ).to_envelope(message_id="msg-1")

    transport.publish(DEFAULT_QUEUE_NAME, serializer.dump(envelope))

    assert listener.listen_once(DEFAULT_QUEUE_NAME) is None
    assert transport.size(DEFAULT_QUEUE_NAME) == 1

    retry_envelope = MessageEnvelope.from_dict(
        serializer.loads(transport.consume(DEFAULT_QUEUE_NAME))
    )
    assert retry_envelope.payload["retries"] == 1
    assert "last_attempt" in retry_envelope.payload
    assert retry_envelope.headers["retries"] == 1

    transport.publish(DEFAULT_QUEUE_NAME, serializer.dump(retry_envelope))

    assert listener.listen_once(DEFAULT_QUEUE_NAME) is None
    assert transport.size(DEFAULT_QUEUE_NAME) == 0
    assert transport.size(DEFAULT_FAILED_QUEUE_NAME) == 1

    dead_letter = MessageEnvelope.from_dict(
        serializer.loads(transport.consume(DEFAULT_FAILED_QUEUE_NAME))
    )
    assert dead_letter.message_id == "msg-1"
    assert dead_letter.headers["dead_lettered_from"] == DEFAULT_QUEUE_NAME
    assert dead_letter.payload["retries"] == 1


def test_listener_applies_retry_delay_from_policy() -> None:
    registry = Registry()
    transport = MemoryTransport()
    serializer = JsonSerializer()
    dispatcher = Dispatcher(registry=registry, serializer=serializer)
    listener = Listener(
        transport=transport,
        dispatcher=dispatcher,
        serializer=serializer,
        retry_policy=RetryPolicy(max_retries=2, delay=2, backoff_factor=2.0),
    )

    def always_fail(message: EventMessage) -> None:
        raise RuntimeError("fail")

    registry.register("event", "student.enrolled", always_fail)

    envelope = EventMessage(
        message_type="student.enrolled",
        payload={"student_id": "S-delay"},
    ).to_envelope(message_id="msg-delay-1")
    transport.publish(DEFAULT_QUEUE_NAME, serializer.dump(envelope))

    with patch("pybus.listener.time.sleep") as mock_sleep:
        listener.listen_once(DEFAULT_QUEUE_NAME)

    # First retry: attempt=0, delay = 2 * (2.0 ** 0) = 2.0
    mock_sleep.assert_called_once_with(2.0)
    assert transport.size(DEFAULT_QUEUE_NAME) == 1

    retry_envelope = MessageEnvelope.from_dict(
        serializer.loads(transport.consume(DEFAULT_QUEUE_NAME))
    )
    transport.publish(DEFAULT_QUEUE_NAME, serializer.dump(retry_envelope))

    with patch("pybus.listener.time.sleep") as mock_sleep2:
        listener.listen_once(DEFAULT_QUEUE_NAME)

    # Second retry: attempt=1, delay = 2 * (2.0 ** 1) = 4.0
    mock_sleep2.assert_called_once_with(4.0)


def test_listener_zero_delay_does_not_sleep() -> None:
    registry = Registry()
    transport = MemoryTransport()
    serializer = JsonSerializer()
    dispatcher = Dispatcher(registry=registry, serializer=serializer)
    listener = Listener(
        transport=transport,
        dispatcher=dispatcher,
        serializer=serializer,
        retry_policy=RetryPolicy(max_retries=1, delay=0),
    )

    def always_fail(message: EventMessage) -> None:
        raise RuntimeError("fail")

    registry.register("event", "student.enrolled", always_fail)

    envelope = EventMessage(
        message_type="student.enrolled",
        payload={"student_id": "S-nodelay"},
    ).to_envelope(message_id="msg-nodelay-1")
    transport.publish(DEFAULT_QUEUE_NAME, serializer.dump(envelope))

    with patch("pybus.listener.time.sleep") as mock_sleep:
        listener.listen_once(DEFAULT_QUEUE_NAME)

    mock_sleep.assert_not_called()
    assert transport.size(DEFAULT_QUEUE_NAME) == 1


def test_listener_consumes_first_available_channel_from_sequence() -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    registry = Registry()
    dispatcher = Dispatcher(registry=registry, serializer=serializer)
    listener = Listener(
        transport=transport, dispatcher=dispatcher, serializer=serializer
    )

    received: list[str] = []

    def handle_event(message: EventMessage) -> str:
        received.append(message.message_type)
        return "ok"

    registry.register("event", "student.enrolled", handle_event)

    envelope = EventMessage(
        message_type="student.enrolled",
        payload={"student_id": "S-2"},
    ).to_envelope(message_id="msg-2")

    transport.publish("queue.one", serializer.dump(envelope))

    result = listener.listen_once(("queue.zero", "queue.one"))

    assert result == "ok"
    assert received == ["student.enrolled"]
