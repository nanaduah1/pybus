from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pybus.dispatcher import Dispatcher
from pybus.envelope import MessageEnvelope
from pybus.handlers import ContinueProcessing, batched_event_handler, event_handler
from pybus.listener import DEFAULT_FAILED_QUEUE_NAME, DEFAULT_QUEUE_NAME, Listener
from pybus.messages import EventMessage
from pybus.registry import Registry
from pybus.retries import RetryPolicy
from pybus.serializer import JsonSerializer
from pybus.transports.memory import MemoryTransport


class RecordingTransport(MemoryTransport):
    def __init__(self) -> None:
        super().__init__()
        self.consume_calls: list[tuple[str, int]] = []

    def consume(self, channel: str, timeout: int = 5) -> bytes | None:
        self.consume_calls.append((channel, timeout))
        return super().consume(channel, timeout=timeout)


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


def test_listener_consumes_first_available_channel_from_sequence() -> None:
    transport = RecordingTransport()
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
    assert transport.consume_calls == [("queue.zero", 1), ("queue.one", 1)]


def test_listener_blocks_boundedly_when_sequence_is_empty() -> None:
    transport = RecordingTransport()
    listener = Listener(transport=transport, serializer=JsonSerializer())

    result = listener.listen_once(("queue.zero", "queue.one"))

    assert result is None
    assert transport.consume_calls == [("queue.zero", 1), ("queue.one", 1)]


def test_listener_requeues_continuation_on_same_queue() -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    registry = Registry()
    dispatcher = Dispatcher(registry=registry, serializer=serializer)
    listener = Listener(
        transport=transport,
        dispatcher=dispatcher,
        serializer=serializer,
    )
    handled: list[str] = []

    @event_handler("billing.chunked")
    def handle_event(message: EventMessage) -> ContinueProcessing:
        handled.append(message.payload["batch_id"])
        return ContinueProcessing()

    registry.register("event", "billing.chunked", handle_event)
    transport.publish(
        DEFAULT_QUEUE_NAME,
        serializer.dump(
            EventMessage(
                message_type="billing.chunked",
                payload={"batch_id": "B-1"},
            ).to_envelope(message_id="msg-cont")
        ),
    )

    assert listener.listen_once(DEFAULT_QUEUE_NAME) is None
    assert handled == ["B-1"]
    assert transport.size(DEFAULT_QUEUE_NAME) == 1


def test_listener_flushes_batched_handlers_by_size() -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    registry = Registry()
    dispatcher = Dispatcher(registry=registry, serializer=serializer)
    listener = Listener(
        transport=transport,
        dispatcher=dispatcher,
        serializer=serializer,
    )
    flushed: list[list[str]] = []

    @batched_event_handler("audit.log", batch_size=2, max_wait=30)
    def handle_batch(messages: list[EventMessage]) -> None:
        flushed.append([message.payload["entry"] for message in messages])

    registry.register("event", "audit.log", handle_batch, allow_multiple=True)
    for entry in ("one", "two"):
        transport.publish(
            DEFAULT_QUEUE_NAME,
            serializer.dump(
                EventMessage(
                    message_type="audit.log",
                    payload={"entry": entry},
                ).to_envelope()
            ),
        )

    assert listener.listen_once(DEFAULT_QUEUE_NAME) is None
    assert listener.listen_once(DEFAULT_QUEUE_NAME) is None
    assert flushed == [["one", "two"]]
    assert transport.size("batched:audit.log") == 0


def test_listener_flushes_batched_handlers_after_max_wait() -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    registry = Registry()
    dispatcher = Dispatcher(registry=registry, serializer=serializer)
    listener = Listener(
        transport=transport,
        dispatcher=dispatcher,
        serializer=serializer,
    )
    flushed: list[list[str]] = []

    @batched_event_handler("audit.log", batch_size=10, max_wait=5)
    def handle_batch(messages: list[EventMessage]) -> None:
        flushed.append([message.payload["entry"] for message in messages])

    registry.register("event", "audit.log", handle_batch, allow_multiple=True)
    transport.publish(
        DEFAULT_QUEUE_NAME,
        serializer.dump(
            EventMessage(
                message_type="audit.log",
                payload={"entry": "late"},
            ).to_envelope()
        ),
    )

    assert listener.listen_once(DEFAULT_QUEUE_NAME) is None
    listener._batch_started_at["audit.log"] = datetime.now(timezone.utc) - timedelta(
        seconds=6
    )

    assert listener.listen_once(DEFAULT_QUEUE_NAME) is None
    assert flushed == [["late"]]


def test_failed_batch_retries_then_dead_letters_with_audit_identity() -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    registry = Registry()
    dispatcher = Dispatcher(registry=registry, serializer=serializer)
    listener = Listener(
        transport=transport,
        dispatcher=dispatcher,
        serializer=serializer,
        dead_letter_channel=DEFAULT_FAILED_QUEUE_NAME,
    )

    @batched_event_handler("audit.log", batch_size=1, max_wait=0, retry_limit=1)
    def fail_batch(messages: list[EventMessage]) -> None:
        raise RuntimeError(messages[0].message_type)

    registry.register("event", "audit.log", fail_batch, allow_multiple=True)
    envelope = EventMessage(
        message_type="audit.log",
        payload={"entry": "one"},
        headers={"tenant_id": 42, "actor_id": 9},
        correlation_id="corr-1",
        causation_id="cause-1",
    ).to_envelope(message_id="msg-batch")
    transport.publish(DEFAULT_QUEUE_NAME, serializer.dump(envelope))

    listener.listen_once(DEFAULT_QUEUE_NAME)
    assert transport.size("batched:audit.log") == 1
    listener._batch_retry_after.pop("audit.log", None)
    listener._flush_ready_batches()

    assert transport.size("batched:audit.log") == 0
    dead_letter = MessageEnvelope.from_dict(
        serializer.loads(transport.consume(DEFAULT_FAILED_QUEUE_NAME))
    )
    assert dead_letter.message_id == "msg-batch"
    assert dead_letter.correlation_id == "corr-1"
    assert dead_letter.causation_id == "cause-1"
    assert dead_letter.headers["tenant_id"] == 42
    assert dead_letter.headers["actor_id"] == 9
    assert dead_letter.headers["retries"] == 1
    assert dead_letter.headers["dead_lettered_from"] == "batched:audit.log"
