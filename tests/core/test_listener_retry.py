from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone

import pytest

from pybus import Pybus, event
from pybus.dispatcher import Dispatcher
from pybus.envelope import MessageEnvelope
from pybus.exceptions import IndeterminateDeliveryError
from pybus.handlers import ContinueProcessing, batched_event_handler, event_handler
from pybus.listener import (
    DEFAULT_FAILED_QUEUE_NAME,
    DEFAULT_QUEUE_NAME,
    DEFAULT_SLOW_QUEUE_NAME,
    Listener,
)
from pybus.messages import EventMessage
from pybus.queues import QueueTopology
from pybus.registry import Registry
from pybus.retries import RetryPolicy
from pybus.serializer import JsonSerializer
from pybus.transports.memory import MemoryTransport
from pybus.worker import Worker


class RecordingTransport(MemoryTransport):
    def __init__(self) -> None:
        super().__init__()
        self.consume_calls: list[tuple[str, int]] = []

    def consume(self, channel: str, timeout: int = 5) -> bytes | None:
        self.consume_calls.append((channel, timeout))
        return super().consume(channel, timeout=timeout)


class SettlementFailureTransport(MemoryTransport):
    def __init__(self) -> None:
        super().__init__()
        self.fail_channels: set[str] = set()

    def publish(self, channel: str, message: bytes) -> None:
        if channel in self.fail_channels:
            raise ConnectionError(f"failed to publish to {channel}")
        super().publish(channel, message)

    def seed(self, channel: str, message: bytes) -> None:
        super().publish(channel, message)


class MutableClock:
    def __init__(self) -> None:
        self.current = datetime(2026, 7, 13, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.current

    def advance(self, seconds: int) -> None:
        self.current += timedelta(seconds=seconds)


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


def test_listener_waits_before_republishing_paced_continuation() -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    registry = Registry()
    sleep_calls: list[float] = []

    def sleep_fn(delay: float) -> None:
        assert transport.size(DEFAULT_SLOW_QUEUE_NAME) == 0
        sleep_calls.append(delay)

    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
        topology=QueueTopology(),
        sleep_fn=sleep_fn,
    )

    @event_handler("billing.chunked")
    def handle_event(message: EventMessage) -> ContinueProcessing:
        return ContinueProcessing(queue=DEFAULT_SLOW_QUEUE_NAME, delay=0.05)

    registry.register("event", "billing.chunked", handle_event)
    original = EventMessage(
        message_type="billing.chunked",
        payload={"batch_id": "B-2"},
        headers={"tenant_id": 42, "retries": 3},
        correlation_id="corr-1",
        causation_id="cause-1",
        reply_to="reply.queue",
        expires_at=datetime(2026, 7, 14, tzinfo=timezone.utc),
        content_type="application/json",
        content_encoding="utf-8",
    ).to_envelope(message_id="msg-paced")
    transport.publish(DEFAULT_QUEUE_NAME, serializer.dump(original))

    assert listener.listen_once(DEFAULT_QUEUE_NAME) is None

    assert sleep_calls == [0.05]
    assert transport.size(DEFAULT_QUEUE_NAME) == 0
    requeued = MessageEnvelope.from_dict(
        serializer.loads(transport.consume(DEFAULT_SLOW_QUEUE_NAME))
    )
    assert requeued.to_dict() == original.to_dict()


def test_zero_delay_continuation_does_not_sleep() -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    registry = Registry()
    sleep_calls: list[float] = []
    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
        sleep_fn=sleep_calls.append,
    )

    @event_handler("billing.chunked")
    def handle_event(message: EventMessage) -> ContinueProcessing:
        return ContinueProcessing()

    registry.register("event", "billing.chunked", handle_event)
    transport.publish(
        DEFAULT_QUEUE_NAME,
        serializer.dump(
            EventMessage(
                message_type="billing.chunked", payload={"batch_id": "B-3"}
            ).to_envelope()
        ),
    )

    listener.listen_once(DEFAULT_QUEUE_NAME)

    assert sleep_calls == []


def test_worker_paces_every_repeated_continuation_without_consuming_retries() -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    registry = Registry()
    sleep_calls: list[float] = []
    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
        sleep_fn=sleep_calls.append,
    )

    @event_handler("billing.chunked")
    def handle_event(message: EventMessage) -> ContinueProcessing:
        return ContinueProcessing(delay=0.05)

    registry.register("event", "billing.chunked", handle_event)
    transport.publish(
        DEFAULT_QUEUE_NAME,
        serializer.dump(
            EventMessage(
                message_type="billing.chunked",
                payload={"batch_id": "B-pace"},
                headers={"retries": 2},
            ).to_envelope(message_id="msg-repeated")
        ),
    )

    Worker(listener, DEFAULT_QUEUE_NAME, error_delay=0).run(max_iterations=3)

    assert sleep_calls == [0.05, 0.05, 0.05]
    raw_requeued = transport.consume(DEFAULT_QUEUE_NAME)
    requeued = MessageEnvelope.from_dict(serializer.loads(raw_requeued))
    assert requeued.message_id == "msg-repeated"
    assert requeued.headers["retries"] == 2


def test_typed_event_continuation_preserves_typed_payload() -> None:
    @event("workflow.typed_continuation")
    class ContinueChunk:
        chunk_id: int

    transport = MemoryTransport()
    bus = Pybus(transport=transport)

    @event_handler(ContinueChunk)
    def handle_event(message: ContinueChunk) -> ContinueProcessing:
        return ContinueProcessing(delay=0.01)

    bus.dispatcher.registry.register(
        "event",
        ContinueChunk.message_type,
        handle_event,
        message_class=ContinueChunk,
        allow_multiple=True,
    )
    bus.listener._sleep_fn = lambda delay: None
    bus.publish_event(ContinueChunk(chunk_id=7))

    bus.listener.listen_once(DEFAULT_QUEUE_NAME)

    raw_requeued = transport.consume(DEFAULT_QUEUE_NAME)
    envelope = MessageEnvelope.from_dict(bus.serializer.loads(raw_requeued))
    decoded = bus.dispatcher.decode(envelope)
    assert decoded == ContinueChunk(chunk_id=7)


@pytest.mark.parametrize("queue", ["undeclared.queue", DEFAULT_FAILED_QUEUE_NAME])
def test_composed_listener_rejects_unsafe_continuation_queue(queue: str) -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    registry = Registry()
    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
        topology=QueueTopology(),
        retry_policy=RetryPolicy(max_retries=0),
    )

    @event_handler("billing.chunked")
    def handle_event(message: EventMessage) -> ContinueProcessing:
        return ContinueProcessing(queue=queue)

    registry.register("event", "billing.chunked", handle_event)
    transport.publish(
        DEFAULT_QUEUE_NAME,
        serializer.dump(
            EventMessage(
                message_type="billing.chunked", payload={"batch_id": "B-4"}
            ).to_envelope()
        ),
    )

    listener.listen_once(DEFAULT_QUEUE_NAME)

    assert transport.size(DEFAULT_FAILED_QUEUE_NAME) == 1
    raw_failed = transport.consume(DEFAULT_FAILED_QUEUE_NAME)
    failed = MessageEnvelope.from_dict(serializer.loads(raw_failed))
    assert failed.headers["dead_lettered_from"] == DEFAULT_QUEUE_NAME
    if queue != DEFAULT_FAILED_QUEUE_NAME:
        assert transport.size(queue) == 0


def test_worker_aborts_after_retry_publication_fails_post_claim() -> None:
    transport = SettlementFailureTransport()
    serializer = JsonSerializer()
    registry = Registry()
    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
        retry_policy=RetryPolicy(max_retries=1),
    )
    registry.register(
        "event",
        "billing.fail",
        lambda message: (_ for _ in ()).throw(RuntimeError("handler failed")),
    )
    for message_id in ("first", "second"):
        transport.seed(
            DEFAULT_QUEUE_NAME,
            serializer.dump(
                EventMessage(
                    message_type="billing.fail",
                    payload={"id": message_id},
                ).to_envelope(message_id=message_id)
            ),
        )
    transport.fail_channels.add(DEFAULT_QUEUE_NAME)

    with pytest.raises(IndeterminateDeliveryError, match="retry requeue"):
        Worker(listener, DEFAULT_QUEUE_NAME, error_delay=0).run()

    assert transport.size(DEFAULT_QUEUE_NAME) == 1


def test_worker_aborts_after_dead_letter_publication_fails_post_claim() -> None:
    transport = SettlementFailureTransport()
    serializer = JsonSerializer()
    listener = Listener(transport=transport, serializer=serializer)
    for message_id in ("first", "second"):
        transport.seed(
            DEFAULT_QUEUE_NAME,
            serializer.dump(
                EventMessage(
                    message_type="unhandled.event",
                    payload={"id": message_id},
                ).to_envelope(message_id=message_id)
            ),
        )
    transport.fail_channels.add(DEFAULT_FAILED_QUEUE_NAME)

    with pytest.raises(IndeterminateDeliveryError, match="dead-letter publication"):
        Worker(listener, DEFAULT_QUEUE_NAME, error_delay=0).run()

    assert transport.size(DEFAULT_QUEUE_NAME) == 1


def test_worker_aborts_after_poison_publication_fails_post_claim() -> None:
    transport = SettlementFailureTransport()
    listener = Listener(transport=transport, serializer=JsonSerializer())
    transport.seed(DEFAULT_QUEUE_NAME, b"not-json")
    transport.seed(DEFAULT_QUEUE_NAME, b"must-not-be-consumed")
    transport.fail_channels.add(DEFAULT_FAILED_QUEUE_NAME)

    with pytest.raises(IndeterminateDeliveryError, match="decode-failure publication"):
        Worker(listener, DEFAULT_QUEUE_NAME, error_delay=0).run()

    assert transport.size(DEFAULT_QUEUE_NAME) == 1


def test_worker_aborts_after_continuation_publication_fails_post_claim() -> None:
    transport = SettlementFailureTransport()
    serializer = JsonSerializer()
    registry = Registry()
    sleep_calls: list[float] = []
    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
        sleep_fn=sleep_calls.append,
    )

    @event_handler("billing.chunked")
    def continue_event(message: EventMessage) -> ContinueProcessing:
        return ContinueProcessing(delay=0.05)

    registry.register("event", "billing.chunked", continue_event)
    for message_id in ("first", "second"):
        transport.seed(
            DEFAULT_QUEUE_NAME,
            serializer.dump(
                EventMessage(
                    message_type="billing.chunked",
                    payload={"id": message_id},
                ).to_envelope(message_id=message_id)
            ),
        )
    transport.fail_channels.add(DEFAULT_QUEUE_NAME)

    with pytest.raises(IndeterminateDeliveryError, match="continuation"):
        Worker(listener, DEFAULT_QUEUE_NAME, error_delay=0).run()

    assert sleep_calls == [0.05]
    assert transport.size(DEFAULT_QUEUE_NAME) == 1


def test_worker_aborts_when_continuation_delay_fails_after_claim() -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    registry = Registry()

    def fail_delay(delay: float) -> None:
        raise RuntimeError(f"clock failed at {delay}")

    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
        sleep_fn=fail_delay,
    )

    @event_handler("billing.chunked")
    def continue_event(message: EventMessage) -> ContinueProcessing:
        return ContinueProcessing(delay=0.05)

    registry.register("event", "billing.chunked", continue_event)
    for message_id in ("first", "second"):
        transport.publish(
            DEFAULT_QUEUE_NAME,
            serializer.dump(
                EventMessage(
                    message_type="billing.chunked",
                    payload={"id": message_id},
                ).to_envelope(message_id=message_id)
            ),
        )

    with pytest.raises(IndeterminateDeliveryError, match="continuation delay"):
        Worker(listener, DEFAULT_QUEUE_NAME, error_delay=0).run()

    assert transport.size(DEFAULT_QUEUE_NAME) == 1


def test_worker_aborts_after_batch_buffer_publication_fails_post_claim() -> None:
    transport = SettlementFailureTransport()
    serializer = JsonSerializer()
    registry = Registry()
    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
    )

    @batched_event_handler("audit.log", batch_size=2, max_wait=30)
    def handle_batch(messages: list[EventMessage]) -> None:
        raise AssertionError("buffering should fail first")

    registry.register("event", "audit.log", handle_batch, allow_multiple=True)
    for message_id in ("first", "second"):
        transport.seed(
            DEFAULT_QUEUE_NAME,
            serializer.dump(
                EventMessage(
                    message_type="audit.log",
                    payload={"id": message_id},
                ).to_envelope(message_id=message_id)
            ),
        )
    transport.fail_channels.add("batched:audit.log")

    with pytest.raises(IndeterminateDeliveryError, match="batch buffering"):
        Worker(listener, DEFAULT_QUEUE_NAME, error_delay=0).run()

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


def test_failed_batch_retry_limit_is_exact_and_failed_queue_is_terminal() -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    registry = Registry()
    dispatcher = Dispatcher(registry=registry, serializer=serializer)
    clock = MutableClock()
    listener = Listener(
        transport=transport,
        dispatcher=dispatcher,
        serializer=serializer,
        dead_letter_channel=DEFAULT_FAILED_QUEUE_NAME,
        now_fn=clock,
    )
    attempts: list[list[str]] = []

    @batched_event_handler("audit.log", batch_size=2, max_wait=5, retry_limit=2)
    def fail_batch(messages: list[EventMessage]) -> None:
        attempts.append([message.payload["entry"] for message in messages])
        raise RuntimeError("batch unavailable")

    registry.register("event", "audit.log", fail_batch, allow_multiple=True)
    for message_id in ("msg-1", "msg-2"):
        transport.publish(
            DEFAULT_QUEUE_NAME,
            serializer.dump(
                EventMessage(
                    message_type="audit.log",
                    payload={"entry": message_id},
                    headers={"tenant_id": 42},
                    correlation_id="corr-1",
                    causation_id="cause-1",
                ).to_envelope(message_id=message_id)
            ),
        )

    listener.listen_once(DEFAULT_QUEUE_NAME)
    listener.listen_once(DEFAULT_QUEUE_NAME)
    assert len(attempts) == 1

    clock.advance(5)
    listener.listen_once(DEFAULT_QUEUE_NAME)
    clock.advance(5)
    listener.listen_once(DEFAULT_QUEUE_NAME)

    assert len(attempts) == 3
    assert transport.size("batched:audit.log") == 0
    assert transport.size(DEFAULT_FAILED_QUEUE_NAME) == 2

    terminal = [
        MessageEnvelope.from_dict(
            serializer.loads(transport.consume(DEFAULT_FAILED_QUEUE_NAME))
        )
        for _ in range(2)
    ]
    assert {envelope.message_id for envelope in terminal} == {"msg-1", "msg-2"}
    assert {envelope.headers["retries"] for envelope in terminal} == {2}
    assert {envelope.headers["tenant_id"] for envelope in terminal} == {42}
    assert {envelope.correlation_id for envelope in terminal} == {"corr-1"}
    assert {envelope.causation_id for envelope in terminal} == {"cause-1"}

    for envelope in terminal:
        transport.publish(DEFAULT_FAILED_QUEUE_NAME, serializer.dump(envelope))
    assert listener.listen_once(DEFAULT_FAILED_QUEUE_NAME) is None
    assert transport.size(DEFAULT_FAILED_QUEUE_NAME) == 2
    assert len(attempts) == 3


@pytest.mark.parametrize(
    "payload_retry,header_retry",
    [(0, 2), ("not-a-count", 2), (-1, 2), (True, 2)],
)
def test_batch_header_retry_count_cannot_be_reset_by_payload_metadata(
    payload_retry: object,
    header_retry: int,
) -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    registry = Registry()
    dispatcher = Dispatcher(registry=registry, serializer=serializer)
    listener = Listener(
        transport=transport,
        dispatcher=dispatcher,
        serializer=serializer,
    )
    attempts = 0

    @batched_event_handler("audit.log", batch_size=1, max_wait=0, retry_limit=2)
    def fail_batch(messages: list[EventMessage]) -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError(messages[0].message_type)

    registry.register("event", "audit.log", fail_batch, allow_multiple=True)
    transport.publish(
        DEFAULT_QUEUE_NAME,
        serializer.dump(
            EventMessage(
                message_type="audit.log",
                payload={"entry": "one", "retries": payload_retry},
                headers={"retries": header_retry},
            ).to_envelope(message_id="msg-conflict")
        ),
    )

    listener.listen_once(DEFAULT_QUEUE_NAME)

    assert attempts == 1
    assert transport.size("batched:audit.log") == 0
    assert transport.size(DEFAULT_FAILED_QUEUE_NAME) == 1


def test_valid_header_retry_count_is_canonical_over_higher_payload_count() -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    registry = Registry()
    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
    )

    @batched_event_handler("audit.log", batch_size=1, max_wait=0, retry_limit=2)
    def fail_batch(messages: list[EventMessage]) -> None:
        raise RuntimeError(messages[0].message_type)

    registry.register("event", "audit.log", fail_batch, allow_multiple=True)
    transport.publish(
        DEFAULT_QUEUE_NAME,
        serializer.dump(
            EventMessage(
                message_type="audit.log",
                payload={"entry": "one", "retries": 99},
                headers={"retries": 0},
            ).to_envelope(message_id="msg-header-canonical")
        ),
    )

    listener.listen_once(DEFAULT_QUEUE_NAME)

    retried = MessageEnvelope.from_dict(
        serializer.loads(transport.consume("batched:audit.log"))
    )
    assert retried.headers["retries"] == 1
    assert retried.payload["retries"] == 1
    assert transport.size(DEFAULT_FAILED_QUEUE_NAME) == 0


def test_failed_batch_partitions_retryable_and_exhausted_envelopes() -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    registry = Registry()
    dispatcher = Dispatcher(registry=registry, serializer=serializer)
    listener = Listener(
        transport=transport,
        dispatcher=dispatcher,
        serializer=serializer,
    )

    @batched_event_handler("audit.log", batch_size=2, max_wait=0, retry_limit=2)
    def fail_batch(messages: list[EventMessage]) -> None:
        raise RuntimeError(messages[0].message_type)

    registry.register("event", "audit.log", fail_batch, allow_multiple=True)
    for message_id, retries in (("retryable", 1), ("exhausted", 2)):
        envelope = EventMessage(
            message_type="audit.log",
            payload={"entry": message_id},
            headers={"retries": retries},
        ).to_envelope(message_id=message_id)
        transport.publish("batched:audit.log", serializer.dump(envelope))

    listener.listen_once(DEFAULT_QUEUE_NAME)

    assert transport.size("batched:audit.log") == 1
    retryable = MessageEnvelope.from_dict(
        serializer.loads(transport.consume("batched:audit.log"))
    )
    assert retryable.message_id == "retryable"
    assert retryable.headers["retries"] == 2
    exhausted = MessageEnvelope.from_dict(
        serializer.loads(transport.consume(DEFAULT_FAILED_QUEUE_NAME))
    )
    assert exhausted.message_id == "exhausted"
    assert exhausted.headers["retries"] == 2


@pytest.mark.parametrize(
    "handler_limit,policy_limit,expected_attempts",
    [(0, 5, 1), (None, 2, 3)],
)
def test_batch_retry_limit_zero_and_listener_default_are_exact(
    handler_limit: int | None,
    policy_limit: int,
    expected_attempts: int,
) -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    registry = Registry()
    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
        retry_policy=RetryPolicy(max_retries=policy_limit),
    )
    attempts = 0

    @batched_event_handler(
        "audit.log",
        batch_size=1,
        max_wait=0,
        retry_limit=handler_limit,
    )
    def fail_batch(messages: list[EventMessage]) -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError(messages[0].message_type)

    registry.register("event", "audit.log", fail_batch, allow_multiple=True)
    transport.publish(
        DEFAULT_QUEUE_NAME,
        serializer.dump(
            EventMessage(
                message_type="audit.log",
                payload={"entry": "one"},
            ).to_envelope()
        ),
    )

    for _ in range(expected_attempts):
        listener.listen_once(DEFAULT_QUEUE_NAME)

    assert attempts == expected_attempts
    assert transport.size("batched:audit.log") == 0
    assert transport.size(DEFAULT_FAILED_QUEUE_NAME) == 1


def test_malformed_batch_item_is_terminal_without_losing_valid_sibling() -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    registry = Registry()
    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
    )
    handled: list[str] = []

    @batched_event_handler("audit.log", batch_size=2, max_wait=0)
    def handle_batch(messages: list[EventMessage]) -> None:
        handled.extend(message.payload["entry"] for message in messages)

    registry.register("event", "audit.log", handle_batch, allow_multiple=True)
    valid = serializer.dump(
        EventMessage(
            message_type="audit.log",
            payload={"entry": "valid"},
        ).to_envelope(message_id="valid-msg")
    )
    malformed = b"not-json"
    transport.publish("batched:audit.log", valid)
    transport.publish("batched:audit.log", malformed)

    listener.listen_once(DEFAULT_QUEUE_NAME)

    assert handled == ["valid"]
    assert transport.size("batched:audit.log") == 0
    poison = MessageEnvelope.from_dict(
        serializer.loads(transport.consume(DEFAULT_FAILED_QUEUE_NAME))
    )
    assert poison.message_type == "pybus.message.decode_failed"
    assert poison.headers == {"dead_lettered_from": "batched:audit.log"}
    assert poison.payload["raw_message_encoding"] == "base64"
    assert poison.payload["error_type"] == "DeserializationError"
    assert base64.b64decode(poison.payload["raw_message"]) == malformed
    assert poison.content_encoding is None


@pytest.mark.parametrize(
    "payload_last_attempt", ["malformed", "2020-01-01T00:00:00+00:00"]
)
def test_valid_header_last_attempt_is_canonical_for_retry_delay(
    payload_last_attempt: str,
) -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    registry = Registry()
    clock = MutableClock()
    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
        now_fn=clock,
    )

    @event_handler("audit.log", delay=10)
    def handle_event(message: EventMessage) -> None:
        return None

    registry.register("event", "audit.log", handle_event)
    envelope = EventMessage(
        message_type="audit.log",
        payload={"entry": "one", "last_attempt": payload_last_attempt},
        headers={"last_attempt": clock.current.isoformat()},
    ).to_envelope()

    assert listener._should_delay(envelope, listener._spec_for(handle_event)) is True


def test_batch_retry_bound_survives_listener_restart() -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    registry = Registry()
    attempts = 0

    @batched_event_handler("audit.log", batch_size=1, max_wait=0, retry_limit=1)
    def fail_batch(messages: list[EventMessage]) -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError(messages[0].message_type)

    registry.register("event", "audit.log", fail_batch, allow_multiple=True)
    first_listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
    )
    transport.publish(
        DEFAULT_QUEUE_NAME,
        serializer.dump(
            EventMessage(
                message_type="audit.log",
                payload={"entry": "one"},
            ).to_envelope()
        ),
    )

    first_listener.listen_once(DEFAULT_QUEUE_NAME)
    restarted_listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
    )
    restarted_listener.listen_once(DEFAULT_QUEUE_NAME)

    assert attempts == 2
    assert transport.size("batched:audit.log") == 0
    assert transport.size(DEFAULT_FAILED_QUEUE_NAME) == 1


def test_naive_header_last_attempt_falls_back_without_consuming_retry() -> None:
    clock = MutableClock()
    listener = Listener(
        transport=MemoryTransport(),
        serializer=JsonSerializer(),
        now_fn=clock,
    )

    @event_handler("audit.log", delay=10)
    def handle_event(message: EventMessage) -> None:
        return None

    envelope = EventMessage(
        message_type="audit.log",
        payload={"last_attempt": clock.current.isoformat()},
        headers={"last_attempt": "2026-07-13T00:00:00"},
    ).to_envelope()

    assert listener._should_delay(envelope, listener._spec_for(handle_event)) is True


def test_malformed_ordinary_message_is_terminal_and_next_message_survives() -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    registry = Registry()
    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
    )
    handled: list[str] = []

    @event_handler("audit.log")
    def handle_event(message: EventMessage) -> None:
        handled.append(message.payload["entry"])

    registry.register("event", "audit.log", handle_event)
    transport.publish(DEFAULT_QUEUE_NAME, b"not-json")
    transport.publish(
        DEFAULT_QUEUE_NAME,
        serializer.dump(
            EventMessage(
                message_type="audit.log",
                payload={"entry": "valid"},
            ).to_envelope()
        ),
    )

    assert listener.listen_once(DEFAULT_QUEUE_NAME) is None
    assert listener.listen_once(DEFAULT_QUEUE_NAME) is None

    assert handled == ["valid"]
    poison = MessageEnvelope.from_dict(
        serializer.loads(transport.consume(DEFAULT_FAILED_QUEUE_NAME))
    )
    assert poison.message_type == "pybus.message.decode_failed"
    assert poison.headers == {"dead_lettered_from": DEFAULT_QUEUE_NAME}


def test_listener_applies_retry_delay_from_policy() -> None:
    sleep_calls: list[float] = []
    registry = Registry()
    transport = MemoryTransport()
    serializer = JsonSerializer()
    dispatcher = Dispatcher(registry=registry, serializer=serializer)
    listener = Listener(
        transport=transport,
        dispatcher=dispatcher,
        serializer=serializer,
        retry_policy=RetryPolicy(max_retries=2, delay=2, backoff_factor=2.0),
        sleep_fn=sleep_calls.append,
    )

    def always_fail(message: EventMessage) -> None:
        raise RuntimeError("fail")

    registry.register("event", "student.enrolled", always_fail)

    envelope = EventMessage(
        message_type="student.enrolled",
        payload={"student_id": "S-delay"},
    ).to_envelope(message_id="msg-delay-1")
    transport.publish(DEFAULT_QUEUE_NAME, serializer.dump(envelope))

    listener.listen_once(DEFAULT_QUEUE_NAME)
    # attempt=0: delay = 2 * (2.0 ** 0) = 2.0
    assert sleep_calls == [2.0]

    retry_envelope = MessageEnvelope.from_dict(
        serializer.loads(transport.consume(DEFAULT_QUEUE_NAME))
    )
    transport.publish(DEFAULT_QUEUE_NAME, serializer.dump(retry_envelope))
    sleep_calls.clear()

    listener.listen_once(DEFAULT_QUEUE_NAME)
    # attempt=1: delay = 2 * (2.0 ** 1) = 4.0
    assert sleep_calls == [4.0]


def test_listener_zero_delay_does_not_sleep() -> None:
    sleep_calls: list[float] = []
    registry = Registry()
    transport = MemoryTransport()
    serializer = JsonSerializer()
    dispatcher = Dispatcher(registry=registry, serializer=serializer)
    listener = Listener(
        transport=transport,
        dispatcher=dispatcher,
        serializer=serializer,
        retry_policy=RetryPolicy(max_retries=1, delay=0),
        sleep_fn=sleep_calls.append,
    )

    def always_fail(message: EventMessage) -> None:
        raise RuntimeError("fail")

    registry.register("event", "student.enrolled", always_fail)

    envelope = EventMessage(
        message_type="student.enrolled",
        payload={"student_id": "S-nodelay"},
    ).to_envelope(message_id="msg-nodelay-1")
    transport.publish(DEFAULT_QUEUE_NAME, serializer.dump(envelope))

    listener.listen_once(DEFAULT_QUEUE_NAME)
    assert sleep_calls == []
