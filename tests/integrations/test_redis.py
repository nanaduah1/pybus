from __future__ import annotations

import base64
import pickle
from types import SimpleNamespace

import pytest

import pybus.integrations.redis as redis_integration
from pybus import (
    CommandDeliveryOutcome,
    CommandDeliveryStatus,
    command,
    command_handler,
)
from pybus.bus import Pybus
from pybus.dispatcher import Dispatcher
from pybus.envelope import MessageEnvelope
from pybus.handlers import ContinueProcessing, batched_event_handler, event_handler
from pybus.exceptions import DeserializationError, IndeterminateDeliveryError
from pybus.integrations.redis import (
    RedisScheduleStateStore,
    RedisTransport,
    decode_json_redis_payload,
    decode_legacy_redis_payload,
    decode_trusted_legacy_redis_payload,
)
from pybus.serializer import JsonSerializer
from pybus.listener import (
    DEFAULT_FAILED_QUEUE_NAME,
    DEFAULT_QUEUE_NAME,
    DEFAULT_SLOW_QUEUE_NAME,
    Listener,
)
from pybus.messages import EventMessage
from pybus.registry import Registry
from pybus.scheduling import configure_scheduler
from pybus.worker import Worker


@command("billing.redis_generate")
class RedisGenerateBill:
    student_id: int


class FakeRedisClient:
    def __init__(self) -> None:
        self.queues: dict[str, list[bytes]] = {}
        self.values: dict[str, bytes] = {}
        self.brpop_error: Exception | None = None
        self.eval_error: Exception | None = None
        self.get_error: Exception | None = None
        self.set_error: Exception | None = None
        self.brpop_calls = 0
        self.eval_calls = 0

    def lpush(self, channel: str, message: bytes) -> None:
        self.queues.setdefault(channel, []).insert(0, message)

    def brpop(self, channel: str, timeout: int = 5):
        self.brpop_calls += 1
        if self.brpop_error is not None:
            raise self.brpop_error
        queue = self.queues.get(channel, [])
        if not queue:
            return None
        return channel, queue.pop()

    def llen(self, channel: str) -> int:
        return len(self.queues.get(channel, []))

    def eval(self, script: str, key_count: int, channel: str, limit: int):
        self.eval_calls += 1
        if self.eval_error is not None:
            raise self.eval_error
        del script, key_count
        queue = self.queues.get(channel, [])
        claimed = queue[:limit]
        self.queues[channel] = queue[limit:]
        return claimed

    def get(self, key: str) -> bytes | None:
        if self.get_error is not None:
            raise self.get_error
        return self.values.get(key)

    def set(self, key: str, value: str) -> None:
        if self.set_error is not None:
            raise self.set_error
        self.values[key] = value.encode("utf-8")


def test_redis_schedule_state_store_round_trips_text() -> None:
    client = FakeRedisClient()
    store = RedisScheduleStateStore(client=client)

    assert store.get("missing") is None

    store.set("pybus.scheduler.state:reports", '{"version":1}')

    assert store.get("pybus.scheduler.state:reports") == '{"version":1}'


def test_configure_scheduler_accepts_redis_state_store() -> None:
    store = RedisScheduleStateStore(client=FakeRedisClient())

    scheduler = configure_scheduler(state_store=store)

    assert scheduler.state_store is store


def test_redis_schedule_state_store_accepts_string_responses() -> None:
    class StringClient:
        def __init__(self) -> None:
            self.value: str | None = None

        def get(self, key: str) -> str | None:
            return self.value

        def set(self, key: str, value: str) -> None:
            self.value = value

    client = StringClient()
    store = RedisScheduleStateStore(client=client)
    store.set("state", "value")

    assert store.get("state") == "value"


def test_redis_schedule_state_store_rejects_unexpected_response_type() -> None:
    class InvalidClient:
        def get(self, key: str) -> int:
            return 42

        def set(self, key: str, value: str) -> None:
            return None

    store = RedisScheduleStateStore(client=InvalidClient())

    with pytest.raises(TypeError, match="bytes, str, or None"):
        store.get("state")


def test_redis_schedule_state_store_propagates_client_errors() -> None:
    client = FakeRedisClient()
    store = RedisScheduleStateStore(client=client)
    client.get_error = ConnectionError("read failed")

    with pytest.raises(ConnectionError, match="read failed"):
        store.get("state")

    client.get_error = None
    client.set_error = ConnectionError("write failed")
    with pytest.raises(ConnectionError, match="write failed"):
        store.set("state", "value")


def test_redis_schedule_state_store_builds_client_lazily_from_url(monkeypatch) -> None:
    client = FakeRedisClient()

    class StrictRedis:
        @staticmethod
        def from_url(url: str):
            assert url == "redis://scheduler"
            return client

    redis_module = SimpleNamespace(StrictRedis=StrictRedis)
    monkeypatch.setattr(
        redis_integration,
        "import_module",
        lambda name: redis_module,
    )

    store = RedisScheduleStateStore(url="redis://scheduler")
    store.set("state", "value")

    assert store.get("state") == "value"


def test_redis_schedule_state_store_url_requires_optional_dependency(
    monkeypatch,
) -> None:
    def missing_redis(name: str):
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(redis_integration, "import_module", missing_redis)

    with pytest.raises(RuntimeError, match="RedisScheduleStateStore from a url"):
        RedisScheduleStateStore(url="redis://scheduler")


def test_redis_transport_publish_and_consume() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)

    transport.publish("pybus.jobs", b"payload")

    assert transport.consume("pybus.jobs") == b"payload"
    assert transport.consume("pybus.jobs") is None


def test_redis_preserves_prepared_identity_and_command_retry_outcomes() -> None:
    outcomes: list[CommandDeliveryOutcome] = []
    client = FakeRedisClient()

    @command_handler(RedisGenerateBill)
    def fail(command_message: RedisGenerateBill) -> None:
        raise RuntimeError("billing unavailable")

    bus = Pybus(
        transport=RedisTransport(client=client),
        handler_targets=[fail],
        command_delivery_observers=[outcomes.append],
    )
    prepared = bus.prepare_command(
        RedisGenerateBill(student_id=7),
        message_id="job-redis-42",
    )
    restored = MessageEnvelope.from_dict(prepared.to_dict())
    bus.publish_prepared(restored)

    for _ in range(bus.listener.retry_policy.max_retries + 1):
        assert bus.listen_once(DEFAULT_QUEUE_NAME) is None

    assert [outcome.status for outcome in outcomes] == [
        status
        for retry_count in range(bus.listener.retry_policy.max_retries + 1)
        for status in (
            CommandDeliveryStatus.STARTED,
            (
                CommandDeliveryStatus.RETRY_SCHEDULED
                if retry_count < bus.listener.retry_policy.max_retries
                else CommandDeliveryStatus.DEAD_LETTERED
            ),
        )
    ]
    assert all(outcome.message_id == "job-redis-42" for outcome in outcomes)
    assert [
        outcome.retry_count
        for outcome in outcomes
        if outcome.status == CommandDeliveryStatus.RETRY_SCHEDULED
    ] == list(range(1, bus.listener.retry_policy.max_retries + 1))
    failed_raw = bus.transport.consume(DEFAULT_FAILED_QUEUE_NAME)
    failed = MessageEnvelope.from_dict(bus.serializer.loads(failed_raw))
    assert failed.message_id == "job-redis-42"
    assert failed.headers["retries"] == bus.listener.retry_policy.max_retries


def test_redis_default_and_slow_workers_dispatch_distinct_queues() -> None:
    client = FakeRedisClient()
    bus = Pybus(transport=RedisTransport(client=client))
    handled: list[tuple[str, object]] = []
    bus.dispatcher.registry.register(
        "event",
        "smoke.default",
        lambda message: handled.append(("default", message.payload)),
    )
    bus.dispatcher.registry.register(
        "event",
        "smoke.slow",
        lambda message: handled.append(("slow", message.payload)),
    )
    bus.publish_event(EventMessage(message_type="smoke.default", payload={"id": 1}))
    bus.publish_event(
        EventMessage(message_type="smoke.slow", payload={"id": 2}),
        queue=bus.topology.slow_queue,
    )
    dead_letter_sentinel = bus.serializer.dump(
        EventMessage(message_type="smoke.failed", payload={"id": 3}).to_envelope()
    )
    bus.transport.publish(bus.topology.dead_letter_queue, dead_letter_sentinel)

    bus.create_worker(error_delay=0).run(max_iterations=1)
    bus.create_worker(bus.topology.slow_queue, error_delay=0).run(max_iterations=1)

    assert handled == [("default", {"id": 1}), ("slow", {"id": 2})]
    assert bus.transport.size(bus.topology.dead_letter_queue) == 1
    assert bus.transport.consume(bus.topology.dead_letter_queue) == dead_letter_sentinel


def test_redis_continuation_waits_before_publishing_to_declared_queue() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    sleep_calls: list[float] = []

    def sleep_fn(delay: float) -> None:
        assert transport.size(DEFAULT_SLOW_QUEUE_NAME) == 0
        sleep_calls.append(delay)

    bus = Pybus(transport=transport)
    bus.listener._sleep_fn = sleep_fn

    @event_handler("workflow.continue")
    def continue_work(message: EventMessage) -> ContinueProcessing:
        return ContinueProcessing(queue=DEFAULT_SLOW_QUEUE_NAME, delay=0.05)

    bus.dispatcher.registry.register("event", "workflow.continue", continue_work)
    bus.publish_event(EventMessage(message_type="workflow.continue", payload={"id": 1}))

    bus.listener.listen_once(DEFAULT_QUEUE_NAME)

    assert sleep_calls == [0.05]
    assert transport.size(DEFAULT_SLOW_QUEUE_NAME) == 1


def test_redis_worker_aborts_on_indeterminate_destructive_pop() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    listener = Listener(transport=transport, serializer=JsonSerializer())
    transport.publish(DEFAULT_QUEUE_NAME, b"later-work")
    client.brpop_error = ConnectionError("reply lost")

    with pytest.raises(IndeterminateDeliveryError, match="destructive-pop"):
        Worker(listener, DEFAULT_QUEUE_NAME, error_delay=0).run()

    assert client.brpop_calls == 1
    assert transport.size(DEFAULT_QUEUE_NAME) == 1


def test_redis_worker_aborts_on_indeterminate_batch_claim() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    serializer = JsonSerializer()
    registry = Registry()
    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
    )

    @batched_event_handler("audit.log", batch_size=1, max_wait=0)
    def handle_batch(messages: list[EventMessage]) -> None:
        raise AssertionError("batch outcome is unknown")

    registry.register("event", "audit.log", handle_batch, allow_multiple=True)
    transport.publish(
        "batched:audit.log",
        serializer.dump(
            EventMessage(
                message_type="audit.log", payload={"entry": "claimed"}
            ).to_envelope()
        ),
    )
    transport.publish(DEFAULT_QUEUE_NAME, b"later-work")
    client.eval_error = ConnectionError("batch reply lost")

    with pytest.raises(IndeterminateDeliveryError, match="batch consume"):
        Worker(listener, DEFAULT_QUEUE_NAME, error_delay=0).run()

    assert client.eval_calls == 1
    assert client.brpop_calls == 0
    assert transport.size(DEFAULT_QUEUE_NAME) == 1


def test_redis_batch_retry_is_bounded_and_failed_queue_is_terminal() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    serializer = JsonSerializer()
    registry = Registry()
    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
    )
    attempts = 0

    @batched_event_handler("audit.log", batch_size=1, max_wait=0, retry_limit=1)
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
            ).to_envelope(message_id="redis-msg")
        ),
    )

    listener.listen_once(DEFAULT_QUEUE_NAME)
    listener.listen_once(DEFAULT_QUEUE_NAME)

    assert attempts == 2
    assert transport.size("batched:audit.log") == 0
    assert transport.size(DEFAULT_FAILED_QUEUE_NAME) == 1


def test_redis_malformed_batch_item_does_not_drop_valid_sibling() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
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
    transport.publish(
        "batched:audit.log",
        serializer.dump(
            EventMessage(
                message_type="audit.log",
                payload={"entry": "valid"},
            ).to_envelope()
        ),
    )
    malformed = b"not-json"
    transport.publish("batched:audit.log", malformed)

    listener.listen_once(DEFAULT_QUEUE_NAME)

    assert handled == ["valid"]
    raw_poison = transport.consume(DEFAULT_FAILED_QUEUE_NAME)
    poison = MessageEnvelope.from_dict(serializer.loads(raw_poison))
    assert poison.message_type == "pybus.message.decode_failed"
    assert poison.headers == {"dead_lettered_from": "batched:audit.log"}
    assert poison.payload["raw_message_encoding"] == "base64"
    assert poison.payload["error_type"] == "DeserializationError"
    assert base64.b64decode(poison.payload["raw_message"]) == malformed
    assert poison.content_encoding is None
    transport.publish(DEFAULT_FAILED_QUEUE_NAME, raw_poison)
    assert listener.listen_once(DEFAULT_FAILED_QUEUE_NAME) is None
    assert transport.size(DEFAULT_FAILED_QUEUE_NAME) == 1


def test_redis_worker_dead_letters_malformed_message_and_continues() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    serializer = JsonSerializer()
    registry = Registry()
    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
    )
    handled: list[str] = []
    registry.register(
        "event",
        "audit.log",
        lambda message: handled.append(message.payload["entry"]),
    )
    malformed = b"not-json"
    transport.publish(DEFAULT_QUEUE_NAME, malformed)
    transport.publish(
        DEFAULT_QUEUE_NAME,
        serializer.dump(
            EventMessage(
                message_type="audit.log",
                payload={"entry": "valid"},
            ).to_envelope()
        ),
    )

    Worker(listener, DEFAULT_QUEUE_NAME, error_delay=0).run(max_iterations=2)

    assert handled == ["valid"]
    raw_poison = transport.consume(DEFAULT_FAILED_QUEUE_NAME)
    poison = MessageEnvelope.from_dict(serializer.loads(raw_poison))
    assert poison.message_type == "pybus.message.decode_failed"
    assert poison.headers == {"dead_lettered_from": DEFAULT_QUEUE_NAME}
    assert base64.b64decode(poison.payload["raw_message"]) == malformed


def test_decode_json_redis_payload_only_accepts_json() -> None:
    assert decode_json_redis_payload(JsonSerializer().dump({"hello": "world"})) == {
        "hello": "world"
    }

    with pytest.raises(DeserializationError):
        decode_json_redis_payload(pickle.dumps({"legacy": True}))


@pytest.mark.parametrize(
    "decoder", [decode_legacy_redis_payload, decode_trusted_legacy_redis_payload]
)
@pytest.mark.parametrize(
    "payload,expected",
    [
        (JsonSerializer().dump({"hello": "world"}), {"hello": "world"}),
        (pickle.dumps({"legacy": True}), {"legacy": True}),
    ],
)
def test_legacy_redis_decoders_handle_json_and_pickle(
    decoder, payload: bytes, expected: dict[str, object]
) -> None:
    assert decoder(payload) == expected
