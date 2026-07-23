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
from pybus.durable import DurableDeliveryDecision
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
from pybus.retries import RetryPolicy
from pybus.scheduling import configure_scheduler
from pybus.worker import Worker


@command("billing.redis_generate")
class RedisGenerateBill:
    student_id: int


class FakeRedisClient:
    def __init__(self) -> None:
        self.queues: dict[str, list[bytes]] = {}
        self.hashes: dict[str, dict[str, bytes]] = {}
        self.values: dict[str, bytes] = {}
        self.blmove_error: Exception | None = None
        self.lmove_error: Exception | None = None
        self.lrem_error: Exception | None = None
        self.hset_error: Exception | None = None
        self.hdel_error: Exception | None = None
        self.lpush_error: Exception | None = None
        self.get_error: Exception | None = None
        self.set_error: Exception | None = None
        self.blmove_calls = 0
        self.lmove_calls = 0

    def lpush(self, channel: str, message: bytes) -> None:
        if self.lpush_error is not None:
            raise self.lpush_error
        self.queues.setdefault(channel, []).insert(0, message)

    def llen(self, channel: str) -> int:
        return len(self.queues.get(channel, []))

    def blmove(
        self,
        first_list: str,
        second_list: str,
        timeout: int,
        src: str = "LEFT",
        dest: str = "RIGHT",
    ):
        self.blmove_calls += 1
        if self.blmove_error is not None:
            raise self.blmove_error
        return self._move(first_list, second_list, src, dest)

    def lmove(
        self,
        first_list: str,
        second_list: str,
        src: str = "LEFT",
        dest: str = "RIGHT",
    ):
        self.lmove_calls += 1
        if self.lmove_error is not None:
            raise self.lmove_error
        return self._move(first_list, second_list, src, dest)

    def _move(self, first_list: str, second_list: str, src: str, dest: str):
        queue = self.queues.get(first_list, [])
        if not queue:
            return None
        value = queue.pop() if src == "RIGHT" else queue.pop(0)
        target = self.queues.setdefault(second_list, [])
        if dest == "LEFT":
            target.insert(0, value)
        else:
            target.append(value)
        return value

    def lrem(self, key: str, count: int, value: bytes) -> int:
        if self.lrem_error is not None:
            raise self.lrem_error
        queue = self.queues.get(key, [])
        limit = abs(count) if count != 0 else len(queue)
        removed = 0
        source = queue if count >= 0 else list(reversed(queue))
        new_queue: list[bytes] = []
        for item in source:
            if item == value and removed < limit:
                removed += 1
                continue
            new_queue.append(item)
        if count < 0:
            new_queue.reverse()
        self.queues[key] = new_queue
        return removed

    def hset(self, key: str, field=None, value=None, mapping=None) -> None:
        if self.hset_error is not None:
            raise self.hset_error
        h = self.hashes.setdefault(key, {})
        if mapping:
            h.update(mapping)
        else:
            h[field] = value

    def hget(self, key: str, field: str):
        return self.hashes.get(key, {}).get(field)

    def hdel(self, key: str, *fields: str) -> int:
        if self.hdel_error is not None:
            raise self.hdel_error
        h = self.hashes.get(key, {})
        removed = 0
        for field in fields:
            if field in h:
                del h[field]
                removed += 1
        return removed

    def get(self, key: str) -> bytes | None:
        if self.get_error is not None:
            raise self.get_error
        return self.values.get(key)

    def set(self, key: str, value: str) -> None:
        if self.set_error is not None:
            raise self.set_error
        self.values[key] = value.encode("utf-8")


class PermissiveDurableStore:
    def __init__(self) -> None:
        self.settlements = []
        self.outcomes = []

    def admit_delivery(self, admission):
        return DurableDeliveryDecision.PROCEED

    def checkpoint_settlement(self, settlement):
        self.settlements.append(settlement)

    def apply_outcome(self, outcome, *, reconciliation_due_at):
        self.outcomes.append(outcome)

    def release_delivery(self, **kwargs):
        return None


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


def test_redis_durable_metadata_survives_retry_and_dead_letter_copies() -> None:
    client = FakeRedisClient()

    @command_handler(RedisGenerateBill)
    def fail(command_message: RedisGenerateBill) -> None:
        raise RuntimeError("billing unavailable")

    bus = Pybus(
        transport=RedisTransport(client=client),
        handler_targets=[fail],
        durable_job_store=PermissiveDurableStore(),
    )
    bus.listener.retry_policy = RetryPolicy(max_retries=1)
    prepared = bus.prepare_command(
        RedisGenerateBill(student_id=8), message_id="redis-durable-retry"
    )
    prepared.headers.update(
        {
            "pybus_durable_record": "record-retry",
            "pybus_durable_generation": 3,
        }
    )
    bus.publish_prepared(prepared)

    bus.listen_once(DEFAULT_QUEUE_NAME)
    retry = MessageEnvelope.from_dict(
        bus.serializer.loads(client.queues[DEFAULT_QUEUE_NAME][-1])
    )
    assert retry.headers["pybus_durable_record"] == "record-retry"
    assert retry.headers["pybus_durable_generation"] == 3
    assert retry.headers["retries"] == 1
    assert "last_attempt" in retry.headers

    bus.listen_once(DEFAULT_QUEUE_NAME)
    failed = MessageEnvelope.from_dict(
        bus.serializer.loads(client.queues[DEFAULT_FAILED_QUEUE_NAME][-1])
    )
    assert failed.headers["pybus_durable_record"] == "record-retry"
    assert failed.headers["pybus_durable_generation"] == 3
    assert failed.headers["retries"] == 1
    assert failed.headers["dead_lettered_from"] == DEFAULT_QUEUE_NAME


def test_redis_durable_metadata_survives_continuation_copy() -> None:
    client = FakeRedisClient()

    @command_handler(RedisGenerateBill)
    def continue_slowly(
        command_message: RedisGenerateBill,
    ) -> ContinueProcessing:
        return ContinueProcessing(queue=DEFAULT_SLOW_QUEUE_NAME)

    bus = Pybus(
        transport=RedisTransport(client=client),
        handler_targets=[continue_slowly],
        durable_job_store=PermissiveDurableStore(),
    )
    prepared = bus.prepare_command(
        RedisGenerateBill(student_id=9), message_id="redis-durable-continuation"
    )
    prepared.headers.update(
        {
            "pybus_durable_record": "record-continuation",
            "pybus_durable_generation": 4,
        }
    )
    bus.publish_prepared(prepared)

    bus.listen_once(DEFAULT_QUEUE_NAME)

    continued = MessageEnvelope.from_dict(
        bus.serializer.loads(client.queues[DEFAULT_SLOW_QUEUE_NAME][-1])
    )
    assert continued.message_id == "redis-durable-continuation"
    assert continued.headers["pybus_durable_record"] == "record-continuation"
    assert continued.headers["pybus_durable_generation"] == 4


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


def test_redis_worker_aborts_on_indeterminate_claim() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    listener = Listener(transport=transport, serializer=JsonSerializer())
    transport.publish(DEFAULT_QUEUE_NAME, b"later-work")
    client.blmove_error = ConnectionError("reply lost")

    with pytest.raises(IndeterminateDeliveryError, match="claim outcome"):
        Worker(listener, DEFAULT_QUEUE_NAME, error_delay=0).run()

    assert client.blmove_calls == 1
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
    client.lmove_error = ConnectionError("batch reply lost")

    with pytest.raises(IndeterminateDeliveryError, match="batch consume"):
        Worker(listener, DEFAULT_QUEUE_NAME, error_delay=0).run()

    assert client.lmove_calls == 1
    assert client.blmove_calls == 0
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


def test_consume_moves_message_into_processing_list_not_destroying_it() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    transport.publish("pybus.jobs", b"payload")

    message = transport.consume("pybus.jobs")

    assert message == b"payload"
    assert client.queues["pybus.jobs"] == []
    assert client.queues["pybus.jobs:processing"] == [b"payload"]


def test_crash_before_ack_leaves_message_in_processing_list() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    transport.publish("pybus.jobs", b"payload")

    transport.consume("pybus.jobs")

    # simulate a crash: nothing else happens, receipt is discarded
    assert client.queues["pybus.jobs"] == []
    assert client.queues["pybus.jobs:processing"] == [b"payload"]


def test_ack_removes_claimed_entry_from_processing_list_and_claims_hash() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    transport.publish("pybus.jobs", b"payload")
    message = transport.consume("pybus.jobs")

    transport.ack(message.receipt)

    assert client.queues["pybus.jobs:processing"] == []
    assert client.queues["pybus.jobs"] == []
    assert client.hashes.get("pybus.jobs:claims", {}) == {}


def test_ack_on_unknown_receipt_is_a_noop() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)

    transport.ack("not-a-real-receipt")
    transport.ack('{"channel": "pybus.jobs", "claim_id": "does-not-exist"}')


def test_ack_is_idempotent_on_already_settled_receipt() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    transport.publish("pybus.jobs", b"payload")
    message = transport.consume("pybus.jobs")

    transport.ack(message.receipt)
    transport.ack(message.receipt)  # second ack is a no-op, not an error

    assert client.queues["pybus.jobs:processing"] == []


def test_nack_is_idempotent_on_already_settled_receipt() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    transport.publish("pybus.jobs", b"payload")
    message = transport.consume("pybus.jobs")

    transport.nack(message.receipt, requeue=True)
    transport.nack(message.receipt, requeue=True)  # second nack is a no-op

    assert client.queues["pybus.jobs"] == [b"payload"]
    assert client.queues["pybus.jobs:processing"] == []


def test_nack_with_requeue_moves_entry_back_to_source_list() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    transport.publish("pybus.jobs", b"payload")
    message = transport.consume("pybus.jobs")

    transport.nack(message.receipt, requeue=True)

    assert client.queues["pybus.jobs"] == [b"payload"]
    assert client.queues["pybus.jobs:processing"] == []
    assert client.hashes.get("pybus.jobs:claims", {}) == {}


def test_nack_without_requeue_drops_entry() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    transport.publish("pybus.jobs", b"payload")
    message = transport.consume("pybus.jobs")

    transport.nack(message.receipt, requeue=False)

    assert client.queues["pybus.jobs"] == []
    assert client.queues["pybus.jobs:processing"] == []
    assert client.hashes.get("pybus.jobs:claims", {}) == {}


def test_ack_removes_exactly_one_duplicate_payload() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    transport.publish("pybus.jobs", b"same-payload")
    transport.publish("pybus.jobs", b"same-payload")

    first = transport.consume("pybus.jobs")
    second = transport.consume("pybus.jobs")
    assert client.queues["pybus.jobs:processing"] == [b"same-payload", b"same-payload"]

    transport.ack(first.receipt)

    assert client.queues["pybus.jobs:processing"] == [b"same-payload"]
    # the second claim is untouched and can still be settled independently
    transport.ack(second.receipt)
    assert client.queues["pybus.jobs:processing"] == []


def test_consume_many_claims_up_to_limit_and_partial_ack_leaves_remainder() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    for i in range(3):
        transport.publish("pybus.jobs", f"payload-{i}".encode())

    claimed = transport.consume_many("pybus.jobs", limit=2)

    assert len(claimed) == 2
    assert client.queues["pybus.jobs"] == [b"payload-2"]
    assert set(client.queues["pybus.jobs:processing"]) == {b"payload-0", b"payload-1"}

    transport.ack(claimed[0].receipt)

    # only one of the two claimed messages is settled; the other survives a crash
    assert len(client.queues["pybus.jobs:processing"]) == 1


def test_consume_many_stops_early_when_source_is_empty() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    transport.publish("pybus.jobs", b"only-one")

    claimed = transport.consume_many("pybus.jobs", limit=5)

    assert len(claimed) == 1
    assert claimed[0] == b"only-one"


def test_consume_raises_indeterminate_delivery_error_on_unknown_outcome() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    transport.publish("pybus.jobs", b"payload")
    client.blmove_error = ConnectionError("connection dropped")

    with pytest.raises(IndeterminateDeliveryError):
        transport.consume("pybus.jobs")


def test_consume_many_raises_indeterminate_delivery_error_on_unknown_outcome() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    transport.publish("pybus.jobs", b"payload")
    client.lmove_error = ConnectionError("connection dropped")

    with pytest.raises(IndeterminateDeliveryError):
        transport.consume_many("pybus.jobs", limit=5)


def test_consume_raises_indeterminate_delivery_error_on_claim_record_failure() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    transport.publish("pybus.jobs", b"payload")
    client.hset_error = ConnectionError("connection dropped")

    with pytest.raises(IndeterminateDeliveryError):
        transport.consume("pybus.jobs")

    # the message survived the failed claim-record write: still recoverable
    assert client.queues["pybus.jobs:processing"] == [b"payload"]


def test_ack_raises_indeterminate_delivery_error_on_unknown_outcome() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    transport.publish("pybus.jobs", b"payload")
    message = transport.consume("pybus.jobs")
    client.lrem_error = ConnectionError("connection dropped")

    with pytest.raises(IndeterminateDeliveryError):
        transport.ack(message.receipt)


def test_nack_requeue_raises_indeterminate_delivery_error_on_unknown_outcome() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    transport.publish("pybus.jobs", b"payload")
    message = transport.consume("pybus.jobs")
    client.lpush_error = ConnectionError("connection dropped")

    with pytest.raises(IndeterminateDeliveryError):
        transport.nack(message.receipt, requeue=True)


def test_redis_listener_acks_plain_success_clears_processing_and_claims() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    serializer = JsonSerializer()
    registry = Registry()
    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
    )
    registry.register("event", "student.enrolled", lambda message: "ok")
    transport.publish(
        DEFAULT_QUEUE_NAME,
        serializer.dump(
            EventMessage(
                message_type="student.enrolled", payload={"student_id": "S-1"}
            ).to_envelope(message_id="redis-ack-1")
        ),
    )

    result = listener.listen_once(DEFAULT_QUEUE_NAME)

    assert result == "ok"
    assert client.queues[f"{DEFAULT_QUEUE_NAME}:processing"] == []
    assert client.hashes.get(f"{DEFAULT_QUEUE_NAME}:claims", {}) == {}


def test_redis_listener_acks_original_claim_after_retry_requeue() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    serializer = JsonSerializer()
    registry = Registry()
    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
        retry_policy=RetryPolicy(max_retries=1),
    )

    def handle_event(message: EventMessage) -> None:
        raise RuntimeError("boom")

    registry.register("event", "billing.fail", handle_event)
    transport.publish(
        DEFAULT_QUEUE_NAME,
        serializer.dump(
            EventMessage(message_type="billing.fail", payload={"id": 1}).to_envelope(
                message_id="redis-retry-1"
            )
        ),
    )

    assert listener.listen_once(DEFAULT_QUEUE_NAME) is None

    assert client.queues[f"{DEFAULT_QUEUE_NAME}:processing"] == []
    assert client.hashes.get(f"{DEFAULT_QUEUE_NAME}:claims", {}) == {}
    assert transport.size(DEFAULT_QUEUE_NAME) == 1
    retried = MessageEnvelope.from_dict(
        serializer.loads(transport.consume(DEFAULT_QUEUE_NAME))
    )
    assert retried.headers["retries"] == 1


def test_redis_listener_acks_original_claim_after_dead_letter_publish() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    serializer = JsonSerializer()
    registry = Registry()
    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
        retry_policy=RetryPolicy(max_retries=0),
    )

    def handle_event(message: EventMessage) -> None:
        raise RuntimeError("boom")

    registry.register("event", "billing.fail", handle_event)
    transport.publish(
        DEFAULT_QUEUE_NAME,
        serializer.dump(
            EventMessage(message_type="billing.fail", payload={"id": 1}).to_envelope(
                message_id="redis-dlq-1"
            )
        ),
    )

    assert listener.listen_once(DEFAULT_QUEUE_NAME) is None

    assert client.queues[f"{DEFAULT_QUEUE_NAME}:processing"] == []
    assert client.hashes.get(f"{DEFAULT_QUEUE_NAME}:claims", {}) == {}
    assert transport.size(DEFAULT_FAILED_QUEUE_NAME) == 1


def test_redis_listener_acks_every_batch_member_claim_on_success() -> None:
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

    @batched_event_handler("audit.log", batch_size=2, max_wait=30)
    def handle_batch(messages: list[EventMessage]) -> None:
        handled.extend(message.payload["entry"] for message in messages)

    registry.register("event", "audit.log", handle_batch, allow_multiple=True)
    for entry in ("one", "two"):
        transport.publish(
            DEFAULT_QUEUE_NAME,
            serializer.dump(
                EventMessage(
                    message_type="audit.log", payload={"entry": entry}
                ).to_envelope()
            ),
        )

    assert listener.listen_once(DEFAULT_QUEUE_NAME) is None
    assert listener.listen_once(DEFAULT_QUEUE_NAME) is None

    assert handled == ["one", "two"]
    assert client.queues[f"{DEFAULT_QUEUE_NAME}:processing"] == []
    assert client.hashes.get(f"{DEFAULT_QUEUE_NAME}:claims", {}) == {}
    assert client.queues["batched:audit.log:processing"] == []
    assert client.hashes.get("batched:audit.log:claims", {}) == {}


def test_redis_listener_settles_batch_partial_failure_per_member_claim() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    serializer = JsonSerializer()
    registry = Registry()
    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
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

    assert client.queues["batched:audit.log:processing"] == []
    assert client.hashes.get("batched:audit.log:claims", {}) == {}
    assert transport.size("batched:audit.log") == 1
    assert transport.size(DEFAULT_FAILED_QUEUE_NAME) == 1


def test_redis_listener_acks_claim_for_undecodable_message() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    serializer = JsonSerializer()
    registry = Registry()
    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
    )
    transport.publish(DEFAULT_QUEUE_NAME, b"not-json")

    assert listener.listen_once(DEFAULT_QUEUE_NAME) is None

    assert client.queues[f"{DEFAULT_QUEUE_NAME}:processing"] == []
    assert client.hashes.get(f"{DEFAULT_QUEUE_NAME}:claims", {}) == {}
    assert transport.size(DEFAULT_FAILED_QUEUE_NAME) == 1


def test_redis_listener_leaves_claim_untouched_when_dead_letter_publish_is_indeterminate() -> (
    None
):
    client = FakeRedisClient()
    transport = RedisTransport(client=client)
    serializer = JsonSerializer()
    registry = Registry()
    listener = Listener(
        transport=transport,
        dispatcher=Dispatcher(registry=registry, serializer=serializer),
        serializer=serializer,
        retry_policy=RetryPolicy(max_retries=0),
    )

    def handle_event(message: EventMessage) -> None:
        raise RuntimeError("boom")

    registry.register("event", "billing.fail", handle_event)
    transport.publish(
        DEFAULT_QUEUE_NAME,
        serializer.dump(
            EventMessage(message_type="billing.fail", payload={"id": 1}).to_envelope(
                message_id="redis-indeterminate-1"
            )
        ),
    )
    client.lpush_error = ConnectionError("dead letter publish lost")

    with pytest.raises(IndeterminateDeliveryError, match="dead-letter publication"):
        listener.listen_once(DEFAULT_QUEUE_NAME)

    assert len(client.queues[f"{DEFAULT_QUEUE_NAME}:processing"]) == 1
    assert client.hashes.get(f"{DEFAULT_QUEUE_NAME}:claims", {}) != {}


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
