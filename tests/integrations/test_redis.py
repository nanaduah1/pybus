from __future__ import annotations

import base64
import pickle

import pytest

from pybus.dispatcher import Dispatcher
from pybus.envelope import MessageEnvelope
from pybus.handlers import batched_event_handler
from pybus.exceptions import DeserializationError
from pybus.integrations.redis import (
    RedisTransport,
    decode_json_redis_payload,
    decode_legacy_redis_payload,
    decode_trusted_legacy_redis_payload,
)
from pybus.serializer import JsonSerializer
from pybus.listener import DEFAULT_FAILED_QUEUE_NAME, DEFAULT_QUEUE_NAME, Listener
from pybus.messages import EventMessage
from pybus.registry import Registry


class FakeRedisClient:
    def __init__(self) -> None:
        self.queues: dict[str, list[bytes]] = {}

    def lpush(self, channel: str, message: bytes) -> None:
        self.queues.setdefault(channel, []).insert(0, message)

    def brpop(self, channel: str, timeout: int = 5):
        queue = self.queues.get(channel, [])
        if not queue:
            return None
        return channel, queue.pop()

    def llen(self, channel: str) -> int:
        return len(self.queues.get(channel, []))

    def eval(self, script: str, key_count: int, channel: str, limit: int):
        del script, key_count
        queue = self.queues.get(channel, [])
        claimed = queue[:limit]
        self.queues[channel] = queue[limit:]
        return claimed


def test_redis_transport_publish_and_consume() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)

    transport.publish("pybus.jobs", b"payload")

    assert transport.consume("pybus.jobs") == b"payload"
    assert transport.consume("pybus.jobs") is None


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
