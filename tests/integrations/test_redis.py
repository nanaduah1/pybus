from __future__ import annotations

import pickle

import pytest

from pybus.exceptions import DeserializationError
from pybus.integrations.redis import (
    RedisTransport,
    decode_json_redis_payload,
    decode_legacy_redis_payload,
    decode_trusted_legacy_redis_payload,
)
from pybus.serializer import JsonSerializer


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


def test_redis_transport_publish_and_consume() -> None:
    client = FakeRedisClient()
    transport = RedisTransport(client=client)

    transport.publish("pybus.jobs", b"payload")

    assert transport.consume("pybus.jobs") == b"payload"
    assert transport.consume("pybus.jobs") is None


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
