from __future__ import annotations

import pickle

import pytest

from pybus.integrations.redis import RedisTransport, decode_legacy_redis_payload
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


@pytest.mark.parametrize(
    "payload,expected",
    [
        (JsonSerializer().dump({"hello": "world"}), {"hello": "world"}),
        (pickle.dumps({"legacy": True}), {"legacy": True}),
    ],
)
def test_decode_legacy_redis_payload_handles_json_and_pickle(
    payload: bytes, expected: dict[str, object]
) -> None:
    assert decode_legacy_redis_payload(payload) == expected
