from __future__ import annotations

import pickle
from importlib import import_module
from typing import Any

from pybus.exceptions import DeserializationError
from pybus.serializer import JsonSerializer


class RedisTransport:
    """Redis transport for queue-based publish/consume flows.

    The transport stays import-safe by loading the `redis` package only when a
    client needs to be created.
    """

    def __init__(
        self,
        *,
        client: Any | None = None,
        url: str | None = None,
        serializer: JsonSerializer | None = None,
    ) -> None:
        if client is None and url is None:
            raise ValueError("Either client or url must be provided")

        self._client = client or self._client_from_url(url)
        self.serializer = serializer or JsonSerializer()

    @staticmethod
    def _client_from_url(url: str | None) -> Any:
        if url is None:
            raise ValueError("A Redis url is required when client is not provided")

        try:
            redis = import_module("redis")
        except ModuleNotFoundError as exc:  # pragma: no cover - import safety
            raise RuntimeError(
                "redis is required to build a RedisTransport from a url"
            ) from exc
        return redis.StrictRedis.from_url(url)

    def publish(self, channel: str, message: bytes) -> None:
        self._client.lpush(channel, message)

    def consume(self, channel: str, timeout: int = 5) -> bytes | None:
        result = self._client.brpop(channel, timeout=timeout)
        if result is None:
            return None
        _, message = result
        return message

    def size(self, channel: str) -> int:
        return int(self._client.llen(channel))

    def consume_many(self, channel: str, limit: int) -> list[bytes]:
        lua_pop = """
        local res = redis.call('LRANGE', KEYS[1], 0, ARGV[1] - 1)
        redis.call('LTRIM', KEYS[1], ARGV[1], -1)
        return res
        """
        return list(self._client.eval(lua_pop, 1, channel, limit))

    def ack(self, receipt: str) -> None:
        """Ack is a no-op for the list-based transport."""
        return None

    def nack(self, receipt: str, *, requeue: bool = True) -> None:
        """Nack is a no-op for the list-based transport."""
        return None


def decode_legacy_redis_payload(payload: bytes | str) -> Any:
    """Decode a Redis payload that may still be JSON or legacy pickle."""

    try:
        return decode_json_redis_payload(payload)
    except DeserializationError:
        raw = payload.encode("utf-8") if isinstance(payload, str) else payload
        try:
            return pickle.loads(raw)
        except Exception as exc:  # pragma: no cover - defensive
            raise DeserializationError(
                "Payload is not valid JSON or legacy pickle"
            ) from exc


def decode_json_redis_payload(payload: bytes | str) -> Any:
    """Decode a Redis payload as JSON only."""

    serializer = JsonSerializer()
    return serializer.loads(payload)


def decode_trusted_legacy_redis_payload(payload: bytes | str) -> Any:
    """Decode trusted migration payloads that may be JSON or legacy pickle."""
    return decode_legacy_redis_payload(payload)


__all__ = [
    "RedisTransport",
    "decode_json_redis_payload",
    "decode_legacy_redis_payload",
    "decode_trusted_legacy_redis_payload",
]
