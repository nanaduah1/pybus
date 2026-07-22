from __future__ import annotations

import json
import pickle
from datetime import datetime, timezone
from importlib import import_module
from typing import Any
from uuid import uuid4

from pybus.exceptions import DeserializationError, IndeterminateDeliveryError
from pybus.serializer import JsonSerializer


def _client_from_url(url: str | None, *, component: str) -> Any:
    if url is None:
        raise ValueError("A Redis url is required when client is not provided")

    try:
        redis = import_module("redis")
    except ModuleNotFoundError as exc:  # pragma: no cover - import safety
        raise RuntimeError(
            f"redis is required to build a {component} from a url"
        ) from exc
    return redis.StrictRedis.from_url(url)


class RedisScheduleStateStore:
    """Durable scheduler state backed by the optional Redis integration."""

    def __init__(
        self,
        *,
        client: Any | None = None,
        url: str | None = None,
    ) -> None:
        if client is None and url is None:
            raise ValueError("Either client or url must be provided")
        self._client = (
            client
            if client is not None
            else _client_from_url(url, component="RedisScheduleStateStore")
        )

    def get(self, key: str) -> str | None:
        value = self._client.get(key)
        if value is None or isinstance(value, str):
            return value
        if isinstance(value, bytes):
            return value.decode("utf-8")
        raise TypeError("Redis scheduler state must be bytes, str, or None")

    def set(self, key: str, value: str) -> None:
        self._client.set(key, value)


class ReceiptedMessage(bytes):
    """A claimed payload carrying the receipt needed to ack/nack it.

    Behaves as plain `bytes` for every existing caller (e.g. `JsonSerializer.loads`
    uses `isinstance`, not a strict type check); the `receipt` attribute is only
    read by callers that claim/settle messages.
    """

    receipt: str

    def __new__(cls, payload: bytes, receipt: str) -> "ReceiptedMessage":
        instance = super().__new__(cls, payload)
        instance.receipt = receipt
        return instance


class RedisTransport:
    """Redis transport for queue-based publish/consume flows.

    The transport stays import-safe by loading the `redis` package only when a
    client needs to be created.

    Claimed messages are moved into a per-channel processing list
    (`<channel>:processing`) instead of being destroyed on pop, so a worker crash
    between claim and `ack`/`nack` leaves the message recoverable. A per-channel
    claims hash (`<channel>:claims`) tracks which processing-list entry belongs to
    which claim, so `ack`/`nack` can settle exactly one claimed entry even when
    duplicate payloads are queued.
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

        self._client = (
            client
            if client is not None
            else _client_from_url(url, component="RedisTransport")
        )
        self.serializer = serializer or JsonSerializer()

    def publish(self, channel: str, message: bytes) -> None:
        self._client.lpush(channel, message)

    def consume(self, channel: str, timeout: int = 5) -> ReceiptedMessage | None:
        processing_key = self._processing_key(channel)
        try:
            payload = self._client.blmove(
                channel, processing_key, timeout, src="RIGHT", dest="LEFT"
            )
        except Exception as exc:
            raise IndeterminateDeliveryError(
                f"Redis consume failed with an unknown claim outcome for {channel!r}"
            ) from exc
        if payload is None:
            return None
        return self._claim(channel, payload)

    def size(self, channel: str) -> int:
        return int(self._client.llen(channel))

    def consume_many(self, channel: str, limit: int) -> list[ReceiptedMessage]:
        processing_key = self._processing_key(channel)
        claimed: list[ReceiptedMessage] = []
        for _ in range(limit):
            try:
                payload = self._client.lmove(channel, processing_key, "RIGHT", "LEFT")
            except Exception as exc:
                raise IndeterminateDeliveryError(
                    "Redis batch consume failed with an unknown claim outcome "
                    f"for {channel!r}"
                ) from exc
            if payload is None:
                break
            claimed.append(self._claim(channel, payload))
        return claimed

    def ack(self, receipt: str) -> None:
        """Remove the claimed entry; a no-op if the receipt is unknown or the
        claim was already settled by a prior ack/nack."""
        claim = self._lookup_claim(receipt)
        if claim is None:
            return None
        channel, claim_id, payload = claim
        self._settle_claim(channel, claim_id, payload)
        return None

    def nack(self, receipt: str, *, requeue: bool = True) -> None:
        """Settle the claim; if `requeue`, put the payload back on the source
        list first so a crash mid-settle risks a duplicate delivery, never a
        loss. A no-op if the receipt is unknown or already settled."""
        claim = self._lookup_claim(receipt)
        if claim is None:
            return None
        channel, claim_id, payload = claim
        if requeue:
            try:
                self._client.lpush(channel, payload)
            except Exception as exc:
                raise IndeterminateDeliveryError(
                    f"Redis nack requeue failed with an unknown outcome for {channel!r}"
                ) from exc
        self._settle_claim(channel, claim_id, payload)
        return None

    def _claim(self, channel: str, payload: bytes) -> ReceiptedMessage:
        claim_id = uuid4().hex
        # claimed_at has no reader yet; it's for epic #44's slice #50 (stale-claim
        # reaper) to age out abandoned claims without a schema change.
        claimed_at = datetime.now(timezone.utc).isoformat()
        try:
            self._client.hset(
                self._claims_key(channel),
                mapping={
                    self._payload_field(claim_id): payload,
                    self._claimed_at_field(claim_id): claimed_at,
                },
            )
        except Exception as exc:
            raise IndeterminateDeliveryError(
                f"Redis claim record failed with an unknown outcome for {channel!r}"
            ) from exc
        receipt = json.dumps(
            {"channel": channel, "claim_id": claim_id}, separators=(",", ":")
        )
        return ReceiptedMessage(payload, receipt)

    def _lookup_claim(self, receipt: str) -> tuple[str, str, bytes] | None:
        # Not atomic against a second concurrent ack/nack on the same receipt —
        # both could pass this lookup before either settles. Accepted per the
        # epic's duplicate-over-loss trade-off, same as the sequential crash case.
        parsed = self._parse_receipt(receipt)
        if parsed is None:
            return None
        channel, claim_id = parsed
        payload = self._client.hget(
            self._claims_key(channel), self._payload_field(claim_id)
        )
        if payload is None:
            return None
        return channel, claim_id, payload

    def _settle_claim(self, channel: str, claim_id: str, payload: bytes) -> None:
        try:
            self._client.lrem(self._processing_key(channel), 1, payload)
        except Exception as exc:
            raise IndeterminateDeliveryError(
                f"Redis claim settlement failed with an unknown outcome for {channel!r}"
            ) from exc
        try:
            self._client.hdel(
                self._claims_key(channel),
                self._payload_field(claim_id),
                self._claimed_at_field(claim_id),
            )
        except Exception as exc:
            raise IndeterminateDeliveryError(
                "Redis claim record cleanup failed with an unknown outcome "
                f"for {channel!r}"
            ) from exc

    @staticmethod
    def _parse_receipt(receipt: str) -> tuple[str, str] | None:
        try:
            data = json.loads(receipt)
        except (TypeError, ValueError):
            return None
        if not isinstance(data, dict):
            return None
        channel = data.get("channel")
        claim_id = data.get("claim_id")
        if not isinstance(channel, str) or not isinstance(claim_id, str):
            return None
        return channel, claim_id

    @staticmethod
    def _processing_key(channel: str) -> str:
        return f"{channel}:processing"

    @staticmethod
    def _claims_key(channel: str) -> str:
        return f"{channel}:claims"

    @staticmethod
    def _payload_field(claim_id: str) -> str:
        return f"{claim_id}:payload"

    @staticmethod
    def _claimed_at_field(claim_id: str) -> str:
        return f"{claim_id}:claimed_at"


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
    "RedisScheduleStateStore",
    "RedisTransport",
    "decode_json_redis_payload",
    "decode_legacy_redis_payload",
    "decode_trusted_legacy_redis_payload",
]
