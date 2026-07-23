from __future__ import annotations

import base64
import json
import logging
import math
import pickle
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from importlib import import_module
from threading import Event
from typing import Any
from uuid import uuid4

from pybus.envelope import MessageEnvelope
from pybus.exceptions import DeserializationError, IndeterminateDeliveryError
from pybus.queues import DEFAULT_FAILED_QUEUE_NAME
from pybus.retries import resolve_retry_count
from pybus.serializer import JsonSerializer
from pybus.worker import Worker, WorkerHook


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
    duplicate payloads are queued. A per-channel claims index (a sorted set,
    `<channel>:claims:index`, scored by claim time) lets a reaper find stale
    claims via `stale_claims()` without scanning claim payloads.

    Delivery guarantee: crash-safe at-least-once once a claim is durably
    indexed. A claim that outlives its worker is not lost — `RedisReaperRunner`
    (via `create_redis_reaper_worker`) finds it through `stale_claims()` and
    redelivers or dead-letters it. The move into the processing list and the
    claim-index write are two separate steps; a crash (or a raised exception
    from the index write) between them leaves the payload in the processing
    list with no index entry, which `stale_claims()` cannot discover — a known,
    tracked gap (see https://github.com/nanaduah1/pybus/issues/57), not a case
    this guarantee currently covers.
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
        claimed_at_dt = datetime.now(timezone.utc)
        try:
            pipeline = self._client.pipeline(transaction=True)
            pipeline.hset(
                self._claims_key(channel),
                mapping={
                    self._payload_field(claim_id): payload,
                    self._claimed_at_field(claim_id): claimed_at_dt.isoformat(),
                },
            )
            pipeline.zadd(
                self._claims_index_key(channel),
                {claim_id: claimed_at_dt.timestamp()},
            )
            pipeline.execute()
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
            pipeline = self._client.pipeline(transaction=True)
            pipeline.lrem(self._processing_key(channel), 1, payload)
            pipeline.hdel(
                self._claims_key(channel),
                self._payload_field(claim_id),
                self._claimed_at_field(claim_id),
            )
            pipeline.zrem(self._claims_index_key(channel), claim_id)
            pipeline.execute()
        except Exception as exc:
            raise IndeterminateDeliveryError(
                f"Redis claim settlement failed with an unknown outcome for {channel!r}"
            ) from exc

    def stale_claims(
        self, channel: str, *, older_than: datetime, limit: int = 100
    ) -> list[ReceiptedMessage]:
        """Return up to `limit` claimed messages whose claim predates `older_than`.

        Read-only: nothing is moved, requeued, or removed. The caller (a reaper)
        decides the outcome of each stale claim and settles it via `ack`/`publish`.
        """
        claim_ids = self._client.zrangebyscore(
            self._claims_index_key(channel),
            "-inf",
            older_than.timestamp(),
            start=0,
            num=limit,
        )
        stale: list[ReceiptedMessage] = []
        for raw_claim_id in claim_ids:
            claim_id = (
                raw_claim_id.decode("utf-8")
                if isinstance(raw_claim_id, bytes)
                else raw_claim_id
            )
            payload = self._client.hget(
                self._claims_key(channel), self._payload_field(claim_id)
            )
            if payload is None:
                # Orphaned index entry — e.g. a concurrent ack/nack settled this
                # claim (removing hash + index together) between our zrangebyscore
                # read and this hget. Self-heal so it isn't rescanned forever.
                self._client.zrem(self._claims_index_key(channel), claim_id)
                continue
            receipt = json.dumps(
                {"channel": channel, "claim_id": claim_id}, separators=(",", ":")
            )
            stale.append(ReceiptedMessage(payload, receipt))
        return stale

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
    def _claims_index_key(channel: str) -> str:
        return f"{channel}:claims:index"

    @staticmethod
    def _payload_field(claim_id: str) -> str:
        return f"{claim_id}:payload"

    @staticmethod
    def _claimed_at_field(claim_id: str) -> str:
        return f"{claim_id}:claimed_at"


@dataclass(frozen=True)
class RedisReaperPolicy:
    """Bounds for reclaiming stale Redis claims.

    Not threaded through `BusConfiguration`/`Pybus` — both are transport-agnostic
    and must stay import-safe without `redis`, so Redis-reaper wiring lives
    entirely in this integration module instead.
    """

    visibility_timeout: timedelta = timedelta(minutes=5)
    max_reclaim_attempts: int = 5

    def __post_init__(self) -> None:
        if not isinstance(self.visibility_timeout, timedelta) or (
            self.visibility_timeout <= timedelta(0)
        ):
            raise ValueError("visibility_timeout must be a positive timedelta")
        if (
            isinstance(self.max_reclaim_attempts, bool)
            or not isinstance(self.max_reclaim_attempts, int)
            or self.max_reclaim_attempts < 0
        ):
            raise ValueError("max_reclaim_attempts must be a non-negative integer")


class RedisReaperRunner:
    """Reclaims stale Redis claims: redelivers them, or dead-letters them once
    reclaimed too many times, using the same `retries`/`last_attempt` header
    convention `Listener` uses so poison messages still terminate through the
    existing dead-letter path."""

    def __init__(
        self,
        transport: RedisTransport,
        channels: Sequence[str],
        *,
        dead_letter_channel: str = DEFAULT_FAILED_QUEUE_NAME,
        policy: RedisReaperPolicy | None = None,
        serializer: JsonSerializer | None = None,
        now_fn: Callable[[], datetime] | None = None,
        batch_limit: int = 100,
    ) -> None:
        channels = tuple(channels)
        if not channels:
            raise ValueError("reaper requires at least one channel")
        if dead_letter_channel in channels:
            raise ValueError("dead-letter channel is terminal and cannot be reaped")
        self.transport = transport
        self.channels = channels
        self.dead_letter_channel = dead_letter_channel
        self.policy = policy or RedisReaperPolicy()
        self.serializer = serializer or JsonSerializer()
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))
        self.batch_limit = batch_limit
        self._logger = logging.getLogger("pybus.redis_reaper")

    def run_once(self) -> int:
        cutoff = self._now_fn() - self.policy.visibility_timeout
        reclaimed = 0
        for channel in self.channels:
            stale = self.transport.stale_claims(
                channel, older_than=cutoff, limit=self.batch_limit
            )
            for message in stale:
                self._reclaim(channel, message)
                reclaimed += 1
        return reclaimed

    def _reclaim(self, channel: str, message: ReceiptedMessage) -> None:
        try:
            envelope = MessageEnvelope.from_dict(self.serializer.loads(message))
        except Exception as exc:
            self._logger.warning(
                "Stale claim on %r could not be decoded: %s", channel, exc
            )
            self._dead_letter_undecodable(channel, message)
            return

        # No dispatcher/registry is available here, so unlike `Listener._retry_count`
        # this never reads `envelope.payload` — a domain payload could legitimately
        # have its own field named "retries" unrelated to framework bookkeeping.
        retries = resolve_retry_count(None, envelope.headers)
        headers = dict(envelope.headers)
        now = self._now_fn()
        if retries >= self.policy.max_reclaim_attempts:
            headers["dead_lettered_from"] = channel
            target = self.dead_letter_channel
        else:
            headers["retries"] = retries + 1
            headers["last_attempt"] = now.isoformat()
            target = channel

        self._settle_after_claim(
            "stale-claim reclaim",
            lambda: self.transport.publish(
                target,
                self.serializer.dump(
                    MessageEnvelope.create(
                        message_id=envelope.message_id,
                        message_type=envelope.message_type,
                        message_kind=envelope.message_kind,
                        version=envelope.version,
                        payload=envelope.payload,
                        headers=headers,
                        created_at=envelope.created_at,
                        correlation_id=envelope.correlation_id,
                        causation_id=envelope.causation_id,
                        reply_to=envelope.reply_to,
                        expires_at=envelope.expires_at,
                        content_type=envelope.content_type,
                        content_encoding=envelope.content_encoding,
                    )
                ),
            ),
        )
        self._ack_claim(message.receipt)

    def _dead_letter_undecodable(self, channel: str, message: ReceiptedMessage) -> None:
        raw_bytes = bytes(message)
        poison = MessageEnvelope.create(
            message_type="pybus.message.decode_failed",
            message_kind="event",
            version=1,
            payload={
                "raw_message": base64.b64encode(raw_bytes).decode("ascii"),
                "raw_message_encoding": "base64",
                "error_type": "ReaperDecodeError",
            },
            headers={"dead_lettered_from": channel},
        )
        self._settle_after_claim(
            "stale-claim decode-failure publication",
            lambda: self.transport.publish(
                self.dead_letter_channel, self.serializer.dump(poison)
            ),
        )
        self._ack_claim(message.receipt)

    def _settle_after_claim(self, action: str, operation: Callable[[], None]) -> None:
        try:
            operation()
        except IndeterminateDeliveryError:
            raise
        except Exception as exc:
            raise IndeterminateDeliveryError(
                f"{action} failed after a stale claim was reclaimed from the transport"
            ) from exc

    def _ack_claim(self, receipt: str) -> None:
        try:
            self.transport.ack(receipt)
        except IndeterminateDeliveryError:
            raise
        except Exception as exc:
            raise IndeterminateDeliveryError(
                "stale claim ack failed after a reclaim outcome was durably published"
            ) from exc


class RedisReaperPoller:
    """Adapt a `RedisReaperRunner` to the regular `Worker` lifecycle."""

    dead_letter_channel = "__pybus_redis_reaper_terminal__"

    def __init__(
        self,
        runner: RedisReaperRunner,
        *,
        idle_delay: float = 1.0,
        stop_event: Event | None = None,
    ) -> None:
        if (
            isinstance(idle_delay, bool)
            or not isinstance(idle_delay, (int, float))
            or not math.isfinite(idle_delay)
            or idle_delay < 0
        ):
            raise ValueError("idle_delay must be a non-negative finite number")
        self.runner = runner
        self.idle_delay = float(idle_delay)
        self.stop_event = stop_event or Event()

    def listen_once(self, channel: str | tuple[str, ...]) -> object | None:
        reclaimed = self.runner.run_once()
        if reclaimed == 0:
            self.stop_event.wait(self.idle_delay)
            return None
        return reclaimed


def create_redis_reaper_worker(
    transport: RedisTransport,
    channels: Sequence[str],
    *,
    dead_letter_channel: str = DEFAULT_FAILED_QUEUE_NAME,
    policy: RedisReaperPolicy | None = None,
    serializer: JsonSerializer | None = None,
    now_fn: Callable[[], datetime] | None = None,
    batch_limit: int = 100,
    hooks: Sequence[WorkerHook] = (),
    error_delay: float = 1.0,
    idle_delay: float = 1.0,
    logger: logging.Logger | None = None,
    stop_event: Event | None = None,
) -> Worker:
    """Build a `Worker` that periodically sweeps `channels` for stale Redis
    claims, following the same runner/poller-adapter pattern
    `Pybus.create_durable_job_worker` uses for durable-job polling."""

    resolved_stop_event = stop_event or Event()
    poller = RedisReaperPoller(
        RedisReaperRunner(
            transport,
            channels,
            dead_letter_channel=dead_letter_channel,
            policy=policy,
            serializer=serializer,
            now_fn=now_fn,
            batch_limit=batch_limit,
        ),
        idle_delay=idle_delay,
        stop_event=resolved_stop_event,
    )
    return Worker(
        poller,
        "__pybus_redis_reaper__",
        hooks=hooks,
        error_delay=error_delay,
        logger=logger,
        stop_event=resolved_stop_event,
    )


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
    "RedisReaperPolicy",
    "RedisReaperPoller",
    "RedisReaperRunner",
    "RedisScheduleStateStore",
    "RedisTransport",
    "create_redis_reaper_worker",
    "decode_json_redis_payload",
    "decode_legacy_redis_payload",
    "decode_trusted_legacy_redis_payload",
]
