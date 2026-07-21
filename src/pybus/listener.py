from __future__ import annotations

import time
from collections.abc import Sequence
from datetime import datetime, timezone

from pybus.dispatcher import Dispatcher
from pybus.envelope import MessageEnvelope
from pybus.exceptions import HandlerNotFoundError
from pybus.queues import (
    DEFAULT_FAILED_QUEUE_NAME as _DEFAULT_FAILED_QUEUE_NAME,
    DEFAULT_QUEUE_NAME as _DEFAULT_QUEUE_NAME,
    DEFAULT_SLOW_QUEUE_NAME as _DEFAULT_SLOW_QUEUE_NAME,
)
from pybus.retries import RetryPolicy, next_retry_payload
from pybus.serializer import JsonSerializer

DEFAULT_QUEUE_NAME = _DEFAULT_QUEUE_NAME
DEFAULT_SLOW_QUEUE_NAME = _DEFAULT_SLOW_QUEUE_NAME
DEFAULT_FAILED_QUEUE_NAME = _DEFAULT_FAILED_QUEUE_NAME


class Listener:
    def __init__(
        self,
        transport,
        dispatcher: Dispatcher | None = None,
        serializer: JsonSerializer | None = None,
        retry_policy: RetryPolicy | None = None,
        dead_letter_channel: str = DEFAULT_FAILED_QUEUE_NAME,
    ) -> None:
        self.transport = transport
        self.dispatcher = dispatcher or Dispatcher()
        self.serializer = serializer or JsonSerializer()
        self.retry_policy = retry_policy or RetryPolicy()
        self.dead_letter_channel = dead_letter_channel

    def listen_once(self, channel: str | Sequence[str]) -> object | None:
        channel_name, envelope = self._consume_envelope(channel)
        if envelope is None:
            return None

        try:
            return self.dispatcher.dispatch(envelope)
        except HandlerNotFoundError:
            self._dead_letter(channel_name, envelope)
        except Exception:
            if self._should_retry(envelope):
                self._requeue(channel_name, envelope)
            else:
                self._dead_letter(channel_name, envelope)
        return None

    def listen(
        self,
        channel: str | Sequence[str],
        *,
        max_messages: int | None = None,
    ) -> list[object]:
        results: list[object] = []
        count = 0
        while max_messages is None or count < max_messages:
            result = self.listen_once(channel)
            if result is None:
                break
            results.append(result)
            count += 1
        return results

    def _consume_envelope(
        self, channel: str | Sequence[str]
    ) -> tuple[str | None, MessageEnvelope | None]:
        channels = (channel,) if isinstance(channel, str) else tuple(channel)
        for channel_name in channels:
            raw_message = self.transport.consume(channel_name)
            if raw_message is None:
                continue
            envelope = MessageEnvelope.from_dict(self.serializer.loads(raw_message))
            return channel_name, envelope
        return None, None

    def _should_retry(self, envelope: MessageEnvelope) -> bool:
        return self._retry_count(envelope) < self.retry_policy.max_retries

    def _retry_count(self, envelope: MessageEnvelope) -> int:
        if isinstance(envelope.payload, dict):
            value = envelope.payload.get("retries", 0)
        else:
            value = envelope.headers.get("retries", 0)
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def _requeue(self, channel_name: str, envelope: MessageEnvelope) -> None:
        # Blocks the listener for the configured delay. On multi-channel listeners
        # this stalls all channels for the duration; keep delay values small.
        delay = self.retry_policy.next_delay(self._retry_count(envelope))
        if delay > 0:
            time.sleep(delay)
        now = datetime.now(timezone.utc)
        if isinstance(envelope.payload, dict):
            payload = next_retry_payload(envelope.payload, attempted_at=now)
        else:
            payload = envelope.payload
        headers = dict(envelope.headers)
        headers["retries"] = self._retry_count(envelope) + 1
        headers["last_attempt"] = now.isoformat()
        self.transport.publish(
            channel_name,
            self.serializer.dump(
                MessageEnvelope.create(
                    message_id=envelope.message_id,
                    message_type=envelope.message_type,
                    message_kind=envelope.message_kind,
                    version=envelope.version,
                    payload=payload,
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
        )

    def _dead_letter(self, channel_name: str, envelope: MessageEnvelope) -> None:
        headers = dict(envelope.headers)
        headers["dead_lettered_from"] = channel_name
        self.transport.publish(
            self.dead_letter_channel,
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
        )
