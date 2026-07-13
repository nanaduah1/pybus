from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
import time

from pybus.batching import batched_buffer_key
from pybus.dispatcher import Dispatcher
from pybus.envelope import MessageEnvelope
from pybus.exceptions import HandlerNotFoundError
from pybus.handlers import ContinueProcessing, HandlerSpec, _handler_spec
from pybus.messages import RequestMessage, ResponseMessage
from pybus.queues import (
    DEFAULT_FAILED_QUEUE_NAME as _DEFAULT_FAILED_QUEUE_NAME,
    DEFAULT_QUEUE_NAME as _DEFAULT_QUEUE_NAME,
    DEFAULT_SLOW_QUEUE_NAME as _DEFAULT_SLOW_QUEUE_NAME,
)
from pybus.request_response import DEFAULT_REPLY_QUEUE
from pybus.retries import RetryPolicy, next_retry_payload
from pybus.serializer import JsonSerializer

DEFAULT_QUEUE_NAME = _DEFAULT_QUEUE_NAME
DEFAULT_SLOW_QUEUE_NAME = _DEFAULT_SLOW_QUEUE_NAME
DEFAULT_FAILED_QUEUE_NAME = _DEFAULT_FAILED_QUEUE_NAME
MULTI_QUEUE_CONSUME_TIMEOUT = 1


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
        self._batch_started_at: dict[str, datetime] = {}
        self._batch_retry_after: dict[str, datetime] = {}

    def listen_once(self, channel: str | Sequence[str]) -> object | None:
        self._flush_ready_batches()
        channel_name, envelope = self._consume_envelope(channel)
        if envelope is None:
            return None

        try:
            result = self._dispatch(channel_name, envelope)
        except HandlerNotFoundError:
            self._dead_letter(channel_name, envelope)
            return None
        except Exception:
            if self._should_retry(envelope):
                self._requeue(channel_name, envelope)
            else:
                self._dead_letter(channel_name, envelope)
            return None

        if envelope.message_kind == RequestMessage.message_kind and isinstance(
            result, ResponseMessage
        ):
            self._publish_response(envelope, result)
        return result

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
        consume_timeout = 5 if len(channels) == 1 else MULTI_QUEUE_CONSUME_TIMEOUT
        for channel_name in channels:
            raw_message = self.transport.consume(channel_name, timeout=consume_timeout)
            if raw_message is None:
                continue
            envelope = MessageEnvelope.from_dict(self.serializer.loads(raw_message))
            return channel_name, envelope
        return None, None

    def _dispatch(self, channel_name: str, envelope: MessageEnvelope) -> object | None:
        message = self.dispatcher.decode(envelope)
        handlers = self.dispatcher.registry.handlers_for(
            message.message_kind,
            message.message_type,
        )
        if not handlers:
            raise HandlerNotFoundError(
                f"No handlers registered for {message.message_kind}:{message.message_type}"
            )

        if self._has_batched_handlers(handlers):
            self._buffer_batched_message(message.message_type, envelope)
            self._flush_ready_batches()
            return None

        results: list[object] = []
        for handler in handlers:
            spec = self._spec_for(handler)
            if self._should_delay(envelope, spec):
                time.sleep(spec.delay)

            try:
                result = handler(message)
            except Exception:
                self._handle_failed_delivery(channel_name, envelope, spec)
                return None

            if isinstance(result, ContinueProcessing):
                self._continue_processing(channel_name, envelope, result)
                results.append(None)
                continue

            results.append(result)

        return results[0] if len(results) == 1 else results

    def _has_batched_handlers(self, handlers: Sequence[object]) -> bool:
        return any(self._spec_for(handler).is_batched for handler in handlers)

    def _spec_for(self, handler: object) -> HandlerSpec:
        spec = _handler_spec(handler)
        if spec is None:
            return HandlerSpec(message_kind="", message_type="")
        return spec

    def _should_delay(self, envelope: MessageEnvelope, spec: HandlerSpec) -> bool:
        if spec.delay <= 0:
            return False

        last_attempt_raw = None
        if isinstance(envelope.payload, dict):
            last_attempt_raw = envelope.payload.get("last_attempt")
        if last_attempt_raw is None:
            last_attempt_raw = envelope.headers.get("last_attempt")
        if not isinstance(last_attempt_raw, str):
            return False

        try:
            last_attempt = datetime.fromisoformat(last_attempt_raw)
        except ValueError:
            return False

        return (datetime.now(timezone.utc) - last_attempt).total_seconds() < spec.delay

    def _handle_failed_delivery(
        self,
        channel_name: str,
        envelope: MessageEnvelope,
        spec: HandlerSpec,
    ) -> None:
        max_retries = (
            self.retry_policy.max_retries
            if spec.retry_limit is None
            else spec.retry_limit
        )
        if self._retry_count(envelope) < max_retries:
            if spec.delay > 0:
                time.sleep(spec.delay)
            self._requeue(channel_name, envelope)
            return
        self._dead_letter(channel_name, envelope)

    def _continue_processing(
        self,
        channel_name: str,
        envelope: MessageEnvelope,
        continuation: ContinueProcessing,
    ) -> None:
        target_queue = continuation.queue or channel_name
        self.transport.publish(target_queue, self.serializer.dump(envelope))

    def _flush_ready_batches(self) -> None:
        if not hasattr(self.transport, "size") or not hasattr(
            self.transport, "consume_many"
        ):
            return

        registry = self.dispatcher.registry
        for (message_kind, message_type), handlers in registry.items():
            if message_kind != "event":
                continue
            for handler in handlers:
                spec = self._spec_for(handler)
                if not spec.is_batched:
                    continue
                if self._batch_in_backoff(message_type):
                    continue
                buffer_key = batched_buffer_key(message_type)
                buffer_size = self.transport.size(buffer_key)
                if buffer_size == 0:
                    self._batch_started_at.pop(message_type, None)
                    continue
                started_at = self._batch_started_at.setdefault(
                    message_type,
                    datetime.now(timezone.utc),
                )
                ready = (
                    buffer_size >= spec.batch_size
                    or (datetime.now(timezone.utc) - started_at).total_seconds()
                    >= spec.max_wait
                )
                if ready:
                    self._flush_batch(message_type, handler, spec)

    def _batch_in_backoff(self, message_type: str) -> bool:
        retry_at = self._batch_retry_after.get(message_type)
        return retry_at is not None and datetime.now(timezone.utc) < retry_at

    def _buffer_batched_message(
        self,
        message_type: str,
        envelope: MessageEnvelope,
    ) -> None:
        if not hasattr(self.transport, "size"):
            return
        buffer_key = batched_buffer_key(message_type)
        if self.transport.size(buffer_key) == 0:
            self._batch_started_at[message_type] = datetime.now(timezone.utc)
        self.transport.publish(buffer_key, self.serializer.dump(envelope))

    def _flush_batch(
        self,
        message_type: str,
        handler: object,
        spec: HandlerSpec,
    ) -> None:
        buffer_key = batched_buffer_key(message_type)
        raw_messages = self.transport.consume_many(buffer_key, spec.batch_size)
        if not raw_messages:
            return

        envelopes = [
            MessageEnvelope.from_dict(self.serializer.loads(raw_message))
            for raw_message in raw_messages
        ]
        events = [self.dispatcher.decode(envelope) for envelope in envelopes]
        try:
            handler(events)
        except Exception:
            requeued = False
            for envelope in envelopes:
                max_retries = (
                    self.retry_policy.max_retries
                    if spec.retry_limit is None
                    else spec.retry_limit
                )
                if self._retry_count(envelope) < max_retries:
                    self._requeue(buffer_key, envelope)
                    requeued = True
                else:
                    self._dead_letter(buffer_key, envelope)
            if requeued:
                self._batch_retry_after[message_type] = datetime.now(
                    timezone.utc
                ) + timedelta(seconds=spec.max_wait)
            else:
                self._batch_retry_after.pop(message_type, None)
            return

        self._batch_started_at.pop(message_type, None)
        self._batch_retry_after.pop(message_type, None)

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

    def _publish_response(
        self,
        request_envelope: MessageEnvelope,
        response: ResponseMessage,
    ) -> None:
        reply_queue = request_envelope.reply_to or DEFAULT_REPLY_QUEUE
        codec = self.dispatcher.payload_codec
        encoded_headers = codec.encode(response.headers)
        response_envelope = MessageEnvelope.create(
            message_type=response.message_type,
            message_kind=response.message_kind,
            version=response.version,
            payload=codec.encode(response.payload, context=response.headers),
            headers=encoded_headers,
            created_at=datetime.now(timezone.utc),
            correlation_id=request_envelope.correlation_id,
            causation_id=request_envelope.message_id,
            reply_to=reply_queue,
            expires_at=response.expires_at,
            content_type=response.content_type,
            content_encoding=response.content_encoding,
        )
        self.transport.publish(reply_queue, self.serializer.dump(response_envelope))
