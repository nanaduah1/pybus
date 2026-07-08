from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from pybus.envelope import MessageEnvelope
from pybus.exceptions import MessageTimeoutError
from pybus.messages import RequestMessage, ResponseMessage

DEFAULT_REPLY_QUEUE = "pybus.responses"


@dataclass(frozen=True)
class RequestTicket:
    correlation_id: str
    request_id: str
    reply_to: str
    expires_at: datetime | None


class RequestResponseCoordinator:
    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._responses: dict[str, MessageEnvelope] = {}

    def prepare_request(
        self,
        request: RequestMessage,
        *,
        timeout: int | None = 5,
        message_id: str | None = None,
        reply_to: str = DEFAULT_REPLY_QUEUE,
        created_at: datetime | None = None,
    ) -> tuple[MessageEnvelope, RequestTicket]:
        created_at = created_at or datetime.now(timezone.utc)
        request_id = message_id or str(uuid4())
        correlation_id = request.correlation_id or str(uuid4())
        expires_at = (
            created_at + timedelta(seconds=timeout) if timeout is not None else None
        )
        envelope = MessageEnvelope.create(
            message_type=request.message_type,
            message_kind=request.message_kind,
            version=request.version,
            payload=request.payload,
            headers=request.headers,
            correlation_id=correlation_id,
            causation_id=request.causation_id,
            reply_to=reply_to,
            expires_at=expires_at,
            content_type=request.content_type,
            content_encoding=request.content_encoding,
            message_id=request_id,
            created_at=created_at,
        )
        ticket = RequestTicket(
            correlation_id=correlation_id,
            request_id=request_id,
            reply_to=reply_to,
            expires_at=expires_at,
        )
        return envelope, ticket

    def prepare_response(
        self,
        request_envelope: MessageEnvelope,
        response: ResponseMessage,
        *,
        message_id: str | None = None,
        created_at: datetime | None = None,
    ) -> MessageEnvelope:
        created_at = created_at or datetime.now(timezone.utc)
        return MessageEnvelope.create(
            message_type=response.message_type,
            message_kind=response.message_kind,
            version=response.version,
            payload=response.payload,
            headers=response.headers,
            correlation_id=request_envelope.correlation_id,
            causation_id=request_envelope.message_id,
            reply_to=request_envelope.reply_to,
            expires_at=response.expires_at,
            content_type=response.content_type,
            content_encoding=response.content_encoding,
            message_id=message_id,
            created_at=created_at,
        )

    def store_response(self, response_envelope: MessageEnvelope) -> None:
        if not response_envelope.correlation_id:
            raise ValueError("response envelope must include a correlation_id")
        with self._condition:
            self._responses[response_envelope.correlation_id] = response_envelope
            self._condition.notify_all()

    def wait_for_response(
        self,
        correlation_id: str,
        *,
        timeout: int | None = 5,
    ) -> MessageEnvelope:
        deadline = None
        if timeout is not None:
            deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout)

        with self._condition:
            while correlation_id not in self._responses:
                if deadline is None:
                    self._condition.wait()
                    continue

                remaining = (deadline - datetime.now(timezone.utc)).total_seconds()
                if remaining <= 0:
                    raise MessageTimeoutError(
                        f"Timed out waiting for response {correlation_id}"
                    )
                self._condition.wait(timeout=remaining)

            return self._responses.pop(correlation_id)
