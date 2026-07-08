from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar

from pybus._codec import decode_value, encode_value
from pybus.envelope import MessageEnvelope


@dataclass
class BaseMessage:
    message_type: str
    payload: Any
    headers: dict[str, Any] = field(default_factory=dict)
    correlation_id: str | None = None
    causation_id: str | None = None
    reply_to: str | None = None
    expires_at: datetime | None = None
    content_type: str | None = None
    content_encoding: str | None = None

    message_kind: ClassVar[str] = "message"
    version: ClassVar[int] = 1

    def validate(self) -> "BaseMessage":
        if not self.message_type:
            raise ValueError("message_type is required")
        if not isinstance(self.headers, dict):
            raise TypeError("headers must be a dictionary")
        return self

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "message_type": self.message_type,
            "message_kind": self.message_kind,
            "version": self.version,
            "payload": encode_value(self.payload),
            "headers": encode_value(self.headers),
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "reply_to": self.reply_to,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "content_type": self.content_type,
            "content_encoding": self.content_encoding,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BaseMessage":
        expires_at = data.get("expires_at")
        if isinstance(expires_at, str):
            expires_at = datetime.fromisoformat(expires_at)
        return cls(
            message_type=data["message_type"],
            payload=decode_value(data.get("payload")),
            headers=decode_value(data.get("headers") or {}),
            correlation_id=data.get("correlation_id"),
            causation_id=data.get("causation_id"),
            reply_to=data.get("reply_to"),
            expires_at=expires_at,
            content_type=data.get("content_type"),
            content_encoding=data.get("content_encoding"),
        ).validate()

    def to_envelope(self, *, message_id: str | None = None) -> MessageEnvelope:
        return MessageEnvelope.create(
            message_type=self.message_type,
            message_kind=self.message_kind,
            version=self.version,
            payload=encode_value(self.payload),
            headers=encode_value(self.headers),
            correlation_id=self.correlation_id,
            causation_id=self.causation_id,
            reply_to=self.reply_to,
            expires_at=self.expires_at,
            content_type=self.content_type,
            content_encoding=self.content_encoding,
            message_id=message_id,
        )

    @classmethod
    def from_envelope(cls, envelope: MessageEnvelope) -> "BaseMessage":
        if envelope.message_kind != cls.message_kind:
            raise ValueError(
                f"Expected message_kind={cls.message_kind!r}, got {envelope.message_kind!r}"
            )
        return cls(
            message_type=envelope.message_type,
            payload=decode_value(envelope.payload),
            headers=decode_value(envelope.headers),
            correlation_id=envelope.correlation_id,
            causation_id=envelope.causation_id,
            reply_to=envelope.reply_to,
            expires_at=envelope.expires_at,
            content_type=envelope.content_type,
            content_encoding=envelope.content_encoding,
        ).validate()


@dataclass
class EventMessage(BaseMessage):
    message_kind: ClassVar[str] = "event"


@dataclass
class CommandMessage(BaseMessage):
    message_kind: ClassVar[str] = "command"


@dataclass
class RequestMessage(BaseMessage):
    message_kind: ClassVar[str] = "request"


@dataclass
class ResponseMessage(BaseMessage):
    message_kind: ClassVar[str] = "response"


MESSAGE_KIND_TO_CLASS: dict[str, type[BaseMessage]] = {
    EventMessage.message_kind: EventMessage,
    CommandMessage.message_kind: CommandMessage,
    RequestMessage.message_kind: RequestMessage,
    ResponseMessage.message_kind: ResponseMessage,
}


def message_class_for_kind(message_kind: str) -> type[BaseMessage]:
    try:
        return MESSAGE_KIND_TO_CLASS[message_kind]
    except KeyError as exc:
        raise ValueError(f"Unsupported message_kind: {message_kind!r}") from exc
