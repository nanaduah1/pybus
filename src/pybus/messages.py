from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar

from pybus.codecs import PayloadCodec, resolve_payload_codec
from pybus.envelope import MessageEnvelope
from pybus.exceptions import InvalidMessageDefinitionError


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
            raise InvalidMessageDefinitionError("message_type is required")
        if not isinstance(self.headers, dict):
            raise InvalidMessageDefinitionError("headers must be a dictionary")
        return self

    def to_dict(self, *, payload_codec: PayloadCodec | None = None) -> dict[str, Any]:
        self.validate()
        codec = resolve_payload_codec(payload_codec)
        encoded_headers = codec.encode(self.headers)
        return {
            "message_type": self.message_type,
            "message_kind": self.message_kind,
            "version": self.version,
            "payload": codec.encode(self.payload, context=self.headers),
            "headers": encoded_headers,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "reply_to": self.reply_to,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "content_type": self.content_type,
            "content_encoding": self.content_encoding,
        }

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        *,
        payload_codec: PayloadCodec | None = None,
    ) -> "BaseMessage":
        try:
            codec = resolve_payload_codec(payload_codec)
            message_kind = data.get("message_kind")
            if message_kind is not None and message_kind != cls.message_kind:
                raise InvalidMessageDefinitionError(
                    f"Expected message_kind={cls.message_kind!r}, got {message_kind!r}"
                )
            version = data.get("version")
            if version is not None and version != cls.version:
                raise InvalidMessageDefinitionError(
                    f"Expected version={cls.version!r}, got {version!r}"
                )
            expires_at = data.get("expires_at")
            if isinstance(expires_at, str):
                expires_at = datetime.fromisoformat(expires_at)
            headers = codec.decode(data.get("headers") or {})
            return cls(
                message_type=data["message_type"],
                payload=codec.decode(data.get("payload"), context=headers),
                headers=headers,
                correlation_id=data.get("correlation_id"),
                causation_id=data.get("causation_id"),
                reply_to=data.get("reply_to"),
                expires_at=expires_at,
                content_type=data.get("content_type"),
                content_encoding=data.get("content_encoding"),
            ).validate()
        except KeyError as exc:
            raise InvalidMessageDefinitionError(
                f"Missing message field: {exc.args[0]}"
            ) from exc

    def to_envelope(
        self,
        *,
        message_id: str | None = None,
        payload_codec: PayloadCodec | None = None,
    ) -> MessageEnvelope:
        codec = resolve_payload_codec(payload_codec)
        encoded_headers = codec.encode(self.headers)
        return MessageEnvelope.create(
            message_type=self.message_type,
            message_kind=self.message_kind,
            version=self.version,
            payload=codec.encode(self.payload, context=self.headers),
            headers=encoded_headers,
            correlation_id=self.correlation_id,
            causation_id=self.causation_id,
            reply_to=self.reply_to,
            expires_at=self.expires_at,
            content_type=self.content_type,
            content_encoding=self.content_encoding,
            message_id=message_id,
        )

    @classmethod
    def from_envelope(
        cls,
        envelope: MessageEnvelope,
        *,
        payload_codec: PayloadCodec | None = None,
    ) -> "BaseMessage":
        if envelope.message_kind != cls.message_kind:
            raise InvalidMessageDefinitionError(
                f"Expected message_kind={cls.message_kind!r}, got {envelope.message_kind!r}"
            )
        codec = resolve_payload_codec(payload_codec)
        headers = codec.decode(envelope.headers)
        return cls(
            message_type=envelope.message_type,
            payload=codec.decode(envelope.payload, context=headers),
            headers=headers,
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
        raise InvalidMessageDefinitionError(
            f"Unsupported message_kind: {message_kind!r}"
        ) from exc
