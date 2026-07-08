from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pybus._codec import decode_value, encode_value
from pybus.exceptions import InvalidMessageDefinitionError


@dataclass
class MessageEnvelope:
    message_id: str
    message_type: str
    message_kind: str
    version: int
    payload: Any
    headers: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    correlation_id: str | None = None
    causation_id: str | None = None
    reply_to: str | None = None
    expires_at: datetime | None = None
    content_type: str | None = None
    content_encoding: str | None = None

    @classmethod
    def create(
        cls,
        *,
        message_type: str,
        message_kind: str,
        version: int,
        payload: Any,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        reply_to: str | None = None,
        expires_at: datetime | None = None,
        content_type: str | None = None,
        content_encoding: str | None = None,
        message_id: str | None = None,
        created_at: datetime | None = None,
    ) -> "MessageEnvelope":
        return cls(
            message_id=message_id or str(uuid4()),
            message_type=message_type,
            message_kind=message_kind,
            version=version,
            payload=payload,
            headers=headers or {},
            created_at=created_at or datetime.now(timezone.utc),
            correlation_id=correlation_id,
            causation_id=causation_id,
            reply_to=reply_to,
            expires_at=expires_at,
            content_type=content_type,
            content_encoding=content_encoding,
        )

    def validate(self) -> "MessageEnvelope":
        if not self.message_id:
            raise InvalidMessageDefinitionError("message_id is required")
        if not self.message_type:
            raise InvalidMessageDefinitionError("message_type is required")
        if not self.message_kind:
            raise InvalidMessageDefinitionError("message_kind is required")
        if self.version is None:
            raise InvalidMessageDefinitionError("version is required")
        if not isinstance(self.headers, dict):
            raise InvalidMessageDefinitionError("headers must be a dictionary")
        return self

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "message_id": self.message_id,
            "message_type": self.message_type,
            "message_kind": self.message_kind,
            "version": self.version,
            "payload": encode_value(self.payload),
            "headers": encode_value(self.headers),
            "created_at": self.created_at.isoformat(),
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "reply_to": self.reply_to,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "content_type": self.content_type,
            "content_encoding": self.content_encoding,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MessageEnvelope":
        try:
            expires_at = data.get("expires_at")
            created_at = data.get("created_at")
            if isinstance(created_at, str):
                created_at = datetime.fromisoformat(created_at)
            if isinstance(expires_at, str):
                expires_at = datetime.fromisoformat(expires_at)
            return cls(
                message_id=data["message_id"],
                message_type=data["message_type"],
                message_kind=data["message_kind"],
                version=data["version"],
                payload=decode_value(data["payload"]),
                headers=decode_value(data.get("headers") or {}),
                created_at=created_at or datetime.now(timezone.utc),
                correlation_id=data.get("correlation_id"),
                causation_id=data.get("causation_id"),
                reply_to=data.get("reply_to"),
                expires_at=expires_at,
                content_type=data.get("content_type"),
                content_encoding=data.get("content_encoding"),
            ).validate()
        except KeyError as exc:
            raise InvalidMessageDefinitionError(
                f"Missing envelope field: {exc.args[0]}"
            ) from exc
