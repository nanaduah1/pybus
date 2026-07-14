from __future__ import annotations

from pybus.codecs import PayloadCodec, resolve_payload_codec
from pybus.envelope import MessageEnvelope
from pybus.exceptions import (
    DeserializationError,
    HandlerNotFoundError,
    InvalidMessageDefinitionError,
)
from pybus.messages import BaseMessage, message_class_for_kind
from pybus.registry import Registry
from pybus.serializer import JsonSerializer


class Dispatcher:
    def __init__(
        self,
        registry: Registry | None = None,
        serializer: JsonSerializer | None = None,
        payload_codec: PayloadCodec | None = None,
    ) -> None:
        self.registry = registry or Registry()
        self.serializer = serializer or JsonSerializer()
        self.payload_codec = resolve_payload_codec(payload_codec)

    def decode(self, message: MessageEnvelope | BaseMessage | dict) -> object:
        if isinstance(message, BaseMessage):
            return message
        if isinstance(message, MessageEnvelope):
            typed_class = self.registry.message_class_for(
                message.message_kind,
                message.message_type,
            )
            if typed_class is not None:
                expected_version = getattr(typed_class, "version", 1)
                if message.version != expected_version:
                    raise InvalidMessageDefinitionError(
                        f"Expected version={expected_version!r}, "
                        f"got {message.version!r}"
                    )
                headers = self.payload_codec.decode(message.headers)
                payload = self.payload_codec.decode(
                    message.payload,
                    context=headers,
                )
                if not isinstance(payload, dict):
                    raise DeserializationError(
                        "Typed message payload must decode to a mapping"
                    )
                try:
                    return typed_class(**payload)
                except (TypeError, ValueError) as exc:
                    raise DeserializationError(
                        f"Invalid payload for {message.message_type!r}"
                    ) from exc
            message_cls = message_class_for_kind(message.message_kind)
            return message_cls.from_envelope(
                message,
                payload_codec=self.payload_codec,
            )
        if isinstance(message, dict):
            envelope = MessageEnvelope.from_dict(message)
            return self.decode(envelope)
        raise TypeError(f"Unsupported message payload: {type(message)!r}")

    def dispatch(self, message: MessageEnvelope | BaseMessage | dict) -> object:
        decoded = self.decode(message)
        message_kind = getattr(decoded, "message_kind", None)
        message_type = getattr(decoded, "message_type", None)
        handlers = self.registry.handlers_for(
            message_kind,
            message_type,
        )
        if not handlers:
            raise HandlerNotFoundError(
                f"No handlers registered for {message_kind}:{message_type}"
            )

        results = [handler(decoded) for handler in handlers]
        return results[0] if len(results) == 1 else results
