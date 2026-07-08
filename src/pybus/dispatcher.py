from __future__ import annotations

from pybus.envelope import MessageEnvelope
from pybus.exceptions import HandlerNotFoundError
from pybus.messages import BaseMessage, message_class_for_kind
from pybus.registry import Registry
from pybus.serializer import JsonSerializer


class Dispatcher:
    def __init__(
        self,
        registry: Registry | None = None,
        serializer: JsonSerializer | None = None,
    ) -> None:
        self.registry = registry or Registry()
        self.serializer = serializer or JsonSerializer()

    def decode(self, message: MessageEnvelope | BaseMessage | dict) -> BaseMessage:
        if isinstance(message, BaseMessage):
            return message
        if isinstance(message, MessageEnvelope):
            message_cls = message_class_for_kind(message.message_kind)
            return message_cls.from_envelope(message)
        if isinstance(message, dict):
            envelope = MessageEnvelope.from_dict(message)
            return self.decode(envelope)
        raise TypeError(f"Unsupported message payload: {type(message)!r}")

    def dispatch(self, message: MessageEnvelope | BaseMessage | dict) -> object:
        decoded = self.decode(message)
        handlers = self.registry.handlers_for(
            decoded.message_kind,
            decoded.message_type,
        )
        if not handlers:
            raise HandlerNotFoundError(
                f"No handlers registered for {decoded.message_kind}:{decoded.message_type}"
            )

        results = [handler(decoded) for handler in handlers]
        return results[0] if len(results) == 1 else results
