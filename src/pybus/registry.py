from __future__ import annotations

from collections.abc import Callable

from pybus.exceptions import HandlerNotFoundError
from pybus.messages import BaseMessage

Handler = Callable[[BaseMessage], object]


class Registry:
    def __init__(self) -> None:
        self._handlers: dict[tuple[str, str], list[Handler]] = {}

    def register(
        self,
        message_kind: str,
        message_type: str,
        handler: Handler,
        *,
        allow_multiple: bool | None = None,
    ) -> Handler:
        if allow_multiple is None:
            allow_multiple = message_kind == "event"

        key = (message_kind, message_type)
        handlers = self._handlers.setdefault(key, [])

        if not allow_multiple and handlers:
            raise ValueError(
                f"Handler already registered for {message_kind}:{message_type}"
            )

        handlers.append(handler)
        return handler

    def handlers_for(self, message_kind: str, message_type: str) -> tuple[Handler, ...]:
        return tuple(self._handlers.get((message_kind, message_type), ()))

    def require_handler(self, message_kind: str, message_type: str) -> Handler:
        handlers = self.handlers_for(message_kind, message_type)
        if not handlers:
            raise HandlerNotFoundError(
                f"No handlers registered for {message_kind}:{message_type}"
            )
        if len(handlers) > 1:
            raise ValueError(
                f"Multiple handlers registered for {message_kind}:{message_type}"
            )
        return handlers[0]

    def clear(self) -> None:
        self._handlers.clear()

    def has_handlers(self, message_kind: str, message_type: str) -> bool:
        return bool(self.handlers_for(message_kind, message_type))
