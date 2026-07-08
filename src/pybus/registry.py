from __future__ import annotations

from collections.abc import Callable

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

    def clear(self) -> None:
        self._handlers.clear()

    def has_handlers(self, message_kind: str, message_type: str) -> bool:
        return bool(self.handlers_for(message_kind, message_type))
