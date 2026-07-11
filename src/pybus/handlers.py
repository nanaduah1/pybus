from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass

from pybus.messages import BaseMessage
from pybus.registry import Registry

Handler = Callable[[BaseMessage], object]


@dataclass(frozen=True, slots=True)
class HandlerSpec:
    message_kind: str
    message_type: str
    allow_multiple: bool | None = None


MessageBinding = str | type[BaseMessage]


def _handler_spec(handler: object) -> HandlerSpec | None:
    spec = getattr(handler, "__pybus_handler_spec__", None)
    if spec is not None:
        return spec

    func = getattr(handler, "__func__", None)
    if func is not None:
        return getattr(func, "__pybus_handler_spec__", None)

    return None


def _resolve_message_binding(
    message: MessageBinding,
    *,
    expected_kind: str,
) -> tuple[str, str]:
    if isinstance(message, str):
        return expected_kind, message

    if not inspect.isclass(message) or not issubclass(message, BaseMessage):
        raise TypeError("message binding must be a message type or message_type string")

    message_kind = getattr(message, "message_kind", None)
    if message_kind != expected_kind:
        raise ValueError(
            f"Expected a {expected_kind} message type, got {message_kind!r}"
        )

    message_type = getattr(message, "message_type", None)
    if not isinstance(message_type, str) or not message_type:
        raise TypeError(
            f"{message.__name__} must define a non-empty message_type class attribute"
        )

    return message_kind, message_type


def _decorate_handler(
    message_kind: str,
    message: MessageBinding,
    *,
    allow_multiple: bool | None = None,
    registry: Registry | None = None,
) -> Callable[[Handler], Handler]:
    resolved_kind, resolved_type = _resolve_message_binding(
        message,
        expected_kind=message_kind,
    )
    spec = HandlerSpec(
        message_kind=resolved_kind,
        message_type=resolved_type,
        allow_multiple=allow_multiple,
    )

    def decorator(handler: Handler) -> Handler:
        setattr(handler, "__pybus_handler_spec__", spec)
        if registry is not None:
            registry.register(
                spec.message_kind,
                spec.message_type,
                handler,
                allow_multiple=spec.allow_multiple,
            )
        return handler

    return decorator


def event_handler(
    message: MessageBinding,
    *,
    allow_multiple: bool | None = None,
    registry: Registry | None = None,
) -> Callable[[Handler], Handler]:
    return _decorate_handler(
        "event",
        message,
        allow_multiple=allow_multiple,
        registry=registry,
    )


def command_handler(
    message: MessageBinding,
    *,
    allow_multiple: bool | None = None,
    registry: Registry | None = None,
) -> Callable[[Handler], Handler]:
    return _decorate_handler(
        "command",
        message,
        allow_multiple=allow_multiple,
        registry=registry,
    )


def request_handler(
    message: MessageBinding,
    *,
    allow_multiple: bool | None = None,
    registry: Registry | None = None,
) -> Callable[[Handler], Handler]:
    return _decorate_handler(
        "request",
        message,
        allow_multiple=allow_multiple,
        registry=registry,
    )


def register_handlers(registry: Registry, *targets: object) -> tuple[Handler, ...]:
    registered: list[Handler] = []
    for target in targets:
        registered.extend(_register_target_handlers(registry, target))
    return tuple(registered)


def bind_handlers(registry: Registry, *targets: object) -> tuple[Handler, ...]:
    return register_handlers(registry, *targets)


def _register_target_handlers(registry: Registry, target: object) -> list[Handler]:
    direct_spec = _handler_spec(target)
    if direct_spec is not None and callable(target):
        return [
            registry.register(
                direct_spec.message_kind,
                direct_spec.message_type,
                target,  # type: ignore[arg-type]
                allow_multiple=direct_spec.allow_multiple,
            )
        ]

    registered: list[Handler] = []
    for _, candidate in inspect.getmembers(target, predicate=callable):
        spec = _handler_spec(candidate)
        if spec is None:
            continue
        registered.append(
            registry.register(
                spec.message_kind,
                spec.message_type,
                candidate,
                allow_multiple=spec.allow_multiple,
            )
        )
    return registered


__all__ = [
    "HandlerSpec",
    "bind_handlers",
    "command_handler",
    "event_handler",
    "register_handlers",
    "request_handler",
]
