from __future__ import annotations

from collections.abc import Iterable, Mapping
from importlib import import_module
from typing import Any, Callable

from pybus.codecs import (
    TYPE_KEY,
    PayloadTypeRegistry,
    PythonPayloadCodec,
)
from pybus.exceptions import DeserializationError, SerializationError
from pybus.serializer import JsonSerializer


_MODEL_TYPE = "django_model"
_DJANGO_TYPE_PREFIX = "django://"


def _django_model_base() -> type[Any] | None:
    try:
        models_module = import_module("django.db.models")
    except ModuleNotFoundError:
        return None
    return getattr(models_module, "Model", None)


class DjangoPayloadCodec(PythonPayloadCodec):
    """Extend the generic Python codec with Django model references."""

    def __init__(
        self,
        *,
        type_registry: PayloadTypeRegistry | Iterable[type[Any]] | None = None,
        model_resolvers: Mapping[str, Callable[[Any, Mapping[str, Any]], Any]]
        | None = None,
    ) -> None:
        super().__init__(type_registry=type_registry)
        self.model_resolvers = dict(model_resolvers or {})

    def encode(self, value: Any, *, context: Mapping[str, Any] | None = None) -> Any:
        model_base = _django_model_base()
        if model_base is not None and isinstance(value, model_base):
            model_type = (
                f"{_DJANGO_TYPE_PREFIX}{value._meta.app_label}/{value._meta.model_name}"
            )
            if model_type not in self.model_resolvers:
                raise SerializationError(
                    f"Django model type {model_type!r} has no configured resolver"
                )
            return {
                TYPE_KEY: _MODEL_TYPE,
                "type": model_type,
                "pk": super().encode(value.pk, context=context),
            }
        return super().encode(value, context=context)

    def decode(self, value: Any, *, context: Mapping[str, Any] | None = None) -> Any:
        if isinstance(value, dict) and value.get(TYPE_KEY) == _MODEL_TYPE:
            model_type = value.get("type")
            if model_type is None:
                app_label = value.get("app_label")
                model_name = value.get("model_name")
                if isinstance(app_label, str) and isinstance(model_name, str):
                    model_type = f"{_DJANGO_TYPE_PREFIX}{app_label}/{model_name}"
            if not isinstance(model_type, str) or not model_type.startswith(
                _DJANGO_TYPE_PREFIX
            ):
                raise DeserializationError("Invalid Django model type")
            resolver = self.model_resolvers.get(model_type)
            if resolver is None:
                raise DeserializationError(
                    f"Django model type {model_type!r} has no configured resolver"
                )
            pk = super().decode(value.get("pk"), context=context)
            try:
                resolved = resolver(pk, context or {})
            except DeserializationError:
                raise
            except Exception as exc:
                raise DeserializationError(
                    f"Could not resolve Django model reference {model_type}({pk})"
                ) from exc
            if resolved is None:
                raise DeserializationError(
                    f"Django model reference {model_type}({pk}) was not found"
                )
            return resolved
        return super().decode(value, context=context)


def serialize_django_payload(
    value: Any,
    *,
    type_registry: PayloadTypeRegistry | Iterable[type[Any]] | None = None,
    model_resolvers: Mapping[str, Callable[[Any, Mapping[str, Any]], Any]]
    | None = None,
    context: Mapping[str, Any] | None = None,
) -> Any:
    """Convert payloads into JSON-safe data while preserving Django models."""

    return DjangoPayloadCodec(
        type_registry=type_registry,
        model_resolvers=model_resolvers,
    ).encode(value, context=context)


def deserialize_django_payload(
    value: Any,
    *,
    type_registry: PayloadTypeRegistry | Iterable[type[Any]] | None = None,
    model_resolvers: Mapping[str, Callable[[Any, Mapping[str, Any]], Any]]
    | None = None,
    context: Mapping[str, Any] | None = None,
) -> Any:
    """Restore payloads created by `serialize_django_payload`."""

    return DjangoPayloadCodec(
        type_registry=type_registry,
        model_resolvers=model_resolvers,
    ).decode(value, context=context)


class DjangoPayloadSerializer:
    """JSON serializer for payloads with Django model support."""

    def __init__(
        self,
        serializer: JsonSerializer | None = None,
        *,
        type_registry: PayloadTypeRegistry | Iterable[type[Any]] | None = None,
        model_resolvers: Mapping[str, Callable[[Any, Mapping[str, Any]], Any]]
        | None = None,
    ) -> None:
        self._serializer = serializer or JsonSerializer()
        self.codec = DjangoPayloadCodec(
            type_registry=type_registry,
            model_resolvers=model_resolvers,
        )

    def dumps(self, value: Any) -> str:
        return self._serializer.dumps(self.codec.encode(value))

    def dump(self, value: Any) -> bytes:
        return self._serializer.dump(self.codec.encode(value))

    def loads(self, value: str | bytes) -> Any:
        return self.codec.decode(self._serializer.loads(value))

    def load(self, value: bytes) -> Any:
        return self.loads(value)


__all__ = [
    "DjangoPayloadCodec",
    "DjangoPayloadSerializer",
    "deserialize_django_payload",
    "serialize_django_payload",
]
