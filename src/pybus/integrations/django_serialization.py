from __future__ import annotations

from dataclasses import fields, is_dataclass
from decimal import Decimal
from importlib import import_module
from typing import Any

from pybus._codec import decode_value, encode_value
from pybus.exceptions import DeserializationError
from pybus.serializer import JsonSerializer


_TYPE_KEY = "__pybus_type__"
_MODEL_TYPE = "django_model"
_DECIMAL_TYPE = "decimal"
_DATACLASS_TYPE = "dataclass"


def _resolve_qualname(module_name: str, qualname: str) -> Any:
    value = import_module(module_name)
    for part in qualname.split("."):
        value = getattr(value, part)
    return value


def _django_model_base() -> type[Any] | None:
    try:
        models_module = import_module("django.db.models")
    except ModuleNotFoundError:
        return None
    return getattr(models_module, "Model", None)


def _django_apps_registry() -> Any:
    return import_module("django.apps").apps


def serialize_django_payload(value: Any) -> Any:
    """Convert payloads into JSON-safe data while preserving Django models."""

    model_base = _django_model_base()
    if model_base is not None and isinstance(value, model_base):
        return {
            _TYPE_KEY: _MODEL_TYPE,
            "app_label": value._meta.app_label,
            "model_name": value._meta.model_name,
            "pk": serialize_django_payload(value.pk),
        }

    if isinstance(value, Decimal):
        return {
            _TYPE_KEY: _DECIMAL_TYPE,
            "value": str(value),
        }

    if is_dataclass(value) and not isinstance(value, type):
        return {
            _TYPE_KEY: _DATACLASS_TYPE,
            "module": value.__class__.__module__,
            "qualname": value.__class__.__qualname__,
            "fields": {
                field.name: serialize_django_payload(getattr(value, field.name))
                for field in fields(value)
            },
        }

    if isinstance(value, dict):
        return {
            str(key): serialize_django_payload(inner) for key, inner in value.items()
        }

    if isinstance(value, list):
        return [serialize_django_payload(inner) for inner in value]

    if isinstance(value, tuple):
        return [serialize_django_payload(inner) for inner in value]

    return encode_value(value)


def deserialize_django_payload(value: Any) -> Any:
    """Restore payloads created by `serialize_django_payload`."""

    if isinstance(value, list):
        return [deserialize_django_payload(inner) for inner in value]

    if isinstance(value, dict):
        if value.get(_TYPE_KEY) == _MODEL_TYPE:
            model = _django_apps_registry().get_model(
                value["app_label"],
                value["model_name"],
            )
            pk = deserialize_django_payload(value.get("pk"))
            try:
                return model.objects.get(pk=pk)
            except model.DoesNotExist as exc:
                raise DeserializationError(
                    "Referenced "
                    f"{value['app_label']}.{value['model_name']}({pk}) no longer exists"
                ) from exc

        if value.get(_TYPE_KEY) == _DECIMAL_TYPE:
            return Decimal(value["value"])

        if value.get(_TYPE_KEY) == _DATACLASS_TYPE:
            cls = _resolve_qualname(value["module"], value["qualname"])
            restored_fields = {
                key: deserialize_django_payload(inner)
                for key, inner in value.get("fields", {}).items()
            }
            return cls(**restored_fields)

        return {key: deserialize_django_payload(inner) for key, inner in value.items()}

    return decode_value(value)


class DjangoPayloadSerializer:
    """JSON serializer for payloads with Django model support."""

    def __init__(self, serializer: JsonSerializer | None = None) -> None:
        self._serializer = serializer or JsonSerializer()

    def dumps(self, value: Any) -> str:
        return self._serializer.dumps(serialize_django_payload(value))

    def dump(self, value: Any) -> bytes:
        return self._serializer.dump(serialize_django_payload(value))

    def loads(self, value: str | bytes) -> Any:
        return deserialize_django_payload(self._serializer.loads(value))

    def load(self, value: bytes) -> Any:
        return self.loads(value)


__all__ = [
    "DjangoPayloadSerializer",
    "deserialize_django_payload",
    "serialize_django_payload",
]
