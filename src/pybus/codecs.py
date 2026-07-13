from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import fields, is_dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from pybus._codec import decode_value, encode_value
from pybus.exceptions import DeserializationError, SerializationError


TYPE_KEY = "__pybus_type__"


class PayloadCodec(Protocol):
    def encode(
        self, value: Any, *, context: Mapping[str, Any] | None = None
    ) -> Any: ...

    def decode(
        self, value: Any, *, context: Mapping[str, Any] | None = None
    ) -> Any: ...


class PayloadTypeRegistry:
    def __init__(self, types: Iterable[type[Any]] = ()) -> None:
        self._types: dict[str, type[Any]] = {}
        self._canonical_ids: dict[type[Any], str] = {}
        for value_type in types:
            self.register(value_type)

    def register(
        self,
        value_type: type[Any],
        *,
        type_id: str | None = None,
        aliases: Iterable[str] = (),
    ) -> str:
        if not is_dataclass(value_type):
            raise TypeError("payload types must be dataclass types")
        canonical_id = type_id or fully_qualified_type_name(value_type)
        existing_id = self._canonical_ids.get(value_type)
        if existing_id is not None and existing_id != canonical_id:
            raise ValueError(
                f"Payload type {value_type!r} is already registered as {existing_id!r}"
            )
        for registered_id in (canonical_id, *aliases):
            existing = self._types.get(registered_id)
            if existing is not None and existing is not value_type:
                raise ValueError(
                    f"Payload type identifier {registered_id!r} is already registered"
                )
            self._types[registered_id] = value_type
        self._canonical_ids[value_type] = canonical_id
        return canonical_id

    def type_id_for(self, value_type: type[Any]) -> str:
        try:
            return self._canonical_ids[value_type]
        except KeyError as exc:
            raise SerializationError(
                f"Payload type {fully_qualified_type_name(value_type)!r} is not registered"
            ) from exc

    def resolve(self, type_id: str) -> type[Any]:
        try:
            return self._types[type_id]
        except KeyError as exc:
            raise DeserializationError(
                f"Payload type {type_id!r} is not registered"
            ) from exc


def fully_qualified_type_name(value_type: type[Any]) -> str:
    return f"{value_type.__module__}:{value_type.__qualname__}"


class PythonPayloadCodec:
    def __init__(
        self,
        *,
        type_registry: PayloadTypeRegistry | Iterable[type[Any]] | None = None,
    ) -> None:
        if isinstance(type_registry, PayloadTypeRegistry):
            self.type_registry = type_registry
        else:
            self.type_registry = PayloadTypeRegistry(type_registry or ())

    def encode(self, value: Any, *, context: Mapping[str, Any] | None = None) -> Any:
        if isinstance(value, Decimal):
            if not value.is_finite():
                raise SerializationError("Decimal payload values must be finite")
            return {TYPE_KEY: "decimal", "value": str(value)}

        if is_dataclass(value) and not isinstance(value, type):
            value_type = type(value)
            return {
                TYPE_KEY: "dataclass",
                "type": self.type_registry.type_id_for(value_type),
                "version": 1,
                "fields": {
                    field.name: self.encode(getattr(value, field.name), context=context)
                    for field in fields(value)
                },
            }

        if isinstance(value, dict):
            return {
                str(key): self.encode(inner, context=context)
                for key, inner in value.items()
            }
        if isinstance(value, (list, tuple)):
            return [self.encode(inner, context=context) for inner in value]
        return encode_value(value)

    def decode(self, value: Any, *, context: Mapping[str, Any] | None = None) -> Any:
        if isinstance(value, list):
            return [self.decode(inner, context=context) for inner in value]

        if isinstance(value, dict):
            value_kind = value.get(TYPE_KEY)
            if value_kind == "decimal":
                encoded_decimal = value.get("value")
                if not isinstance(encoded_decimal, str):
                    raise DeserializationError("Invalid decimal encoding")
                try:
                    decoded_decimal = Decimal(encoded_decimal)
                except InvalidOperation as exc:
                    raise DeserializationError("Invalid decimal encoding") from exc
                if not decoded_decimal.is_finite():
                    raise DeserializationError("Decimal payload values must be finite")
                return decoded_decimal

            if value_kind == "dataclass":
                type_id = value.get("type")
                if type_id is None:
                    module = value.get("module")
                    qualname = value.get("qualname")
                    if isinstance(module, str) and isinstance(qualname, str):
                        type_id = f"{module}:{qualname}"
                if not isinstance(type_id, str) or not type_id:
                    raise DeserializationError("Dataclass encoding requires a type")
                if value.get("version", 1) != 1:
                    raise DeserializationError(
                        f"Unsupported dataclass payload version: {value.get('version')!r}"
                    )
                value_type = self.type_registry.resolve(type_id)
                encoded_fields = value.get("fields")
                if not isinstance(encoded_fields, dict):
                    raise DeserializationError("Dataclass encoding requires fields")
                try:
                    return value_type(
                        **{
                            key: self.decode(inner, context=context)
                            for key, inner in encoded_fields.items()
                        }
                    )
                except TypeError as exc:
                    raise DeserializationError(
                        f"Invalid fields for payload type {type_id!r}"
                    ) from exc

            if value_kind in {"datetime", "date", "time", "uuid"}:
                return decode_value(value)
            if value_kind is not None:
                raise DeserializationError(
                    f"Unsupported payload type marker: {value_kind!r}"
                )

            return {
                key: self.decode(inner, context=context) for key, inner in value.items()
            }

        return decode_value(value)


def resolve_payload_codec(payload_codec: PayloadCodec | None) -> PayloadCodec:
    return payload_codec or PythonPayloadCodec()


__all__ = [
    "PayloadCodec",
    "PayloadTypeRegistry",
    "PythonPayloadCodec",
    "TYPE_KEY",
    "fully_qualified_type_name",
]
