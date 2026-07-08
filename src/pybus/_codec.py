from __future__ import annotations

from datetime import date, datetime, time, timezone
from typing import Any
from uuid import UUID

_TYPE_KEY = "__pybus_type__"
_VALUE_KEY = "value"


def _encode_datetime(value: datetime) -> dict[str, str]:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return {_TYPE_KEY: "datetime", _VALUE_KEY: value.isoformat()}


def _encode_date(value: date) -> dict[str, str]:
    return {_TYPE_KEY: "date", _VALUE_KEY: value.isoformat()}


def _encode_time(value: time) -> dict[str, str]:
    if value.tzinfo is None:
        return {_TYPE_KEY: "time", _VALUE_KEY: value.isoformat()}
    return {_TYPE_KEY: "time", _VALUE_KEY: value.isoformat()}


def _encode_uuid(value: UUID) -> dict[str, str]:
    return {_TYPE_KEY: "uuid", _VALUE_KEY: str(value)}


def encode_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return _encode_datetime(value)
    if isinstance(value, date) and not isinstance(value, datetime):
        return _encode_date(value)
    if isinstance(value, time):
        return _encode_time(value)
    if isinstance(value, UUID):
        return _encode_uuid(value)
    if isinstance(value, dict):
        return {str(key): encode_value(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [encode_value(inner) for inner in value]
    if isinstance(value, tuple):
        return [encode_value(inner) for inner in value]
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return encode_value(to_dict())
    return value


def decode_value(value: Any) -> Any:
    if isinstance(value, list):
        return [decode_value(inner) for inner in value]
    if isinstance(value, dict):
        type_name = value.get(_TYPE_KEY)
        if type_name == "datetime":
            return datetime.fromisoformat(value[_VALUE_KEY])
        if type_name == "date":
            return date.fromisoformat(value[_VALUE_KEY])
        if type_name == "time":
            return time.fromisoformat(value[_VALUE_KEY])
        if type_name == "uuid":
            return UUID(value[_VALUE_KEY])
        return {key: decode_value(inner) for key, inner in value.items()}
    return value
