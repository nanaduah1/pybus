from __future__ import annotations

import json
from typing import Any

from pybus._codec import decode_value, encode_value
from pybus.exceptions import DeserializationError, SerializationError


class JsonSerializer:
    """Serialize pybus envelopes and message payloads as canonical JSON."""

    def dumps(self, value: Any) -> str:
        try:
            return json.dumps(
                encode_value(value), separators=(",", ":"), sort_keys=True
            )
        except (TypeError, ValueError) as exc:
            raise SerializationError("Unable to serialize value as JSON") from exc

    def dump(self, value: Any) -> bytes:
        return self.dumps(value).encode("utf-8")

    def loads(self, value: str | bytes) -> Any:
        try:
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            return decode_value(json.loads(value))
        except UnicodeDecodeError as exc:
            raise DeserializationError("Payload is not valid UTF-8") from exc
        except json.JSONDecodeError as exc:
            raise DeserializationError("Payload is not valid JSON") from exc

    def load(self, value: bytes) -> Any:
        return self.loads(value)
