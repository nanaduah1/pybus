from __future__ import annotations

import json
from typing import Any

from pybus._codec import decode_value, encode_value


class JsonSerializer:
    """Serialize pybus envelopes and message payloads as canonical JSON."""

    def dumps(self, value: Any) -> str:
        return json.dumps(encode_value(value), separators=(",", ":"), sort_keys=True)

    def dump(self, value: Any) -> bytes:
        return self.dumps(value).encode("utf-8")

    def loads(self, value: str | bytes) -> Any:
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        return decode_value(json.loads(value))

    def load(self, value: bytes) -> Any:
        return self.loads(value)
