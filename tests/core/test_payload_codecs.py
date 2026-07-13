from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import threading
import time
from uuid import UUID

import pytest

from pybus import CommandMessage, EventMessage, Pybus, RequestMessage, ResponseMessage
from pybus.codecs import CODEC_KEY, PayloadTypeRegistry, PythonPayloadCodec
from pybus.envelope import MessageEnvelope
from pybus.exceptions import DeserializationError, SerializationError
from pybus.handlers import batched_event_handler
from pybus.serializer import JsonSerializer
from pybus.transports.memory import MemoryTransport


@dataclass
class ReportDescriptor:
    report_name: str
    total: Decimal


@dataclass
class ReportRequest:
    report_id: str


@dataclass
class ReportResponse:
    report_id: str
    total: Decimal


def configured_codec() -> PythonPayloadCodec:
    registry = PayloadTypeRegistry()
    registry.register(ReportDescriptor)
    registry.register(ReportRequest)
    registry.register(ReportResponse)
    return PythonPayloadCodec(type_registry=registry)


def test_python_payload_codec_stores_fully_qualified_dataclass_type() -> None:
    codec = configured_codec()
    payload = ReportDescriptor(report_name="Bills", total=Decimal("12.50"))

    encoded = codec.encode(payload)

    assert encoded == {
        "__pybus_codec__": "dataclass",
        "type": f"{ReportDescriptor.__module__}:{ReportDescriptor.__qualname__}",
        "version": 1,
        "fields": {
            "report_name": "Bills",
            "total": {
                "__pybus_codec__": "decimal",
                "version": 1,
                "value": "12.50",
            },
        },
    }
    assert codec.decode(encoded) == payload


def test_python_payload_codec_resolves_registered_type_alias() -> None:
    registry = PayloadTypeRegistry()
    registry.register(ReportDescriptor, aliases=("reports.legacy:Descriptor",))
    codec = PythonPayloadCodec(type_registry=registry)

    restored = codec.decode(
        {
            "__pybus_type__": "dataclass",
            "type": "reports.legacy:Descriptor",
            "version": 1,
            "fields": {
                "report_name": "Bills",
                "total": {"__pybus_type__": "decimal", "value": "8.25"},
            },
        }
    )

    assert restored == ReportDescriptor(report_name="Bills", total=Decimal("8.25"))


def test_python_payload_codec_rejects_unregistered_type() -> None:
    codec = PythonPayloadCodec()

    with pytest.raises(DeserializationError, match="not registered"):
        codec.decode(
            {
                "__pybus_type__": "dataclass",
                "type": "untrusted.module:Payload",
                "version": 1,
                "fields": {},
            }
        )


def test_python_payload_codec_rejects_malformed_decimal() -> None:
    codec = PythonPayloadCodec()

    with pytest.raises(DeserializationError, match="Invalid decimal"):
        codec.decode({"__pybus_type__": "decimal", "value": "not-money"})


@pytest.mark.parametrize("value", [0.1, True, None])
def test_python_payload_codec_rejects_non_string_decimal(value: object) -> None:
    codec = PythonPayloadCodec()

    with pytest.raises(DeserializationError, match="Invalid decimal"):
        codec.decode({"__pybus_type__": "decimal", "value": value})


def test_python_payload_codec_preserves_decimal_boundaries_exactly() -> None:
    codec = PythonPayloadCodec()

    for value in (Decimal("0.1"), Decimal("-0.00000001"), Decimal("1E+1000")):
        assert codec.decode(codec.encode(value)) == value


def test_python_payload_codec_preserves_unknown_legacy_type_marker() -> None:
    codec = PythonPayloadCodec()
    payload = {"__pybus_type__": "future_money", "value": "1.00"}

    assert codec.decode(payload) == payload


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity")])
def test_python_payload_codec_rejects_non_finite_decimal(value: Decimal) -> None:
    codec = PythonPayloadCodec()

    with pytest.raises(SerializationError, match="finite"):
        codec.encode(value)


def test_python_payload_codec_preserves_builtin_typed_values() -> None:
    codec = PythonPayloadCodec()
    payload = {
        "created_at": datetime(2026, 7, 13, 12, 30, tzinfo=timezone.utc),
        "message_id": UUID("12345678-1234-5678-1234-567812345678"),
    }

    assert codec.decode(codec.encode(payload)) == payload


def test_registry_uses_custom_canonical_id_across_independent_codecs() -> None:
    producer_registry = PayloadTypeRegistry()
    producer_registry.register(ReportDescriptor, type_id="reports:descriptor-v1")
    consumer_registry = PayloadTypeRegistry()
    consumer_registry.register(ReportDescriptor, type_id="reports:descriptor-v1")

    encoded = PythonPayloadCodec(type_registry=producer_registry).encode(
        ReportDescriptor("Bills", Decimal("12.50"))
    )

    assert encoded["type"] == "reports:descriptor-v1"
    assert PythonPayloadCodec(type_registry=consumer_registry).decode(encoded) == (
        ReportDescriptor("Bills", Decimal("12.50"))
    )


def test_registry_rejects_conflicting_canonical_ids() -> None:
    registry = PayloadTypeRegistry([ReportDescriptor])

    with pytest.raises(ValueError, match="already registered"):
        registry.register(ReportDescriptor, type_id="reports:other")


def test_registry_alias_conflict_does_not_partially_register_type() -> None:
    registry = PayloadTypeRegistry()
    registry.register(ReportDescriptor, type_id="reports:descriptor")

    with pytest.raises(ValueError, match="already registered"):
        registry.register(
            ReportRequest,
            type_id="reports:request",
            aliases=("reports:descriptor",),
        )

    with pytest.raises(DeserializationError, match="not registered"):
        registry.resolve("reports:request")
    with pytest.raises(SerializationError, match="not registered"):
        registry.type_id_for(ReportRequest)


def test_codec_requires_registration_before_encoding_dataclass() -> None:
    with pytest.raises(SerializationError, match="not registered"):
        PythonPayloadCodec().encode(ReportDescriptor("Bills", Decimal("1.00")))


def test_codec_preserves_application_owned_legacy_type_marker() -> None:
    transport = MemoryTransport()
    bus = Pybus(transport=transport)
    handled: list[EventMessage] = []
    bus.dispatcher.registry.register("event", "rules.recorded", handled.append)
    payload = {
        "__pybus_type__": "business_rule",
        "value": "keep-me",
        "nested": {
            "__pybus_codec__": "business_rule",
            "version": 1,
            "value": "also-keep-me",
        },
    }

    bus.publish_event(EventMessage(message_type="rules.recorded", payload=payload))
    bus.listen_once(bus.topology.default_queue)

    assert handled[0].payload == payload


def test_codec_wraps_application_mapping_with_codec_marker() -> None:
    codec = PythonPayloadCodec()
    payload = {CODEC_KEY: "business_rule", "version": 1, "value": "keep-me"}

    encoded = codec.encode(payload)

    assert encoded == {
        CODEC_KEY: "mapping",
        "version": 1,
        "value": payload,
    }
    assert codec.decode(encoded) == payload


def test_codec_rejects_unknown_codec_owned_type_marker() -> None:
    codec = PythonPayloadCodec()

    with pytest.raises(DeserializationError, match="Unsupported payload codec type"):
        codec.decode({CODEC_KEY: "future_money", "version": 1, "value": "1.00"})


@pytest.mark.parametrize("version", [True, 1.0])
def test_codec_rejects_non_integer_codec_version(version: object) -> None:
    codec = PythonPayloadCodec()

    with pytest.raises(DeserializationError, match="Unsupported payload codec version"):
        codec.decode({CODEC_KEY: "decimal", "version": version, "value": "1.00"})


def test_codec_decodes_registry_backed_legacy_dataclass_shape() -> None:
    codec = configured_codec()

    restored = codec.decode(
        {
            "__pybus_type__": "dataclass",
            "module": ReportDescriptor.__module__,
            "qualname": ReportDescriptor.__qualname__,
            "fields": {
                "report_name": "Bills",
                "total": {"__pybus_type__": "decimal", "value": "12.50"},
            },
        }
    )

    assert restored == ReportDescriptor("Bills", Decimal("12.50"))


def test_independent_codecs_round_trip_typed_command() -> None:
    producer = configured_codec()
    consumer = configured_codec()
    envelope = CommandMessage(
        message_type="report.generate",
        payload=ReportRequest("R-1"),
    ).to_envelope(payload_codec=producer)

    restored = CommandMessage.from_envelope(envelope, payload_codec=consumer)

    assert restored.payload == ReportRequest("R-1")


def test_configured_codec_round_trips_event_payload_and_headers() -> None:
    transport = MemoryTransport()
    codec = configured_codec()
    bus = Pybus(transport=transport, payload_codec=codec)
    handled: list[EventMessage] = []

    bus.dispatcher.registry.register(
        "event",
        "report.queued",
        handled.append,
    )
    published = bus.publish_event(
        EventMessage(
            message_type="report.queued",
            payload=ReportDescriptor("Bills", Decimal("12.50")),
            headers={"school_id": 7, "amount": Decimal("12.50")},
            correlation_id="corr-1",
            causation_id="cause-1",
        )
    )

    bus.listen_once(bus.topology.default_queue)

    assert len(handled) == 1
    assert handled[0].payload == ReportDescriptor("Bills", Decimal("12.50"))
    assert handled[0].headers == {"school_id": 7, "amount": Decimal("12.50")}
    assert handled[0].correlation_id == "corr-1"
    assert handled[0].causation_id == "cause-1"
    assert published.message_id


def test_batch_requeue_preserves_message_identity_and_encoded_payload() -> None:
    transport = MemoryTransport()
    codec = configured_codec()
    bus = Pybus(transport=transport, payload_codec=codec)

    @batched_event_handler("report.queued", batch_size=1, max_wait=30)
    def fail_batch(messages: list[EventMessage]) -> None:
        assert messages[0].payload == ReportDescriptor("Bills", Decimal("12.50"))
        raise RuntimeError("retry")

    bus.dispatcher.registry.register(
        "event",
        "report.queued",
        fail_batch,
        allow_multiple=True,
    )
    published = bus.publish_event(
        EventMessage(
            message_type="report.queued",
            payload=ReportDescriptor("Bills", Decimal("12.50")),
            headers={"school_id": 7},
        )
    )

    bus.listen_once(bus.topology.default_queue)

    raw_message = transport.consume("batched:report.queued")
    assert raw_message is not None
    envelope = MessageEnvelope.from_dict(JsonSerializer().loads(raw_message))
    restored = bus.dispatcher.decode(envelope)
    assert envelope.message_id == published.message_id
    assert restored.payload == ReportDescriptor("Bills", Decimal("12.50"))
    assert restored.headers["school_id"] == 7
    assert envelope.headers["retries"] == 1


def test_configured_codec_round_trips_request_and_response() -> None:
    transport = MemoryTransport()
    codec = configured_codec()
    bus = Pybus(transport=transport, payload_codec=codec)

    def handle_report(message: RequestMessage) -> ResponseMessage:
        assert message.payload == ReportRequest(report_id="R-1")
        return ResponseMessage(
            message_type="report.get.response",
            payload=ReportResponse(report_id="R-1", total=Decimal("99.95")),
        )

    bus.dispatcher.registry.register("request", "report.get", handle_report)

    def run_worker() -> None:
        time.sleep(0.01)
        bus.listen_once(bus.topology.default_queue)

    worker = threading.Thread(target=run_worker)
    worker.start()
    response = bus.request(
        RequestMessage(
            message_type="report.get",
            payload=ReportRequest(report_id="R-1"),
            correlation_id="corr-report",
        ),
        timeout=1,
    )
    worker.join(timeout=1)

    assert response.payload == ReportResponse(
        report_id="R-1",
        total=Decimal("99.95"),
    )
    assert response.correlation_id == "corr-report"
    assert worker.is_alive() is False
