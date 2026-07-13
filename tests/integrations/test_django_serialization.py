from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import sys
from types import ModuleType, SimpleNamespace

import pytest

from pybus.exceptions import DeserializationError
from pybus.messages import EventMessage
from pybus.integrations.django_serialization import (
    DjangoPayloadCodec,
    DjangoPayloadSerializer,
    deserialize_django_payload,
    serialize_django_payload,
)


@dataclass
class ExamplePayload:
    amount: Decimal
    name: str


def test_django_payload_serializer_round_trips_dataclass_and_decimal() -> None:
    serializer = DjangoPayloadSerializer(type_registry=[ExamplePayload])

    restored = serializer.loads(
        serializer.dumps(
            ExamplePayload(
                amount=Decimal("12.50"),
                name="Tuition",
            )
        )
    )

    assert restored == ExamplePayload(amount=Decimal("12.50"), name="Tuition")


def test_serialize_django_payload_works_without_django_installed(monkeypatch) -> None:
    real_import = __import__("importlib").import_module

    def fake_import(name: str, package: str | None = None):
        if name == "django.db.models":
            raise ModuleNotFoundError(name)
        return real_import(name, package)

    monkeypatch.setattr(
        "pybus.integrations.django_serialization.import_module",
        fake_import,
    )

    payload = {"amount": Decimal("3.75")}

    assert serialize_django_payload(payload) == {
        "amount": {
            "__pybus_codec__": "decimal",
            "version": 1,
            "value": "3.75",
        }
    }


def test_django_payload_serializer_round_trips_fake_django_model(monkeypatch) -> None:
    class FakeModel:
        pass

    class FakeDoesNotExist(Exception):
        pass

    fake_instance = FakeModel()
    fake_instance.pk = 7
    fake_instance._meta = SimpleNamespace(
        app_label="schools",
        model_name="student",
    )

    class FakeManager:
        def get(self, *, pk: int):
            assert pk == 7
            return fake_instance

    fake_model_class = SimpleNamespace(
        objects=FakeManager(),
        DoesNotExist=FakeDoesNotExist,
    )
    fake_apps = SimpleNamespace(
        get_model=lambda app_label, model_name: (
            fake_model_class
            if (app_label, model_name) == ("schools", "student")
            else None
        )
    )

    django_apps_module = ModuleType("django.apps")
    django_apps_module.apps = fake_apps
    django_models_module = ModuleType("django.db.models")
    django_models_module.Model = FakeModel

    monkeypatch.setitem(sys.modules, "django.apps", django_apps_module)
    monkeypatch.setitem(sys.modules, "django.db.models", django_models_module)

    def resolver(pk: int, context: object) -> object | None:
        return fake_instance if pk == 7 else None

    model_resolvers = {"django://schools/student": resolver}
    serialized = serialize_django_payload(
        fake_instance,
        model_resolvers=model_resolvers,
    )

    assert serialized == {
        "__pybus_codec__": "django_model",
        "version": 1,
        "type": "django://schools/student",
        "pk": 7,
    }
    assert (
        deserialize_django_payload(serialized, model_resolvers=model_resolvers)
        is fake_instance
    )


def test_deserialize_django_payload_raises_when_model_is_missing(monkeypatch) -> None:
    with pytest.raises(DeserializationError):
        deserialize_django_payload(
            {
                "__pybus_type__": "django_model",
                "type": "django://schools/student",
                "pk": 99,
            },
            model_resolvers={
                "django://schools/student": lambda pk, context: (_ for _ in ()).throw(
                    LookupError(pk)
                )
            },
        )


def test_django_codec_rejects_unregistered_model_reference() -> None:
    codec = DjangoPayloadCodec()

    with pytest.raises(DeserializationError, match="no configured resolver"):
        codec.decode(
            {
                "__pybus_type__": "django_model",
                "type": "django://schools/student",
                "pk": 7,
            }
        )


@pytest.mark.parametrize("version", [True, 1.0])
def test_django_codec_rejects_non_integer_codec_version(version: object) -> None:
    codec = DjangoPayloadCodec(
        model_resolvers={"django://schools/student": lambda pk, context: {"pk": pk}}
    )

    with pytest.raises(DeserializationError, match="Unsupported payload codec version"):
        codec.decode(
            {
                "__pybus_codec__": "django_model",
                "version": version,
                "type": "django://schools/student",
                "pk": 7,
            }
        )


def test_django_codec_uses_explicit_tenant_scoped_resolver() -> None:
    calls: list[tuple[int, int]] = []
    tenant_id = 42

    def resolve_student(pk: int, context: object) -> object:
        assert isinstance(context, dict)
        resolved_tenant_id = context["tenant_id"]
        calls.append((resolved_tenant_id, pk))
        return {"tenant_id": resolved_tenant_id, "pk": pk}

    codec = DjangoPayloadCodec(
        model_resolvers={"django://schools/student": resolve_student}
    )

    restored = codec.decode(
        {
            "__pybus_type__": "django_model",
            "type": "django://schools/student",
            "pk": 7,
        },
        context={"tenant_id": tenant_id},
    )

    assert restored == {"tenant_id": 42, "pk": 7}
    assert calls == [(42, 7)]


def test_message_decode_passes_headers_to_django_resolver() -> None:
    codec = DjangoPayloadCodec(
        model_resolvers={
            "django://schools/student": lambda pk, context: {
                "tenant_id": context["tenant_id"],
                "pk": pk,
            }
        }
    )
    envelope = EventMessage(
        message_type="student.selected",
        payload={
            "student": {
                "__pybus_type__": "django_model",
                "type": "django://schools/student",
                "pk": 7,
            }
        },
        headers={"tenant_id": 42},
    ).to_envelope()

    restored = EventMessage.from_envelope(envelope, payload_codec=codec)

    assert restored.payload["student"] == {"tenant_id": 42, "pk": 7}


def test_django_codec_decodes_allowlisted_legacy_model_shape() -> None:
    codec = DjangoPayloadCodec(
        model_resolvers={"django://schools/student": lambda pk, context: {"pk": pk}}
    )

    restored = codec.decode(
        {
            "__pybus_type__": "django_model",
            "app_label": "schools",
            "model_name": "student",
            "pk": 7,
        }
    )

    assert restored == {"pk": 7}


def test_django_codec_rejects_resolver_returning_none() -> None:
    codec = DjangoPayloadCodec(
        model_resolvers={"django://schools/student": lambda pk, context: None}
    )

    with pytest.raises(DeserializationError, match="was not found"):
        codec.decode(
            {
                "__pybus_type__": "django_model",
                "type": "django://schools/student",
                "pk": 7,
            },
            context={"tenant_id": 42},
        )


def test_django_payload_codec_delegates_python_values_to_core_codec() -> None:
    codec = DjangoPayloadCodec(type_registry=[ExamplePayload])
    payload = ExamplePayload(amount=Decimal("42.10"), name="Tuition")

    encoded = codec.encode(payload)

    assert encoded["__pybus_codec__"] == "dataclass"
    assert (
        encoded["type"] == f"{ExamplePayload.__module__}:{ExamplePayload.__qualname__}"
    )
    assert codec.decode(encoded) == payload
