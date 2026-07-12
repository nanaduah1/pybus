from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import sys
from types import ModuleType, SimpleNamespace

import pytest

from pybus.exceptions import DeserializationError
from pybus.integrations.django_serialization import (
    DjangoPayloadSerializer,
    deserialize_django_payload,
    serialize_django_payload,
)


@dataclass
class ExamplePayload:
    amount: Decimal
    name: str


def test_django_payload_serializer_round_trips_dataclass_and_decimal() -> None:
    serializer = DjangoPayloadSerializer()

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
            "__pybus_type__": "decimal",
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

    serialized = serialize_django_payload(fake_instance)

    assert serialized == {
        "__pybus_type__": "django_model",
        "app_label": "schools",
        "model_name": "student",
        "pk": 7,
    }
    assert deserialize_django_payload(serialized) is fake_instance


def test_deserialize_django_payload_raises_when_model_is_missing(monkeypatch) -> None:
    class FakeDoesNotExist(Exception):
        pass

    class FakeManager:
        def get(self, *, pk: int):
            raise FakeDoesNotExist(pk)

    fake_model_class = SimpleNamespace(
        objects=FakeManager(),
        DoesNotExist=FakeDoesNotExist,
    )
    fake_apps = SimpleNamespace(
        get_model=lambda app_label, model_name: fake_model_class,
    )
    django_apps_module = ModuleType("django.apps")
    django_apps_module.apps = fake_apps

    monkeypatch.setitem(sys.modules, "django.apps", django_apps_module)

    with pytest.raises(DeserializationError):
        deserialize_django_payload(
            {
                "__pybus_type__": "django_model",
                "app_label": "schools",
                "model_name": "student",
                "pk": 99,
            }
        )
