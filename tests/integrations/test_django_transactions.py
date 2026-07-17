# ruff: noqa: E402

import pytest


django = pytest.importorskip("django")

from django.conf import settings


if not settings.configured:
    settings.configure(
        DATABASES={
            "default": {
                "ENGINE": "django.db.backends.sqlite3",
                "NAME": ":memory:",
            }
        },
        SECRET_KEY="pybus-tests",
    )
    django.setup()

from django.db import connection, transaction

from pybus import configure_transport, event
from pybus.envelope import MessageEnvelope
from pybus.integrations.django import prepare_event, publish_event, publish_prepared
from pybus.queues import QueueTopology
from pybus.serializer import JsonSerializer
from pybus.transports.memory import MemoryTransport


@event("student.enrolled.transactional", queue="student.lifecycle")
class StudentEnrolled:
    student_id: int


class FailingTransport(MemoryTransport):
    def __init__(self) -> None:
        super().__init__()
        self.publish_attempts = 0
        self.attempted_channels: list[str] = []

    def publish(self, channel: str, message: bytes) -> None:
        self.publish_attempts += 1
        self.attempted_channels.append(channel)
        raise RuntimeError("transport unavailable")


MESSAGE_TOPOLOGY = QueueTopology().declare_queue("student.lifecycle")


def _stored_student_ids(transport: MemoryTransport) -> list[int]:
    values: list[int] = []
    while raw := transport.consume("student.lifecycle"):
        envelope = MessageEnvelope.from_dict(JsonSerializer().loads(raw))
        values.append(envelope.payload["student_id"])
    return values


def test_django_publish_event_defers_until_outer_commit() -> None:
    transport = MemoryTransport()
    configure_transport(transport, topology=MESSAGE_TOPOLOGY)

    with transaction.atomic():
        publish_event(StudentEnrolled(student_id=1))
        assert transport.size("student.lifecycle") == 0

    assert _stored_student_ids(transport) == [1]


def test_django_publish_event_suppresses_rolled_back_work() -> None:
    transport = MemoryTransport()
    configure_transport(transport, topology=MESSAGE_TOPOLOGY)

    with pytest.raises(RuntimeError, match="rollback"):
        with transaction.atomic():
            publish_event(StudentEnrolled(student_id=2))
            raise RuntimeError("rollback")

    assert transport.size("student.lifecycle") == 0


def test_django_publish_prepared_suppresses_rolled_back_work() -> None:
    transport = MemoryTransport()
    configure_transport(transport, topology=MESSAGE_TOPOLOGY)
    prepared = prepare_event(StudentEnrolled(student_id=20), message_id="event-20")

    with pytest.raises(RuntimeError, match="rollback"):
        with transaction.atomic():
            publish_prepared(prepared, queue="student.lifecycle")
            raise RuntimeError("rollback")

    assert transport.size("student.lifecycle") == 0


def test_django_publish_event_discards_rolled_back_savepoint_callback() -> None:
    transport = MemoryTransport()
    configure_transport(transport, topology=MESSAGE_TOPOLOGY)

    with transaction.atomic():
        publish_event(StudentEnrolled(student_id=3))
        with pytest.raises(RuntimeError, match="savepoint"):
            with transaction.atomic():
                publish_event(StudentEnrolled(student_id=4))
                raise RuntimeError("savepoint")
        publish_event(StudentEnrolled(student_id=5))

    assert _stored_student_ids(transport) == [3, 5]


def test_post_commit_transport_failure_is_observable_after_database_commit() -> None:
    transport = FailingTransport()
    configure_transport(transport, topology=MESSAGE_TOPOLOGY)
    with connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS pybus_commit_probe")
        cursor.execute(
            "CREATE TABLE pybus_commit_probe "
            "(id INTEGER PRIMARY KEY, value INTEGER NOT NULL)"
        )

    try:
        with pytest.raises(RuntimeError, match="transport unavailable"):
            with transaction.atomic():
                with connection.cursor() as cursor:
                    cursor.execute(
                        "INSERT INTO pybus_commit_probe (id, value) VALUES (1, 42)"
                    )
                publish_event(StudentEnrolled(student_id=6))

        with connection.cursor() as cursor:
            cursor.execute("SELECT value FROM pybus_commit_probe WHERE id = 1")
            assert cursor.fetchone() == (42,)
        assert transport.publish_attempts == 1
        assert transport.attempted_channels == ["student.lifecycle"]
    finally:
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE pybus_commit_probe")
