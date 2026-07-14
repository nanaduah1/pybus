from dataclasses import FrozenInstanceError, dataclass, is_dataclass

import pytest

import pybus
from pybus import (
    Pybus,
    batched_event_handler,
    command,
    command_handler,
    event,
    event_handler,
    publish_event,
    send_command,
)
from pybus.envelope import MessageEnvelope
from pybus.exceptions import InvalidMessageDefinitionError
from pybus.queues import QueueTopology
from pybus.transports.memory import MemoryTransport


@event("student.enrolled")
class StudentEnrolled:
    student_id: int
    school_id: int


@command("billing.generate")
class GenerateBill:
    student_id: int


@event("student.archived", queue="student.lifecycle")
class StudentArchived:
    student_id: int


@command("billing.collect", queue="billing.commands")
class CollectBill:
    student_id: int


MESSAGE_TOPOLOGY = QueueTopology().declare_queues(
    "student.lifecycle", "billing.commands", "student.priority"
)


def test_message_decorators_create_immutable_slotted_typed_messages() -> None:
    message = StudentEnrolled(student_id=7, school_id=3)

    assert is_dataclass(message)
    assert message.student_id == 7
    assert message.school_id == 3
    assert StudentEnrolled.message_kind == "event"
    assert StudentEnrolled.message_type == "student.enrolled"
    assert StudentEnrolled.version == 1
    assert not hasattr(message, "__dict__")
    with pytest.raises(FrozenInstanceError):
        message.student_id = 8


def test_publish_event_builds_the_envelope_from_the_typed_event() -> None:
    transport = MemoryTransport()
    bus = Pybus(transport, topology=MESSAGE_TOPOLOGY)
    event_message = StudentEnrolled(student_id=7, school_id=3)

    envelope = bus.publish_event(event_message)

    assert envelope.message_type == "student.enrolled"
    assert envelope.message_kind == "event"
    assert envelope.version == 1
    assert envelope.payload == {"school_id": 3, "student_id": 7}
    raw = transport.consume(bus.topology.default_queue)
    assert raw is not None
    stored = MessageEnvelope.from_dict(bus.serializer.loads(raw))
    assert stored.payload == {"school_id": 3, "student_id": 7}
    assert "__pybus_codec__" not in raw.decode()


def test_class_bound_handler_receives_the_typed_event_directly() -> None:
    received: list[StudentEnrolled] = []

    @event_handler(StudentEnrolled)
    def handle(message: StudentEnrolled) -> None:
        received.append(message)

    bus = Pybus(MemoryTransport(), handler_targets=[handle])
    bus.publish_event(StudentEnrolled(student_id=9, school_id=4))

    bus.create_worker(error_delay=0).run(max_iterations=1)

    assert received == [StudentEnrolled(student_id=9, school_id=4)]
    assert type(received[0]) is StudentEnrolled


def test_typed_command_uses_the_same_object_level_contract() -> None:
    received: list[GenerateBill] = []

    @command_handler(GenerateBill)
    def handle(command_message: GenerateBill) -> None:
        received.append(command_message)

    bus = Pybus(MemoryTransport(), handler_targets=[handle])

    envelope = bus.send_command(GenerateBill(student_id=11))
    bus.create_worker(error_delay=0).run(max_iterations=1)

    assert envelope.message_type == "billing.generate"
    assert envelope.payload == {"student_id": 11}
    assert received == [GenerateBill(student_id=11)]


@pytest.mark.parametrize(
    ("message", "publish", "declared_queue"),
    [
        (StudentArchived(student_id=12), "publish_event", "student.lifecycle"),
        (CollectBill(student_id=12), "send_command", "billing.commands"),
    ],
)
def test_decorator_queue_is_the_default_publication_route(
    message, publish, declared_queue
) -> None:
    transport = MemoryTransport()
    bus = Pybus(transport, topology=MESSAGE_TOPOLOGY)

    getattr(bus, publish)(message)

    assert transport.size(declared_queue) == 1
    assert transport.size(bus.topology.default_queue) == 0


def test_worker_can_consume_from_a_declared_message_queue() -> None:
    received: list[StudentArchived] = []

    @event_handler(StudentArchived)
    def handle(message: StudentArchived) -> None:
        received.append(message)

    bus = Pybus(MemoryTransport(), topology=MESSAGE_TOPOLOGY, handler_targets=[handle])
    bus.publish_event(StudentArchived(student_id=12))

    bus.create_worker("student.lifecycle", error_delay=0).run(max_iterations=1)

    assert received == [StudentArchived(student_id=12)]


def test_call_site_queue_overrides_the_decorator_queue() -> None:
    transport = MemoryTransport()
    bus = Pybus(transport, topology=MESSAGE_TOPOLOGY)

    envelope = bus.publish_event(
        StudentArchived(student_id=12), queue="student.priority"
    )

    assert transport.size("student.priority") == 1
    assert transport.size("student.lifecycle") == 0
    assert "queue" not in envelope.to_dict()
    assert "queue" not in envelope.payload


def test_typed_message_uses_a_custom_bus_default_as_the_last_precedence_tier() -> None:
    transport = MemoryTransport()
    topology = QueueTopology(default_queue="application.jobs")
    bus = Pybus(transport, topology=topology)

    bus.publish_event(StudentEnrolled(student_id=12, school_id=4))

    assert transport.size("application.jobs") == 1


def test_invalid_bus_default_is_rejected_before_transport_write() -> None:
    transport = MemoryTransport()
    bus = Pybus(transport, topology=QueueTopology(default_queue="   "))

    with pytest.raises(InvalidMessageDefinitionError, match="queue"):
        bus.publish_event(StudentEnrolled(student_id=12, school_id=4))

    assert transport.size("   ") == 0


def test_declared_queue_must_exist_in_the_bus_topology() -> None:
    transport = MemoryTransport()
    bus = Pybus(transport)

    with pytest.raises(InvalidMessageDefinitionError, match="topology"):
        bus.publish_event(StudentArchived(student_id=12))

    assert all(transport.size(queue) == 0 for queue in bus.topology.queues)


@pytest.mark.parametrize("invalid_queue", ["", "   ", 7, True])
def test_call_site_queue_is_validated_before_transport_write(invalid_queue) -> None:
    transport = MemoryTransport()
    bus = Pybus(transport, topology=MESSAGE_TOPOLOGY)

    with pytest.raises(InvalidMessageDefinitionError, match="queue"):
        bus.publish_event(StudentArchived(student_id=12), queue=invalid_queue)

    assert all(transport.size(queue) == 0 for queue in bus.topology.queues)


def test_mutated_decorator_queue_is_validated_before_transport_write(
    monkeypatch,
) -> None:
    transport = MemoryTransport()
    bus = Pybus(transport, topology=MESSAGE_TOPOLOGY)
    monkeypatch.setattr(StudentArchived, "__pybus_default_queue__", "   ")

    with pytest.raises(InvalidMessageDefinitionError, match="queue"):
        bus.publish_event(StudentArchived(student_id=12))

    assert all(transport.size(queue) == 0 for queue in bus.topology.queues)


def test_module_publish_functions_keep_the_same_typed_inputs() -> None:
    transport = MemoryTransport()
    pybus.configure_transport(transport)

    event_envelope = publish_event(StudentEnrolled(student_id=13, school_id=5))
    command_envelope = send_command(GenerateBill(student_id=13))

    assert event_envelope.message_type == "student.enrolled"
    assert command_envelope.message_type == "billing.generate"


def test_wrong_message_kind_is_rejected_before_transport_write() -> None:
    transport = MemoryTransport()
    bus = Pybus(transport)

    with pytest.raises(InvalidMessageDefinitionError, match="Expected an event"):
        bus.publish_event(GenerateBill(student_id=1))

    assert transport.size(bus.topology.default_queue) == 0


def test_decorators_reject_invalid_contracts() -> None:
    with pytest.raises(InvalidMessageDefinitionError, match="message_type"):

        @event("")
        class MissingType:
            value: int

    with pytest.raises(InvalidMessageDefinitionError, match="version"):

        @event("bad.version", version=0)
        class BadVersion:
            value: int

    for invalid_queue in ("", "   ", 7, True):
        with pytest.raises(InvalidMessageDefinitionError, match="queue"):

            @event("bad.queue", queue=invalid_queue)
            class BadQueue:
                value: int

    @dataclass
    class MutableDataclass:
        value: int

    with pytest.raises(InvalidMessageDefinitionError, match="owns the dataclass"):
        event("mutable.event")(MutableDataclass)


def test_duplicate_typed_classes_for_one_route_are_rejected() -> None:
    @event("student.enrolled")
    class ConflictingStudentEnrolled:
        student_id: int

    @event_handler(StudentEnrolled)
    def first(message: StudentEnrolled) -> None:
        return None

    @event_handler(ConflictingStudentEnrolled)
    def second(message: ConflictingStudentEnrolled) -> None:
        return None

    with pytest.raises(ValueError, match="different typed message classes"):
        Pybus(MemoryTransport(), handler_targets=[first, second])


def test_typed_and_string_bound_handlers_cannot_share_one_route() -> None:
    @event_handler("student.enrolled")
    def legacy(message) -> None:
        return None

    @event_handler(StudentEnrolled)
    def typed(message: StudentEnrolled) -> None:
        return None

    with pytest.raises(ValueError, match="Cannot mix typed and string-bound"):
        Pybus(MemoryTransport(), handler_targets=[legacy, typed])


def test_publisher_rejects_conflicting_or_inherited_typed_classes() -> None:
    @event("student.enrolled")
    class ConflictingStudentEnrolled:
        student_id: int
        school_id: int

    class InheritedStudentEnrolled(StudentEnrolled):
        pass

    class ForgedStudentEnrolled:
        __pybus_typed_message__ = True
        message_kind = "event"
        message_type = "student.enrolled"
        version = 1

    @event_handler(StudentEnrolled)
    def handle(message: StudentEnrolled) -> None:
        return None

    transport = MemoryTransport()
    bus = Pybus(transport, handler_targets=[handle])

    with pytest.raises(InvalidMessageDefinitionError, match="Expected StudentEnrolled"):
        bus.publish_event(ConflictingStudentEnrolled(student_id=1, school_id=2))
    with pytest.raises(
        InvalidMessageDefinitionError,
        match="not a declared pybus message",
    ):
        bus.publish_event(InheritedStudentEnrolled(student_id=1, school_id=2))
    with pytest.raises(
        InvalidMessageDefinitionError,
        match="not a declared pybus message",
    ):
        bus.publish_event(ForgedStudentEnrolled())

    assert transport.size(bus.topology.default_queue) == 0


def test_domain_post_init_validation_applies_locally_and_after_decode() -> None:
    @event("student.validated")
    class ValidatedStudent:
        student_id: int

        def __post_init__(self) -> None:
            if type(self.student_id) is not int or self.student_id <= 0:
                raise ValueError("student_id must be a positive integer")

    with pytest.raises(ValueError, match="positive integer"):
        ValidatedStudent(student_id=True)

    handled: list[ValidatedStudent] = []

    @event_handler(ValidatedStudent, retry_limit=0)
    def handle(message: ValidatedStudent) -> None:
        handled.append(message)

    transport = MemoryTransport()
    bus = Pybus(transport, handler_targets=[handle])
    malformed = MessageEnvelope.create(
        message_type="student.validated",
        message_kind="event",
        version=1,
        payload={"student_id": True},
    )
    transport.publish(bus.topology.default_queue, bus.serializer.dump(malformed))

    bus.create_worker(error_delay=0).run(max_iterations=11)

    assert handled == []
    assert transport.size(bus.topology.dead_letter_queue) == 1


def test_malformed_typed_payload_dead_letters_without_calling_handler() -> None:
    handled: list[StudentEnrolled] = []

    @event_handler(StudentEnrolled, retry_limit=0)
    def handle(message: StudentEnrolled) -> None:
        handled.append(message)

    transport = MemoryTransport()
    bus = Pybus(transport, handler_targets=[handle])
    malformed = MessageEnvelope.create(
        message_type="student.enrolled",
        message_kind="event",
        version=1,
        payload={"student_id": 1, "unexpected": 2},
    )
    transport.publish(bus.topology.default_queue, bus.serializer.dump(malformed))

    bus.create_worker(error_delay=0).run(max_iterations=11)

    assert handled == []
    assert transport.size(bus.topology.dead_letter_queue) == 1


def test_unsupported_typed_version_isolated_while_later_valid_work_runs() -> None:
    handled: list[StudentEnrolled] = []

    @event_handler(StudentEnrolled)
    def handle(message: StudentEnrolled) -> None:
        handled.append(message)

    transport = MemoryTransport()
    bus = Pybus(transport, handler_targets=[handle])
    unsupported = MessageEnvelope.create(
        message_type="student.enrolled",
        message_kind="event",
        version=2,
        payload={"student_id": 1, "school_id": 2},
    )
    transport.publish(bus.topology.default_queue, bus.serializer.dump(unsupported))
    bus.publish_event(StudentEnrolled(student_id=2, school_id=3))

    bus.create_worker(error_delay=0).run(max_iterations=12)

    assert handled == [StudentEnrolled(student_id=2, school_id=3)]
    assert transport.size(bus.topology.default_queue) == 0
    assert transport.size(bus.topology.dead_letter_queue) == 1


def test_unknown_route_isolated_while_later_valid_work_runs() -> None:
    handled: list[StudentEnrolled] = []

    @event_handler(StudentEnrolled)
    def handle(message: StudentEnrolled) -> None:
        handled.append(message)

    transport = MemoryTransport()
    bus = Pybus(transport, handler_targets=[handle])
    unknown = MessageEnvelope.create(
        message_type="student.unknown",
        message_kind="event",
        version=1,
        payload={"student_id": 1},
    )
    transport.publish(bus.topology.default_queue, bus.serializer.dump(unknown))
    bus.publish_event(StudentEnrolled(student_id=3, school_id=4))

    bus.create_worker(error_delay=0).run(max_iterations=2)

    assert handled == [StudentEnrolled(student_id=3, school_id=4)]
    assert transport.size(bus.topology.default_queue) == 0
    assert transport.size(bus.topology.dead_letter_queue) == 1


def test_typed_retry_preserves_domain_payload_and_reconstructs_the_event() -> None:
    attempts: list[StudentEnrolled] = []

    @event_handler(StudentEnrolled, retry_limit=1)
    def fail_once(message: StudentEnrolled) -> None:
        attempts.append(message)
        if len(attempts) == 1:
            raise RuntimeError("transient")

    transport = MemoryTransport()
    bus = Pybus(transport, handler_targets=[fail_once])
    original = bus.publish_event(StudentEnrolled(student_id=17, school_id=6))

    bus.create_worker(error_delay=0).run(max_iterations=1)

    raw_retry = transport.consume(bus.topology.default_queue)
    retry = MessageEnvelope.from_dict(bus.serializer.loads(raw_retry))
    assert retry.message_id == original.message_id
    assert retry.payload == {"school_id": 6, "student_id": 17}
    assert retry.headers["retries"] == 1
    assert "last_attempt" in retry.headers
    transport.publish(bus.topology.default_queue, raw_retry)

    bus.create_worker(error_delay=0).run(max_iterations=1)

    assert attempts == [
        StudentEnrolled(student_id=17, school_id=6),
        StudentEnrolled(student_id=17, school_id=6),
    ]
    assert transport.size(bus.topology.default_queue) == 0
    assert transport.size(bus.topology.dead_letter_queue) == 0
    assert original.payload == {"school_id": 6, "student_id": 17}


def test_typed_domain_fields_named_like_retry_metadata_are_not_modified() -> None:
    @event("audit.retry_named")
    class RetryNamedEvent:
        retries: int
        last_attempt: str

    @event_handler(RetryNamedEvent, retry_limit=1)
    def fail(message: RetryNamedEvent) -> None:
        raise RuntimeError("forced")

    transport = MemoryTransport()
    bus = Pybus(transport, handler_targets=[fail])
    bus.publish_event(RetryNamedEvent(retries=99, last_attempt="domain-value"))

    bus.create_worker(error_delay=0).run(max_iterations=1)

    raw_retry = transport.consume(bus.topology.default_queue)
    retry = MessageEnvelope.from_dict(bus.serializer.loads(raw_retry))
    assert retry.payload == {"retries": 99, "last_attempt": "domain-value"}
    assert retry.headers["retries"] == 1


def test_typed_retry_exhaustion_dead_letters_one_unchanged_payload() -> None:
    @event("student.rejected")
    class StudentRejected:
        student_id: int

    @event_handler(StudentRejected, retry_limit=1)
    def always_fail(message: StudentRejected) -> None:
        raise RuntimeError("forced")

    transport = MemoryTransport()
    bus = Pybus(transport, handler_targets=[always_fail])
    original = bus.publish_event(StudentRejected(student_id=23))

    bus.create_worker(error_delay=0).run(max_iterations=2)

    assert transport.size(bus.topology.default_queue) == 0
    assert transport.size(bus.topology.dead_letter_queue) == 1
    raw_failed = transport.consume(bus.topology.dead_letter_queue)
    failed = MessageEnvelope.from_dict(bus.serializer.loads(raw_failed))
    assert failed.message_id == original.message_id
    assert failed.payload == {"student_id": 23}
    assert failed.headers["retries"] == 1
    assert failed.headers["dead_lettered_from"] == bus.topology.default_queue


def test_typed_batched_retry_reconstructs_and_dead_letters_unchanged() -> None:
    @event("audit.typed_batch")
    class AuditEntry:
        entry_id: int

    attempts: list[list[AuditEntry]] = []

    @batched_event_handler(AuditEntry, batch_size=1, max_wait=0, retry_limit=1)
    def fail_batch(messages: list[AuditEntry]) -> None:
        attempts.append(messages)
        raise RuntimeError("forced")

    transport = MemoryTransport()
    bus = Pybus(transport, handler_targets=[fail_batch])
    original = bus.publish_event(AuditEntry(entry_id=31))

    bus.create_worker(error_delay=0).run(max_iterations=2)

    assert attempts == [[AuditEntry(entry_id=31)], [AuditEntry(entry_id=31)]]
    assert all(type(attempt[0]) is AuditEntry for attempt in attempts)
    assert transport.size("batched:audit.typed_batch") == 0
    raw_failed = transport.consume(bus.topology.dead_letter_queue)
    failed = MessageEnvelope.from_dict(bus.serializer.loads(raw_failed))
    assert failed.message_id == original.message_id
    assert failed.payload == {"entry_id": 31}
    assert failed.headers["retries"] == 1
    assert failed.headers["dead_lettered_from"] == "batched:audit.typed_batch"
