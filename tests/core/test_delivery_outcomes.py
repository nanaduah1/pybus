from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from pybus import (
    BusConfiguration,
    CommandDeliveryOutcome,
    CommandDeliveryStatus,
    ContinueProcessing,
    DeliveryObservationError,
    Pybus,
    WorkerAbortError,
    command,
    command_handler,
    event,
    event_handler,
)
from pybus.envelope import MessageEnvelope
from pybus.exceptions import IndeterminateDeliveryError, InvalidMessageDefinitionError
from pybus.listener import DEFAULT_FAILED_QUEUE_NAME, DEFAULT_QUEUE_NAME
from pybus.messages import CommandMessage
from pybus.queues import DEFAULT_SLOW_QUEUE_NAME, QueueTopology
from pybus.retries import RetryPolicy
from pybus.serializer import JsonSerializer
from pybus.transports.memory import MemoryTransport
from pybus.worker import Worker, WorkerHook


@command("billing.generate")
class GenerateBill:
    student_id: int


@command("billing.continue")
class ContinueBilling:
    student_id: int


@event("student.enrolled")
class StudentEnrolled:
    student_id: int


class SettlementFailureTransport(MemoryTransport):
    def __init__(self) -> None:
        super().__init__()
        self.fail_channels: set[str] = set()

    def publish(self, channel: str, message: bytes) -> None:
        if channel in self.fail_channels:
            raise ConnectionError(f"failed to publish to {channel}")
        super().publish(channel, message)

    def seed(self, channel: str, message: bytes) -> None:
        super().publish(channel, message)


def test_prepared_command_can_be_persisted_and_published_with_stable_identity() -> None:
    transport = MemoryTransport()
    bus = Pybus(transport)

    prepared = bus.prepare_command(
        GenerateBill(student_id=7),
        message_id="job-42",
        headers={"tenant_id": 3},
    )

    assert transport.size(DEFAULT_QUEUE_NAME) == 0
    restored = MessageEnvelope.from_dict(prepared.to_dict())
    published = bus.publish_prepared(restored)

    assert published is restored
    raw = transport.consume(DEFAULT_QUEUE_NAME)
    received = MessageEnvelope.from_dict(bus.serializer.loads(raw))
    assert received.to_dict() == prepared.to_dict()
    assert received.message_id == "job-42"
    assert received.headers == {"tenant_id": 3}


def test_publish_prepared_validates_identity_and_queue_before_transport_io() -> None:
    transport = MemoryTransport()
    topology = QueueTopology().declare_queue("billing.commands")
    bus = Pybus(transport, topology=topology)
    prepared = bus.prepare_command(GenerateBill(student_id=7), message_id="job-42")
    prepared.message_id = "invalid\nidentity"

    with pytest.raises(InvalidMessageDefinitionError, match="control characters"):
        bus.publish_prepared(prepared)
    with pytest.raises(InvalidMessageDefinitionError, match="not declared"):
        bus.publish_prepared(
            bus.prepare_command(GenerateBill(student_id=7)),
            queue="missing.queue",
        )

    assert transport.size(DEFAULT_QUEUE_NAME) == 0
    assert transport.size("missing.queue") == 0


@pytest.mark.parametrize(
    "message_id",
    ["", "   ", "job\n42", "job\x8542", "x" * 256, 42],
)
def test_publication_rejects_unsafe_caller_supplied_identity(message_id) -> None:
    bus = Pybus(MemoryTransport())

    with pytest.raises(InvalidMessageDefinitionError, match="message_id"):
        bus.send_command(GenerateBill(student_id=7), message_id=message_id)


def test_publication_copies_headers_and_requires_string_keys() -> None:
    bus = Pybus(MemoryTransport())
    headers = {"tenant_id": 3}

    prepared = bus.prepare_command(GenerateBill(student_id=7), headers=headers)
    headers["tenant_id"] = 4

    assert prepared.headers == {"tenant_id": 3}
    with pytest.raises(InvalidMessageDefinitionError, match="header keys"):
        bus.prepare_command(GenerateBill(student_id=7), headers={1: "invalid"})


@pytest.mark.parametrize(
    "reserved_header",
    ["retries", "last_attempt", "dead_lettered_from"],
)
def test_publication_rejects_framework_delivery_headers(reserved_header: str) -> None:
    transport = MemoryTransport()
    bus = Pybus(transport)

    with pytest.raises(InvalidMessageDefinitionError, match="framework-reserved"):
        bus.send_command(
            GenerateBill(student_id=7),
            headers={reserved_header: "forged"},
        )

    assert transport.size(DEFAULT_QUEUE_NAME) == 0


def test_legacy_message_metadata_is_merged_without_mutating_the_message() -> None:
    bus = Pybus(MemoryTransport())
    command_message = CommandMessage(
        message_type="billing.legacy",
        payload={"student_id": 7},
        headers={"tenant_id": 3, "source": "legacy"},
    )

    prepared = bus.prepare_command(
        command_message,
        message_id="job-42",
        headers={"source": "scheduler"},
    )

    assert prepared.message_id == "job-42"
    assert prepared.headers == {"tenant_id": 3, "source": "scheduler"}
    assert command_message.headers == {"tenant_id": 3, "source": "legacy"}


@pytest.mark.parametrize("operation", ["prepare_command", "send_command"])
def test_legacy_message_is_validated_before_preparation(operation: str) -> None:
    bus = Pybus(MemoryTransport())
    invalid = CommandMessage(message_type="", payload={})

    with pytest.raises(InvalidMessageDefinitionError, match="message_type"):
        getattr(bus, operation)(invalid)


def test_legacy_message_invalid_headers_raise_framework_validation_error() -> None:
    bus = Pybus(MemoryTransport())
    invalid = CommandMessage(message_type="billing.legacy", payload={}, headers=None)

    with pytest.raises(InvalidMessageDefinitionError, match="headers"):
        bus.prepare_command(invalid)


def test_command_observer_reports_started_then_succeeded() -> None:
    outcomes: list[CommandDeliveryOutcome] = []
    handled: list[GenerateBill] = []

    @command_handler(GenerateBill)
    def handle(command_message: GenerateBill) -> str:
        handled.append(command_message)
        return "generated"

    bus = Pybus(
        MemoryTransport(),
        handler_targets=[handle],
        command_delivery_observers=[outcomes.append],
    )
    envelope = bus.send_command(GenerateBill(student_id=7), message_id="job-42")

    assert bus.listen_once(DEFAULT_QUEUE_NAME) == "generated"

    assert handled == [GenerateBill(student_id=7)]
    assert [outcome.status for outcome in outcomes] == [
        CommandDeliveryStatus.STARTED,
        CommandDeliveryStatus.SUCCEEDED,
    ]
    assert all(outcome.message_id == envelope.message_id for outcome in outcomes)
    assert not hasattr(outcomes[-1], "payload")
    assert not hasattr(outcomes[-1], "headers")
    assert not hasattr(outcomes[-1], "exception")
    assert outcomes[-1] == CommandDeliveryOutcome(
        status=CommandDeliveryStatus.SUCCEEDED,
        message_id="job-42",
        message_type="billing.generate",
        version=1,
        source_queue=DEFAULT_QUEUE_NAME,
        destination_queue=None,
        retry_count=0,
        max_retries=10,
    )
    with pytest.raises(FrozenInstanceError):
        outcomes[-1].retry_count = 2


def test_command_observer_reports_continuation_after_republication() -> None:
    outcomes: list[CommandDeliveryOutcome] = []
    transport = MemoryTransport()

    @command_handler(ContinueBilling)
    def handle(command_message: ContinueBilling) -> ContinueProcessing:
        return ContinueProcessing(queue=DEFAULT_SLOW_QUEUE_NAME, delay=0.01)

    def observe(outcome: CommandDeliveryOutcome) -> None:
        if outcome.status == CommandDeliveryStatus.CONTINUED:
            assert transport.size(DEFAULT_SLOW_QUEUE_NAME) == 1
        outcomes.append(outcome)

    bus = Pybus(
        transport,
        topology=QueueTopology(),
        handler_targets=[handle],
        command_delivery_observers=[observe],
    )
    bus.listener._sleep_fn = lambda _: None
    bus.send_command(ContinueBilling(student_id=7), message_id="job-43")

    assert bus.listen_once(DEFAULT_QUEUE_NAME) is None

    assert [outcome.status for outcome in outcomes] == [
        CommandDeliveryStatus.STARTED,
        CommandDeliveryStatus.CONTINUED,
    ]
    assert outcomes[-1].destination_queue == DEFAULT_SLOW_QUEUE_NAME
    assert outcomes[-1].retry_count == 0


def test_command_observer_reports_each_retry_then_terminal_dead_letter() -> None:
    outcomes: list[CommandDeliveryOutcome] = []

    @command_handler(GenerateBill)
    def fail(command_message: GenerateBill) -> None:
        raise RuntimeError("billing unavailable")

    bus = Pybus(
        MemoryTransport(),
        handler_targets=[fail],
        command_delivery_observers=[outcomes.append],
    )
    bus.listener.retry_policy = RetryPolicy(max_retries=1)
    bus.send_command(GenerateBill(student_id=7), message_id="job-44")

    assert bus.listen_once(DEFAULT_QUEUE_NAME) is None
    assert bus.listen_once(DEFAULT_QUEUE_NAME) is None

    assert [outcome.status for outcome in outcomes] == [
        CommandDeliveryStatus.STARTED,
        CommandDeliveryStatus.RETRY_SCHEDULED,
        CommandDeliveryStatus.STARTED,
        CommandDeliveryStatus.DEAD_LETTERED,
    ]
    assert [outcome.retry_count for outcome in outcomes] == [0, 1, 1, 1]
    assert all(outcome.max_retries == 1 for outcome in outcomes)
    assert all(outcome.message_id == "job-44" for outcome in outcomes)
    assert outcomes[-1].destination_queue == DEFAULT_FAILED_QUEUE_NAME


def test_restored_retry_state_advances_without_changing_identity_or_payload() -> None:
    @command_handler(GenerateBill)
    def fail(command_message: GenerateBill) -> None:
        raise RuntimeError("billing unavailable")

    transport = MemoryTransport()
    bus = Pybus(transport, handler_targets=[fail])
    prepared = bus.prepare_command(GenerateBill(student_id=7), message_id="job-44b")
    restored_data = prepared.to_dict()
    restored_data["headers"] = {"retries": 4, "last_attempt": "framework-state"}
    restored = MessageEnvelope.from_dict(restored_data)
    bus.publish_prepared(restored)

    assert bus.listen_once(DEFAULT_QUEUE_NAME) is None

    raw_retry = transport.consume(DEFAULT_QUEUE_NAME)
    retry = MessageEnvelope.from_dict(bus.serializer.loads(raw_retry))
    assert retry.message_id == "job-44b"
    assert retry.payload == {"student_id": 7}
    assert retry.headers["retries"] == 5


def test_event_delivery_does_not_notify_command_observers() -> None:
    outcomes: list[CommandDeliveryOutcome] = []

    @event_handler(StudentEnrolled)
    def handle(event_message: StudentEnrolled) -> None:
        pass

    bus = Pybus(
        MemoryTransport(),
        handler_targets=[handle],
        command_delivery_observers=[outcomes.append],
    )
    bus.publish_event(StudentEnrolled(student_id=7))

    bus.listen_once(DEFAULT_QUEUE_NAME)

    assert outcomes == []


def test_started_observer_failure_aborts_before_handler_execution() -> None:
    handled: list[GenerateBill] = []
    observed: list[CommandDeliveryOutcome] = []

    @command_handler(GenerateBill)
    def handle(command_message: GenerateBill) -> None:
        handled.append(command_message)

    def fail(outcome: CommandDeliveryOutcome) -> None:
        raise RuntimeError("job store unavailable")

    bus = Pybus(
        MemoryTransport(),
        handler_targets=[handle],
        command_delivery_observers=[fail, observed.append],
    )
    first = bus.send_command(GenerateBill(student_id=7), message_id="job-first")
    second = bus.send_command(GenerateBill(student_id=8), message_id="job-second")

    with pytest.raises(
        DeliveryObservationError, match="unchanged command was restored"
    ):
        Worker(bus.listener, DEFAULT_QUEUE_NAME, error_delay=0).run()

    assert handled == []
    assert [outcome.status for outcome in observed] == [CommandDeliveryStatus.STARTED]
    assert bus.transport.size(DEFAULT_QUEUE_NAME) == 2
    queued = [
        MessageEnvelope.from_dict(
            bus.serializer.loads(bus.transport.consume(DEFAULT_QUEUE_NAME))
        )
        for _ in range(2)
    ]
    assert {envelope.message_id for envelope in queued} == {
        first.message_id,
        second.message_id,
    }
    restored = next(
        envelope for envelope in queued if envelope.message_id == first.message_id
    )
    assert restored.to_dict() == first.to_dict()


def test_started_observer_recovery_failure_is_indeterminate() -> None:
    handled: list[GenerateBill] = []
    transport = SettlementFailureTransport()

    @command_handler(GenerateBill)
    def handle(command_message: GenerateBill) -> None:
        handled.append(command_message)

    def fail(outcome: CommandDeliveryOutcome) -> None:
        raise RuntimeError("job store unavailable")

    bus = Pybus(
        transport,
        handler_targets=[handle],
        command_delivery_observers=[fail],
    )
    prepared = bus.prepare_command(GenerateBill(student_id=7), message_id="job-gated")
    transport.seed(DEFAULT_QUEUE_NAME, bus.serializer.dump(prepared))
    transport.fail_channels.add(DEFAULT_QUEUE_NAME)

    with pytest.raises(IndeterminateDeliveryError, match="command claim recovery"):
        Worker(bus.listener, DEFAULT_QUEUE_NAME, error_delay=0).run()

    assert handled == []
    assert transport.size(DEFAULT_QUEUE_NAME) == 0


def test_post_settlement_observer_failure_does_not_replay_handler(caplog) -> None:
    handled: list[GenerateBill] = []
    observed: list[CommandDeliveryOutcome] = []
    worker_errors: list[Exception] = []

    class RecordErrors(WorkerHook):
        def on_error(self, worker: Worker, exception: Exception) -> None:
            worker_errors.append(exception)

    @command_handler(GenerateBill)
    def handle(command_message: GenerateBill) -> None:
        handled.append(command_message)

    def fail_after_start(outcome: CommandDeliveryOutcome) -> None:
        if outcome.status == CommandDeliveryStatus.SUCCEEDED:
            raise RuntimeError("job store unavailable")

    bus = Pybus(
        MemoryTransport(),
        handler_targets=[handle],
        command_delivery_observers=[fail_after_start, observed.append],
    )
    bus.send_command(GenerateBill(student_id=7))
    bus.send_command(GenerateBill(student_id=8))

    with pytest.raises(DeliveryObservationError, match="observer failed"):
        Worker(
            bus.listener,
            DEFAULT_QUEUE_NAME,
            error_delay=0,
            hooks=[RecordErrors()],
        ).run()

    assert handled == [GenerateBill(student_id=7)]
    assert [outcome.status for outcome in observed] == [
        CommandDeliveryStatus.STARTED,
        CommandDeliveryStatus.SUCCEEDED,
    ]
    assert bus.transport.size(DEFAULT_QUEUE_NAME) == 1
    assert bus.transport.size(DEFAULT_FAILED_QUEUE_NAME) == 0
    assert len(worker_errors) == 1
    assert isinstance(worker_errors[0], DeliveryObservationError)
    assert "Command delivery observer failed" in caplog.text


def test_multiple_command_handlers_are_restored_without_execution() -> None:
    handled: list[str] = []
    transport = MemoryTransport()
    bus = Pybus(transport, command_delivery_observers=[lambda outcome: None])

    @command_handler(GenerateBill, allow_multiple=True)
    def first(command_message: GenerateBill) -> None:
        handled.append("first")

    @command_handler(GenerateBill, allow_multiple=True)
    def second(command_message: GenerateBill) -> None:
        handled.append("second")

    bus.dispatcher.registry.register(
        "command",
        GenerateBill.message_type,
        first,
        message_class=GenerateBill,
        allow_multiple=True,
    )
    bus.dispatcher.registry.register(
        "command",
        GenerateBill.message_type,
        second,
        message_class=GenerateBill,
        allow_multiple=True,
    )
    original = bus.send_command(GenerateBill(student_id=7), message_id="job-multiple")

    with pytest.raises(WorkerAbortError, match="exactly one handler"):
        bus.listen_once(DEFAULT_QUEUE_NAME)

    assert handled == []
    raw_restored = transport.consume(DEFAULT_QUEUE_NAME)
    restored = MessageEnvelope.from_dict(bus.serializer.loads(raw_restored))
    assert restored.to_dict() == original.to_dict()


def test_indeterminate_retry_publication_emits_no_final_outcome() -> None:
    outcomes: list[CommandDeliveryOutcome] = []
    transport = SettlementFailureTransport()

    @command_handler(GenerateBill)
    def fail(command_message: GenerateBill) -> None:
        raise RuntimeError("billing unavailable")

    bus = Pybus(
        transport,
        handler_targets=[fail],
        command_delivery_observers=[outcomes.append],
    )
    prepared = bus.prepare_command(GenerateBill(student_id=7), message_id="job-45")
    transport.seed(DEFAULT_QUEUE_NAME, JsonSerializer().dump(prepared))
    later = bus.prepare_command(GenerateBill(student_id=8), message_id="job-later")
    transport.seed(DEFAULT_QUEUE_NAME, JsonSerializer().dump(later))
    transport.fail_channels.add(DEFAULT_QUEUE_NAME)

    with pytest.raises(IndeterminateDeliveryError, match="retry requeue"):
        bus.listen_once(DEFAULT_QUEUE_NAME)

    assert [outcome.status for outcome in outcomes] == [CommandDeliveryStatus.STARTED]
    assert transport.size(DEFAULT_QUEUE_NAME) == 1
    transport.fail_channels.clear()
    raw_later = transport.consume(DEFAULT_QUEUE_NAME)
    assert MessageEnvelope.from_dict(bus.serializer.loads(raw_later)).message_id == (
        "job-later"
    )


def test_configuration_passes_validated_command_observers_to_each_bus() -> None:
    def observer(outcome: CommandDeliveryOutcome) -> None:
        pass

    configuration = BusConfiguration(
        transport_factory=MemoryTransport,
        command_delivery_observers=[observer],
    )

    first = configuration.create()
    second = configuration.create()

    assert configuration.command_delivery_observers == (observer,)
    assert first.command_delivery_observers == (observer,)
    assert second.command_delivery_observers == (observer,)
    with pytest.raises(TypeError, match="command_delivery_observers"):
        BusConfiguration(
            transport_factory=MemoryTransport,
            command_delivery_observers=[None],
        )
