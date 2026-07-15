from __future__ import annotations

import pytest

import pybus
from pybus.bus import configure_transport, get_bus
from pybus.dispatcher import Dispatcher
from pybus.handlers import (
    MAX_CONTINUATION_DELAY_SECONDS,
    ContinueProcessing,
    batched_event_handler,
    bind_handlers,
    command_handler,
    event_handler,
    register_handlers,
    request_handler,
)
from pybus.messages import CommandMessage, EventMessage, RequestMessage
from pybus.registry import Registry
from pybus.serializer import JsonSerializer
from pybus.transports.memory import MemoryTransport


def test_handler_helpers_are_exposed_from_package_root() -> None:
    assert pybus.event_handler is event_handler
    assert pybus.batched_event_handler is batched_event_handler
    assert pybus.command_handler is command_handler
    assert pybus.request_handler is request_handler
    assert pybus.register_handlers is register_handlers
    assert pybus.bind_handlers is bind_handlers
    assert pybus.ContinueProcessing is ContinueProcessing
    assert pybus.MAX_CONTINUATION_DELAY_SECONDS == MAX_CONTINUATION_DELAY_SECONDS


def test_continuation_defaults_preserve_immediate_same_queue_behavior() -> None:
    continuation = ContinueProcessing()

    assert continuation.queue is None
    assert continuation.delay == 0


@pytest.mark.parametrize("delay", [0, 1, 0.05, MAX_CONTINUATION_DELAY_SECONDS])
def test_continuation_accepts_bounded_finite_delays(delay: float) -> None:
    assert ContinueProcessing("pybus.jobs.slow", delay).delay == delay


@pytest.mark.parametrize(
    "delay",
    [
        -1,
        True,
        False,
        "1",
        None,
        float("nan"),
        float("inf"),
        float("-inf"),
        MAX_CONTINUATION_DELAY_SECONDS + 0.01,
        10**1000,
    ],
)
def test_continuation_rejects_unsafe_delays(delay: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        ContinueProcessing(delay=delay)  # type: ignore[arg-type]


@pytest.mark.parametrize("queue", ["", "   ", 7, False])
def test_continuation_rejects_invalid_queue_overrides(queue: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        ContinueProcessing(queue=queue)  # type: ignore[arg-type]


def test_event_handlers_register_and_fan_out_from_a_handler_object() -> None:
    registry = Registry()
    dispatcher = Dispatcher(registry=registry, serializer=JsonSerializer())
    received: list[tuple[str, str]] = []

    class StudentHandlers:
        @event_handler("student.enrolled")
        def handle_first(self, message: EventMessage) -> str:
            received.append(("first", message.payload["student_id"]))
            return "first"

        @event_handler("student.enrolled")
        def handle_second(self, message: EventMessage) -> str:
            received.append(("second", message.payload["student_id"]))
            return "second"

    registered = register_handlers(registry, StudentHandlers())

    result = dispatcher.dispatch(
        EventMessage(
            message_type="student.enrolled",
            payload={"student_id": "S-1"},
        ).to_envelope(message_id="msg-1")
    )

    assert len(registered) == 2
    assert result == ["first", "second"]
    assert received == [("first", "S-1"), ("second", "S-1")]


def test_command_and_request_handlers_default_to_single_handler_semantics() -> None:
    registry = Registry()
    dispatcher = Dispatcher(registry=registry, serializer=JsonSerializer())

    class BillingHandlers:
        @command_handler("billing.generate_student_bill")
        def handle_generate_student_bill(self, message: CommandMessage) -> str:
            return f"bill:{message.payload['student_id']}"

        @request_handler("billing.get_invoice")
        def handle_get_invoice(self, message: RequestMessage) -> str:
            return f"invoice:{message.payload['invoice_id']}"

    registered = register_handlers(registry, BillingHandlers())

    command_result = dispatcher.dispatch(
        CommandMessage(
            message_type="billing.generate_student_bill",
            payload={"student_id": "S-2"},
        ).to_envelope(message_id="msg-2")
    )
    request_result = dispatcher.dispatch(
        RequestMessage(
            message_type="billing.get_invoice",
            payload={"invoice_id": "INV-1"},
        ).to_envelope(message_id="msg-3")
    )

    assert len(registered) == 2
    assert command_result == "bill:S-2"
    assert request_result == "invoice:INV-1"


def test_duplicate_command_registration_is_rejected() -> None:
    registry = Registry()

    class DuplicateCommandHandlers:
        @command_handler("billing.generate_student_bill")
        def handle_first(self, message: CommandMessage) -> str:
            return "first"

        @command_handler("billing.generate_student_bill")
        def handle_second(self, message: CommandMessage) -> str:
            return "second"

    with pytest.raises(ValueError):
        register_handlers(registry, DuplicateCommandHandlers())


def test_handlers_can_bind_to_message_classes() -> None:
    registry = Registry()
    dispatcher = Dispatcher(registry=registry, serializer=JsonSerializer())
    received: list[tuple[str, str]] = []

    class PaymentConfirmedEvent(EventMessage):
        message_type = "billing.payment_confirmed"

    class GenerateStudentBill(CommandMessage):
        message_type = "billing.generate_student_bill"

    class BillingHandlers:
        @event_handler(PaymentConfirmedEvent)
        def handle_payment_confirmed(self, message: EventMessage) -> str:
            received.append(("event", message.payload["payment_id"]))
            return "payment-confirmed"

        @command_handler(GenerateStudentBill)
        def handle_generate_student_bill(self, message: CommandMessage) -> str:
            received.append(("command", message.payload["student_id"]))
            return "bill-generated"

    registered = register_handlers(registry, BillingHandlers())

    event_result = dispatcher.dispatch(
        EventMessage(
            message_type="billing.payment_confirmed",
            payload={"payment_id": "PAY-1"},
        ).to_envelope(message_id="msg-4")
    )
    command_result = dispatcher.dispatch(
        CommandMessage(
            message_type="billing.generate_student_bill",
            payload={"student_id": "S-3"},
        ).to_envelope(message_id="msg-5")
    )

    assert len(registered) == 2
    assert event_result == "payment-confirmed"
    assert command_result == "bill-generated"
    assert received == [("event", "PAY-1"), ("command", "S-3")]


def test_bind_handlers_uses_default_bus_registry_when_registry_is_omitted() -> None:
    bus = configure_transport(MemoryTransport())
    received: list[str] = []

    class StudentHandlers:
        @event_handler("student.enrolled")
        def handle_enrolled(self, message: EventMessage) -> str:
            received.append(message.payload["student_id"])
            return "handled"

    registered = bind_handlers(StudentHandlers())
    result = get_bus().dispatcher.dispatch(
        EventMessage(
            message_type="student.enrolled",
            payload={"student_id": "S-4"},
        ).to_envelope(message_id="msg-6")
    )

    assert bus is get_bus()
    assert len(registered) == 1
    assert result == "handled"
    assert received == ["S-4"]


def test_configure_transport_can_wire_handler_targets_without_manual_binding() -> None:
    received: list[str] = []

    class StudentHandlers:
        @event_handler("student.enrolled")
        def handle_enrolled(self, message: EventMessage) -> str:
            received.append(message.payload["student_id"])
            return "handled-by-bootstrap"

    bus = configure_transport(
        MemoryTransport(),
        handler_targets=[StudentHandlers()],
    )

    result = bus.dispatcher.dispatch(
        EventMessage(
            message_type="student.enrolled",
            payload={"student_id": "S-5"},
        ).to_envelope(message_id="msg-7")
    )

    assert bus is get_bus()
    assert result == "handled-by-bootstrap"
    assert received == ["S-5"]


def test_batched_event_handler_marks_handler_metadata() -> None:
    class StudentHandlers:
        @batched_event_handler("student.enrolled", batch_size=25, max_wait=30)
        def handle_enrolled_batch(self, messages: list[EventMessage]) -> None:
            return None

    handler = StudentHandlers().handle_enrolled_batch
    spec = getattr(handler.__func__, "__pybus_handler_spec__")

    assert spec.message_kind == "event"
    assert spec.message_type == "student.enrolled"
    assert spec.is_batched is True
    assert spec.batch_size == 25
    assert spec.max_wait == 30


@pytest.mark.parametrize(
    "options",
    [
        {"batch_size": 0},
        {"batch_size": -1},
        {"max_wait": -1},
        {"retry_limit": -1},
    ],
)
def test_batched_event_handler_rejects_invalid_delivery_options(options) -> None:
    with pytest.raises(ValueError):
        batched_event_handler("student.enrolled", **options)


def test_registry_rejects_multiple_batched_handlers_for_one_message_type() -> None:
    registry = Registry()

    @batched_event_handler("student.enrolled")
    def first(messages: list[EventMessage]) -> None:
        return None

    @batched_event_handler("student.enrolled")
    def second(messages: list[EventMessage]) -> None:
        return None

    registry.register("event", "student.enrolled", first, allow_multiple=True)

    with pytest.raises(ValueError, match="batched handler already registered"):
        registry.register("event", "student.enrolled", second, allow_multiple=True)
