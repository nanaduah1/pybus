from __future__ import annotations

import threading
import time

import pytest

from pybus.bus import Pybus
from pybus.dispatcher import Dispatcher
from pybus.envelope import MessageEnvelope
from pybus.exceptions import MessageTimeoutError
from pybus.listener import DEFAULT_QUEUE_NAME, Listener
from pybus.messages import RequestMessage, ResponseMessage
from pybus.registry import Registry
from pybus.request_response import DEFAULT_REPLY_QUEUE
from pybus.serializer import JsonSerializer
from pybus.transports.memory import MemoryTransport


def test_listener_publishes_response_for_request_handlers() -> None:
    registry = Registry()
    transport = MemoryTransport()
    serializer = JsonSerializer()
    dispatcher = Dispatcher(registry=registry, serializer=serializer)
    listener = Listener(
        transport=transport, dispatcher=dispatcher, serializer=serializer
    )

    def handle_invoice_request(message: RequestMessage) -> ResponseMessage:
        return ResponseMessage(
            message_type="billing.get_invoice.response",
            payload={"invoice_id": message.payload["invoice_id"], "total": 100},
        )

    registry.register("request", "billing.get_invoice", handle_invoice_request)

    request_envelope = RequestMessage(
        message_type="billing.get_invoice",
        payload={"invoice_id": "INV-1"},
    ).to_envelope(message_id="req-1")

    transport.publish(DEFAULT_QUEUE_NAME, serializer.dump(request_envelope))

    result = listener.listen_once(DEFAULT_QUEUE_NAME)

    assert isinstance(result, ResponseMessage)
    assert transport.size(DEFAULT_REPLY_QUEUE) == 1

    response_envelope = MessageEnvelope.from_dict(
        serializer.loads(transport.consume(DEFAULT_REPLY_QUEUE))
    )

    assert response_envelope.correlation_id == request_envelope.correlation_id
    assert response_envelope.causation_id == request_envelope.message_id
    assert response_envelope.reply_to == DEFAULT_REPLY_QUEUE


def test_bus_request_waits_for_matching_response() -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    bus = Pybus(transport=transport, serializer=serializer)
    reply_queue = "billing.replies"

    request = RequestMessage(
        message_type="billing.get_invoice",
        payload={"invoice_id": "INV-1"},
        correlation_id="corr-1",
        reply_to=reply_queue,
    )

    def handle_invoice_request(message: RequestMessage) -> ResponseMessage:
        return ResponseMessage(
            message_type="billing.get_invoice.response",
            payload={"invoice_id": message.payload["invoice_id"], "total": 100},
        )

    bus.dispatcher.registry.register(
        "request", "billing.get_invoice", handle_invoice_request
    )

    def run_worker() -> None:
        time.sleep(0.01)
        bus.listen_once(DEFAULT_QUEUE_NAME)

    worker = threading.Thread(target=run_worker)
    worker.start()

    received = bus.request(request, timeout=1)
    worker.join(timeout=1)

    assert received.message_type == "billing.get_invoice.response"
    assert received.payload == {"invoice_id": "INV-1", "total": 100}
    assert received.correlation_id == "corr-1"
    assert received.causation_id is not None
    assert received.reply_to == reply_queue
    assert transport.size(DEFAULT_QUEUE_NAME) == 0
    assert transport.size(reply_queue) == 0
    assert worker.is_alive() is False


def test_bus_request_times_out_when_no_response_arrives() -> None:
    bus = Pybus(transport=MemoryTransport(), serializer=JsonSerializer())

    with pytest.raises(MessageTimeoutError):
        bus.request(
            RequestMessage(
                message_type="billing.get_invoice",
                payload={"invoice_id": "INV-1"},
                correlation_id="corr-missing",
            ),
            timeout=0,
        )


def test_bus_buffers_non_matching_responses_for_later_callers() -> None:
    transport = MemoryTransport()
    serializer = JsonSerializer()
    bus = Pybus(transport=transport, serializer=serializer)
    reply_queue = "billing.replies"

    first_response = ResponseMessage(
        message_type="billing.get_invoice.response",
        payload={"invoice_id": "INV-2", "total": 200},
        correlation_id="corr-2",
        reply_to=reply_queue,
    ).to_envelope(message_id="resp-2")
    second_response = ResponseMessage(
        message_type="billing.get_invoice.response",
        payload={"invoice_id": "INV-1", "total": 100},
        correlation_id="corr-1",
        reply_to=reply_queue,
    ).to_envelope(message_id="resp-1")

    transport.publish(reply_queue, serializer.dump(first_response))
    transport.publish(reply_queue, serializer.dump(second_response))

    received = bus._await_response("corr-1", timeout=1, reply_queue=reply_queue)
    buffered = bus.coordinator.take_response("corr-2")

    assert received.correlation_id == "corr-1"
    assert buffered is not None
    assert buffered.correlation_id == "corr-2"
    assert transport.size(reply_queue) == 0


def test_bus_request_honors_subsecond_timeout() -> None:
    bus = Pybus(transport=MemoryTransport(), serializer=JsonSerializer())

    started_at = time.monotonic()
    with pytest.raises(MessageTimeoutError):
        bus.request(
            RequestMessage(
                message_type="billing.get_invoice",
                payload={"invoice_id": "INV-1"},
                correlation_id="corr-short-timeout",
            ),
            timeout=0.1,
        )
    elapsed = time.monotonic() - started_at

    assert elapsed < 0.5
