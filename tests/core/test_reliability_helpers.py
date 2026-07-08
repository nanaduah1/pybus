from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pybus.batching import BatchBuffer, batched_buffer_key
from pybus.exceptions import DuplicateMessageError, MessageTimeoutError
from pybus.inbox import InMemoryInboxStore
from pybus.outbox import InMemoryOutboxStore
from pybus.request_response import RequestResponseCoordinator
from pybus.messages import RequestMessage, ResponseMessage
from pybus.retries import DEFAULT_RETRY_DELAY, DEFAULT_RETRY_LIMIT, RetryPolicy


def test_retry_defaults_and_next_delay() -> None:
    policy = RetryPolicy()

    assert DEFAULT_RETRY_LIMIT == 10
    assert DEFAULT_RETRY_DELAY == 0
    assert policy.should_retry(9) is True
    assert policy.should_retry(10) is False
    assert policy.next_delay(2) == 0


def test_batch_buffer_flushes_on_size_and_tracks_key() -> None:
    buffer = BatchBuffer(event_type="student.enrolled", batch_size=2, max_wait=10)

    assert batched_buffer_key("student.enrolled") == "batched:student.enrolled"
    assert buffer.add("first", now=datetime(2026, 7, 7, tzinfo=timezone.utc)) is False
    assert buffer.add("second", now=datetime(2026, 7, 7, tzinfo=timezone.utc)) is True
    assert buffer.flush() == ["first", "second"]
    assert buffer.items == []


def test_inbox_store_is_idempotent() -> None:
    inbox = InMemoryInboxStore()

    assert inbox.seen("msg-1") is False
    inbox.record("msg-1")
    assert inbox.seen("msg-1") is True

    with pytest.raises(DuplicateMessageError):
        inbox.record("msg-1")


def test_outbox_store_claims_messages_fifo() -> None:
    outbox = InMemoryOutboxStore()

    first = outbox.add(b"one")
    second = outbox.add(b"two")

    assert first != second
    assert outbox.claim() == [b"one", b"two"]
    outbox.complete(first)


def test_request_response_round_trip_and_timeout() -> None:
    coordinator = RequestResponseCoordinator()
    request = RequestMessage(
        message_type="billing.get_invoice",
        payload={"invoice_id": "INV-1"},
    )
    response = ResponseMessage(
        message_type="billing.get_invoice.response",
        payload={"invoice_id": "INV-1", "total": 100},
    )

    request_envelope, ticket = coordinator.prepare_request(request, timeout=5)
    response_envelope = coordinator.prepare_response(request_envelope, response)
    coordinator.store_response(response_envelope)

    received = coordinator.wait_for_response(ticket.correlation_id, timeout=1)

    assert request_envelope.correlation_id == ticket.correlation_id
    assert received.causation_id == request_envelope.message_id
    assert received.correlation_id == ticket.correlation_id

    with pytest.raises(MessageTimeoutError):
        coordinator.wait_for_response("missing", timeout=0)
