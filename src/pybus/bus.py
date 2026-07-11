from __future__ import annotations

import time
from dataclasses import dataclass
from typing import cast

from pybus.dispatcher import Dispatcher
from pybus.envelope import MessageEnvelope
from pybus.exceptions import MessageTimeoutError
from pybus.listener import Listener
from pybus.messages import (
    BaseMessage,
    CommandMessage,
    EventMessage,
    RequestMessage,
    ResponseMessage,
    message_class_for_kind,
)
from pybus.contracts import Transport
from pybus.queues import QueueTopology
from pybus.request_response import DEFAULT_REPLY_QUEUE, RequestResponseCoordinator
from pybus.serializer import JsonSerializer


@dataclass(slots=True)
class Pybus:
    transport: Transport
    dispatcher: Dispatcher
    serializer: JsonSerializer
    topology: QueueTopology
    listener: Listener
    coordinator: RequestResponseCoordinator

    def __init__(
        self,
        transport: object,
        *,
        dispatcher: Dispatcher | None = None,
        serializer: JsonSerializer | None = None,
        topology: QueueTopology | None = None,
    ) -> None:
        self.transport = transport
        self.dispatcher = dispatcher or Dispatcher()
        self.serializer = serializer or JsonSerializer()
        self.topology = topology or QueueTopology()
        self.listener = Listener(
            transport=transport,
            dispatcher=self.dispatcher,
            serializer=self.serializer,
            dead_letter_channel=self.topology.dead_letter_queue,
        )
        self.coordinator = RequestResponseCoordinator()

    def publish_event(
        self,
        event: EventMessage,
        *,
        queue: str | None = None,
    ) -> MessageEnvelope:
        return self._publish(event, queue=queue)

    def send_command(
        self,
        command: CommandMessage,
        *,
        queue: str | None = None,
    ) -> MessageEnvelope:
        return self._publish(command, queue=queue)

    def request(
        self,
        request: RequestMessage,
        *,
        timeout: int | None = 5,
        queue: str | None = None,
        reply_to: str | None = None,
    ) -> ResponseMessage:
        request_queue = queue or self.topology.default_queue
        reply_queue = reply_to or request.reply_to or DEFAULT_REPLY_QUEUE
        request_envelope, ticket = self.coordinator.prepare_request(
            request,
            timeout=timeout,
            reply_to=reply_queue,
        )
        self.transport.publish(
            request_queue,
            self.serializer.dump(request_envelope),
        )
        response_envelope = self._await_response(
            ticket.correlation_id,
            timeout,
            reply_queue,
        )
        response_cls = message_class_for_kind(response_envelope.message_kind)
        return cast(ResponseMessage, response_cls.from_envelope(response_envelope))

    def listen_once(self, channel: str | tuple[str, ...] | list[str]) -> object | None:
        return self.listener.listen_once(channel)

    def listen(
        self,
        channel: str | tuple[str, ...] | list[str],
        *,
        max_messages: int | None = None,
    ) -> list[object]:
        return self.listener.listen(channel, max_messages=max_messages)

    def _publish(
        self,
        message: BaseMessage,
        *,
        queue: str | None = None,
    ) -> MessageEnvelope:
        envelope = message.to_envelope()
        self.transport.publish(
            queue or self.topology.default_queue, self.serializer.dump(envelope)
        )
        return envelope

    def _await_response(
        self,
        correlation_id: str,
        timeout: int | None,
        reply_queue: str,
    ) -> MessageEnvelope:
        deadline = None
        if timeout is not None:
            deadline = time.monotonic() + timeout

        while True:
            buffered = self.coordinator.take_response(correlation_id)
            if buffered is not None:
                return buffered

            if deadline is not None and time.monotonic() >= deadline:
                raise MessageTimeoutError(
                    f"Timed out waiting for response {correlation_id}"
                )

            poll_timeout = 1
            sleep_interval = 0.01
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise MessageTimeoutError(
                        f"Timed out waiting for response {correlation_id}"
                    )
                # Poll in short slices so sub-second timeouts are respected.
                if remaining < 1:
                    poll_timeout = 0
                    sleep_interval = min(0.01, remaining)
                else:
                    poll_timeout = 1
                    sleep_interval = 0.01

            raw_message = self.transport.consume(reply_queue, timeout=poll_timeout)
            if raw_message is None:
                time.sleep(sleep_interval)
                continue

            envelope = MessageEnvelope.from_dict(self.serializer.loads(raw_message))
            self.coordinator.store_response(envelope)


_default_bus: Pybus | None = None


def configure_transport(
    transport: object,
    *,
    dispatcher: Dispatcher | None = None,
    serializer: JsonSerializer | None = None,
    topology: QueueTopology | None = None,
) -> Pybus:
    global _default_bus
    _default_bus = Pybus(
        transport,
        dispatcher=dispatcher,
        serializer=serializer,
        topology=topology,
    )
    return _default_bus


def get_bus() -> Pybus:
    if _default_bus is None:
        raise RuntimeError("pybus transport has not been configured")
    return _default_bus


def publish_event(event: EventMessage, *, queue: str | None = None) -> MessageEnvelope:
    return get_bus().publish_event(event, queue=queue)


def send_command(
    command: CommandMessage,
    *,
    queue: str | None = None,
) -> MessageEnvelope:
    return get_bus().send_command(command, queue=queue)


def request(
    request: RequestMessage,
    *,
    timeout: int | None = 5,
    queue: str | None = None,
    reply_to: str | None = None,
) -> ResponseMessage:
    return get_bus().request(request, timeout=timeout, queue=queue, reply_to=reply_to)
