from __future__ import annotations

from collections.abc import Sequence
import time
from dataclasses import dataclass
from typing import cast

from pybus.codecs import PayloadCodec
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
from pybus.request_response import (
    RequestResponseCoordinator,
    default_reply_queue_name,
)
from pybus.serializer import JsonSerializer


@dataclass(slots=True)
class Pybus:
    transport: Transport
    dispatcher: Dispatcher
    serializer: JsonSerializer
    topology: QueueTopology
    listener: Listener
    coordinator: RequestResponseCoordinator
    reply_queue: str
    payload_codec: PayloadCodec

    def __init__(
        self,
        transport: object,
        *,
        dispatcher: Dispatcher | None = None,
        serializer: JsonSerializer | None = None,
        topology: QueueTopology | None = None,
        handler_targets: Sequence[object] | None = None,
        payload_codec: PayloadCodec | None = None,
    ) -> None:
        self.transport = transport
        if dispatcher is not None and payload_codec is not None:
            raise ValueError(
                "payload_codec must be configured on the supplied dispatcher"
            )
        self.dispatcher = dispatcher or Dispatcher(payload_codec=payload_codec)
        self.payload_codec = self.dispatcher.payload_codec
        self.serializer = serializer or JsonSerializer()
        self.topology = topology or QueueTopology()
        self.listener = Listener(
            transport=transport,
            dispatcher=self.dispatcher,
            serializer=self.serializer,
            dead_letter_channel=self.topology.dead_letter_queue,
        )
        self.coordinator = RequestResponseCoordinator()
        self.reply_queue = default_reply_queue_name()
        if handler_targets:
            from pybus.handlers import register_handlers

            register_handlers(*handler_targets, registry=self.dispatcher.registry)

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
        reply_queue = reply_to or request.reply_to or self.reply_queue
        request_envelope, ticket = self.coordinator.prepare_request(
            request,
            timeout=timeout,
            reply_to=reply_queue,
            payload_codec=self.payload_codec,
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
        return cast(
            ResponseMessage,
            response_cls.from_envelope(
                response_envelope,
                payload_codec=self.payload_codec,
            ),
        )

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
        envelope = message.to_envelope(payload_codec=self.payload_codec)
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
    handler_targets: Sequence[object] | None = None,
    payload_codec: PayloadCodec | None = None,
) -> Pybus:
    global _default_bus
    _default_bus = Pybus(
        transport,
        dispatcher=dispatcher,
        serializer=serializer,
        topology=topology,
        handler_targets=handler_targets,
        payload_codec=payload_codec,
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
