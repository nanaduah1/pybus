from __future__ import annotations

from pybus.dispatcher import Dispatcher
from pybus.envelope import MessageEnvelope
from pybus.serializer import JsonSerializer


class Listener:
    def __init__(
        self,
        transport,
        dispatcher: Dispatcher | None = None,
        serializer: JsonSerializer | None = None,
    ) -> None:
        self.transport = transport
        self.dispatcher = dispatcher or Dispatcher()
        self.serializer = serializer or JsonSerializer()

    def listen_once(self, channel: str) -> object | None:
        raw_message = self.transport.consume(channel)
        if raw_message is None:
            return None

        envelope = MessageEnvelope.from_dict(self.serializer.loads(raw_message))
        return self.dispatcher.dispatch(envelope)

    def listen(self, channel: str, *, max_messages: int | None = None) -> list[object]:
        results: list[object] = []
        count = 0
        while max_messages is None or count < max_messages:
            result = self.listen_once(channel)
            if result is None:
                break
            results.append(result)
            count += 1
        return results
