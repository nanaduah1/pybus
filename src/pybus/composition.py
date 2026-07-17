from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from importlib import import_module
from threading import Lock

from pybus.bus import Pybus, _set_default_bus
from pybus.codecs import PayloadCodec
from pybus.delivery import CommandDeliveryObserver
from pybus.contracts import Transport
from pybus.queues import QueueTopology
from pybus.serializer import JsonSerializer
from pybus.worker import WorkerHook


TransportFactory = Callable[[], Transport]
WorkerHookFactory = Callable[[], WorkerHook]


class _ConfigurationState:
    def __init__(self) -> None:
        self.lock = Lock()
        self.bus: Pybus | None = None


@dataclass(frozen=True, slots=True)
class BusConfiguration:
    """Reusable application choices for isolated and process-default buses."""

    transport_factory: TransportFactory
    topology: QueueTopology = field(default_factory=QueueTopology)
    handler_modules: Sequence[str] = ()
    handler_targets: Sequence[object] = ()
    serializer: JsonSerializer | None = None
    payload_codec: PayloadCodec | None = None
    worker_hook_factories: Sequence[WorkerHookFactory] | None = None
    command_delivery_observers: Sequence[CommandDeliveryObserver] = ()
    _state: _ConfigurationState = field(
        default_factory=_ConfigurationState,
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if not callable(self.transport_factory):
            raise TypeError("transport_factory must be callable")

        if isinstance(self.handler_modules, (str, bytes)):
            raise TypeError("handler_modules must be a sequence of module paths")
        modules = tuple(self.handler_modules)
        if any(not isinstance(module, str) or not module.strip() for module in modules):
            raise ValueError("handler_modules must contain non-empty strings")
        if len(set(modules)) != len(modules):
            raise ValueError("handler_modules must not contain duplicates")

        targets = tuple(self.handler_targets)
        if len({id(target) for target in targets}) != len(targets):
            raise ValueError("handler_targets must not contain duplicates")

        hook_factories = tuple(self.worker_hook_factories or ())
        if any(not callable(factory) for factory in hook_factories):
            raise TypeError("worker_hook_factories must contain callables")

        object.__setattr__(self, "handler_modules", modules)
        object.__setattr__(self, "handler_targets", targets)
        object.__setattr__(self, "worker_hook_factories", hook_factories)
        observers = tuple(self.command_delivery_observers)
        if any(not callable(observer) for observer in observers):
            raise TypeError("command_delivery_observers must contain callables")
        object.__setattr__(self, "command_delivery_observers", observers)

    def create(
        self,
        *,
        transport: Transport | None = None,
    ) -> Pybus:
        """Build a fresh isolated bus without changing the process default."""
        modules = []
        for path in self.handler_modules:
            try:
                modules.append(import_module(path))
            except ImportError as exc:
                raise ImportError(
                    f"Failed to import handler module {path!r}: {exc}"
                ) from exc
        resolved_transport = (
            self.transport_factory() if transport is None else transport
        )
        _validate_transport(resolved_transport)
        return Pybus(
            resolved_transport,
            serializer=self.serializer,
            topology=self.topology,
            handler_targets=(*modules, *self.handler_targets),
            payload_codec=self.payload_codec,
            worker_hook_factories=self.worker_hook_factories,
            command_delivery_observers=self.command_delivery_observers,
        )

    def configure(self) -> Pybus:
        """Idempotently install this configuration's bus as the process default."""
        with self._state.lock:
            if self._state.bus is None:
                self._state.bus = self.create()
            _set_default_bus(self._state.bus)
            return self._state.bus


def _validate_transport(transport: object) -> None:
    missing = [
        method
        for method in ("publish", "consume")
        if not callable(getattr(transport, method, None))
    ]
    if missing:
        raise TypeError(
            "transport must provide callable publish and consume methods; "
            f"missing {', '.join(missing)}"
        )


__all__ = ["BusConfiguration", "TransportFactory", "WorkerHookFactory"]
