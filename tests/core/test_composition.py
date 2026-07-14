from concurrent.futures import ThreadPoolExecutor
import time
from types import ModuleType
from unittest.mock import Mock

import pytest

import pybus.bus as bus_module
from pybus import BusConfiguration, event, event_handler, get_bus
from pybus.bus import configure_transport
from pybus.transports.memory import MemoryTransport
from pybus.worker import WorkerHook


@event("student.enrolled")
class StudentEnrolled:
    student_id: int


def _handler_module(name: str, received: list[tuple[str, int]]) -> ModuleType:
    module = ModuleType(name)

    @event_handler(StudentEnrolled)
    def handle(message: StudentEnrolled) -> None:
        received.append((name, message.student_id))

    module.handle = handle
    return module


def test_configuration_is_declarative_until_a_bus_is_created(monkeypatch) -> None:
    transport_factory = Mock(return_value=MemoryTransport())
    import_handler_module = Mock(return_value=_handler_module("handlers", []))
    monkeypatch.setattr("pybus.composition.import_module", import_handler_module)

    configuration = BusConfiguration(
        transport_factory=transport_factory,
        handler_modules=("application.handlers",),
    )

    transport_factory.assert_not_called()
    import_handler_module.assert_not_called()

    configuration.create()

    transport_factory.assert_called_once_with()
    import_handler_module.assert_called_once_with("application.handlers")


def test_create_builds_an_isolated_bus_and_transport_override_bypasses_factory(
    monkeypatch,
) -> None:
    received: list[tuple[str, int]] = []
    module = _handler_module("application.handlers", received)
    monkeypatch.setattr("pybus.composition.import_module", lambda _: module)
    transport_factory = Mock(side_effect=AssertionError("factory must not run"))
    configuration = BusConfiguration(
        transport_factory=transport_factory,
        handler_modules=("application.handlers",),
    )
    transport = MemoryTransport()

    bus = configuration.create(transport=transport)
    bus.publish_event(StudentEnrolled(student_id=7))
    bus.create_worker(error_delay=0).run(max_iterations=1)

    assert bus.transport is transport
    assert received == [("application.handlers", 7)]
    transport_factory.assert_not_called()


def test_handler_modules_register_before_explicit_targets(monkeypatch) -> None:
    received: list[tuple[str, int]] = []
    module = _handler_module("application.handlers", received)
    monkeypatch.setattr("pybus.composition.import_module", lambda _: module)

    @event_handler(StudentEnrolled)
    def explicit_handler(message: StudentEnrolled) -> None:
        received.append(("explicit", message.student_id))

    bus = BusConfiguration(
        transport_factory=MemoryTransport,
        handler_modules=("application.handlers",),
        handler_targets=(explicit_handler,),
    ).create()

    bus.publish_event(StudentEnrolled(student_id=8))
    bus.create_worker(error_delay=0).run(max_iterations=1)

    assert received == [("application.handlers", 8), ("explicit", 8)]


def test_create_returns_fresh_buses_registries_and_transports(monkeypatch) -> None:
    module = _handler_module("application.handlers", [])
    monkeypatch.setattr("pybus.composition.import_module", lambda _: module)
    configuration = BusConfiguration(
        transport_factory=MemoryTransport,
        handler_modules=("application.handlers",),
    )

    first = configuration.create()
    second = configuration.create()

    assert first is not second
    assert first.transport is not second.transport
    assert first.dispatcher.registry is not second.dispatcher.registry


def test_configure_is_thread_safe_idempotent_and_reinstalls_its_cached_bus(
    monkeypatch,
) -> None:
    monkeypatch.setattr(bus_module, "_default_bus", bus_module._default_bus)
    calls = 0

    def transport_factory() -> MemoryTransport:
        nonlocal calls
        calls += 1
        time.sleep(0.01)
        return MemoryTransport()

    configuration = BusConfiguration(transport_factory=transport_factory)

    with ThreadPoolExecutor(max_workers=8) as executor:
        buses = list(executor.map(lambda _: configuration.configure(), range(8)))

    configured = buses[0]
    assert all(bus is configured for bus in buses)
    assert calls == 1
    assert get_bus() is configured

    replacement = configure_transport(MemoryTransport())
    assert replacement is get_bus()

    assert configuration.configure() is configured
    assert get_bus() is configured
    assert calls == 1


def test_failed_configuration_preserves_the_previous_default_and_can_retry(
    monkeypatch,
) -> None:
    monkeypatch.setattr(bus_module, "_default_bus", bus_module._default_bus)
    previous = configure_transport(MemoryTransport())
    module = _handler_module("application.handlers", [])
    import_handler_module = Mock(side_effect=ModuleNotFoundError("missing"))
    monkeypatch.setattr("pybus.composition.import_module", import_handler_module)
    configuration = BusConfiguration(
        transport_factory=MemoryTransport,
        handler_modules=("application.handlers",),
    )

    with pytest.raises(ImportError, match="application.handlers.*missing"):
        configuration.configure()

    assert get_bus() is previous

    import_handler_module.side_effect = None
    import_handler_module.return_value = module
    configured = configuration.configure()

    assert get_bus() is configured
    assert import_handler_module.call_count == 2


def test_failed_transport_factory_is_not_cached_and_preserves_default(
    monkeypatch,
) -> None:
    monkeypatch.setattr(bus_module, "_default_bus", bus_module._default_bus)
    previous = configure_transport(MemoryTransport())
    transport_factory = Mock(
        side_effect=[RuntimeError("configuration unavailable"), MemoryTransport()]
    )
    configuration = BusConfiguration(transport_factory=transport_factory)

    with pytest.raises(RuntimeError, match="configuration unavailable"):
        configuration.configure()

    assert get_bus() is previous

    configured = configuration.configure()

    assert get_bus() is configured
    assert transport_factory.call_count == 2


def test_invalid_transport_factory_result_is_not_cached_and_can_retry(
    monkeypatch,
) -> None:
    monkeypatch.setattr(bus_module, "_default_bus", bus_module._default_bus)
    previous = configure_transport(MemoryTransport())
    transport_factory = Mock(side_effect=[None, MemoryTransport()])
    configuration = BusConfiguration(transport_factory=transport_factory)

    with pytest.raises(TypeError, match="transport.*publish.*consume"):
        configuration.configure()

    assert get_bus() is previous

    configured = configuration.configure()

    assert get_bus() is configured
    assert transport_factory.call_count == 2


@pytest.mark.parametrize(
    "handler_modules",
    [("",), ("   ",), (7,), ("application.handlers", "application.handlers")],
)
def test_configuration_rejects_invalid_or_duplicate_handler_modules(
    handler_modules,
) -> None:
    with pytest.raises(ValueError, match="handler_modules"):
        BusConfiguration(
            transport_factory=MemoryTransport,
            handler_modules=handler_modules,
        )


def test_configuration_rejects_non_callable_factories() -> None:
    with pytest.raises(TypeError, match="transport_factory"):
        BusConfiguration(transport_factory=MemoryTransport())

    with pytest.raises(TypeError, match="worker_hook_factories"):
        BusConfiguration(
            transport_factory=MemoryTransport,
            worker_hook_factories=(WorkerHook(),),
        )


def test_configuration_rejects_a_string_handler_modules_value() -> None:
    with pytest.raises(TypeError, match="handler_modules.*sequence"):
        BusConfiguration(
            transport_factory=MemoryTransport,
            handler_modules="application.handlers",
        )


def test_configuration_rejects_duplicate_explicit_handler_targets() -> None:
    @event_handler(StudentEnrolled)
    def handler(message: StudentEnrolled) -> None:
        pass

    with pytest.raises(ValueError, match="handler_targets.*duplicates"):
        BusConfiguration(
            transport_factory=MemoryTransport,
            handler_targets=(handler, handler),
        )


def test_create_rejects_module_and_explicit_target_overlap(monkeypatch) -> None:
    module = _handler_module("application.handlers", [])
    monkeypatch.setattr("pybus.composition.import_module", lambda _: module)

    configuration = BusConfiguration(
        transport_factory=MemoryTransport,
        handler_modules=("application.handlers",),
        handler_targets=(module.handle,),
    )

    with pytest.raises(ValueError, match="Duplicate handler"):
        configuration.create()


def test_handler_module_import_error_names_the_declared_module(monkeypatch) -> None:
    monkeypatch.setattr(
        "pybus.composition.import_module",
        Mock(side_effect=ModuleNotFoundError("missing dependency")),
    )
    configuration = BusConfiguration(
        transport_factory=MemoryTransport,
        handler_modules=("application.handlers",),
    )

    with pytest.raises(
        ImportError,
        match="application.handlers.*missing dependency",
    ):
        configuration.create()


def test_default_worker_hooks_are_fresh_and_explicit_hooks_replace_them() -> None:
    created: list[WorkerHook] = []

    def hook_factory() -> WorkerHook:
        hook = WorkerHook()
        created.append(hook)
        return hook

    configuration = BusConfiguration(
        transport_factory=MemoryTransport,
        worker_hook_factories=(hook_factory,),
    )
    bus = configuration.create()

    first = bus.create_worker()
    second = bus.create_worker()
    explicit = WorkerHook()
    overridden = bus.create_worker(hooks=(explicit,))
    disabled = bus.create_worker(hooks=())

    assert first.hooks == (created[0],)
    assert second.hooks == (created[1],)
    assert first.hooks[0] is not second.hooks[0]
    assert overridden.hooks == (explicit,)
    assert disabled.hooks == ()
    assert len(created) == 2


def test_worker_hook_factory_failure_prevents_worker_creation() -> None:
    def fail() -> WorkerHook:
        raise RuntimeError("Django is unavailable")

    bus = BusConfiguration(
        transport_factory=MemoryTransport,
        worker_hook_factories=(fail,),
    ).create()

    with pytest.raises(RuntimeError, match="Django is unavailable"):
        bus.create_worker()


def test_worker_hook_factory_must_return_a_worker_hook() -> None:
    bus = BusConfiguration(
        transport_factory=MemoryTransport,
        worker_hook_factories=(lambda: None,),
    ).create()

    with pytest.raises(TypeError, match="WorkerHook"):
        bus.create_worker()
