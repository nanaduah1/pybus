from __future__ import annotations

from dataclasses import replace

import pytest

import pybus
from pybus import durable, exceptions, recurrence
from pybus.bus import Pybus
from pybus.composition import (
    BusConfiguration,
    DurableCommandStoreFactory,
    DurableJobStoreFactory,
)
from pybus.listener import Listener
from pybus.transports.memory import MemoryTransport


class JobStore:
    pass


def test_command_oriented_durability_names_are_exact_job_aliases() -> None:
    pairs = [
        (pybus.DurableCommandHandle, pybus.DurableJobHandle),
        (pybus.DurableCommandPolicy, pybus.DurableJobPolicy),
        (pybus.DurableCommandState, pybus.DurableJobState),
        (pybus.DurableCommandStore, pybus.DurableJobStore),
        (pybus.DurableCommandConflictError, pybus.DurableJobConflictError),
        (pybus.DurableCommandsNotConfiguredError, pybus.DurableJobsNotConfiguredError),
        (pybus.RecurringCommandSeriesHandle, pybus.JobSeriesHandle),
        (pybus.RecurringCommandSeriesState, pybus.JobSeriesState),
        (pybus.RecurringCommandSeriesNotFoundError, pybus.JobSeriesNotFoundError),
    ]

    assert all(legacy is canonical for legacy, canonical in pairs)
    assert pybus.DurableJobHandle.__name__ == "DurableJobHandle"
    assert pybus.JobSeriesHandle.__name__ == "JobSeriesHandle"
    module_pairs = [
        (durable.DurableCommandState, durable.DurableJobState),
        (durable.DurableCommandDraft, durable.DurableJobDraft),
        (durable.DurableCommandRecord, durable.DurableJobRecord),
        (durable.DurableCommandClaim, durable.DurableJobClaim),
        (durable.DurableCommandHandle, durable.DurableJobHandle),
        (durable.DurableCommandStore, durable.DurableJobStore),
        (durable.DurableCommandPolicy, durable.DurableJobPolicy),
        (durable.DurableCommandRunner, durable.DurableJobRunner),
        (durable.DurableCommandController, durable.DurableJobController),
        (durable.DurableCommandPoller, durable.DurableJobPoller),
        (recurrence.RecurringCommandSeriesState, recurrence.JobSeriesState),
        (recurrence.RecurringCommandSeriesDraft, recurrence.JobSeriesDraft),
        (recurrence.RecurringCommandSeriesRecord, recurrence.JobSeriesRecord),
        (recurrence.RecurringCommandSeriesHandle, recurrence.JobSeriesHandle),
        (recurrence.RecurringCommandStore, recurrence.JobSeriesStore),
        (exceptions.DurableCommandConflictError, exceptions.DurableJobConflictError),
        (
            exceptions.DurableCommandsNotConfiguredError,
            exceptions.DurableJobsNotConfiguredError,
        ),
        (
            exceptions.DurableRecurrenceNotSupportedError,
            exceptions.JobSeriesNotSupportedError,
        ),
        (
            exceptions.RecurringCommandSeriesNotFoundError,
            exceptions.JobSeriesNotFoundError,
        ),
        (DurableCommandStoreFactory, DurableJobStoreFactory),
    ]
    assert all(legacy is canonical for legacy, canonical in module_pairs)


def test_pybus_normalizes_canonical_and_legacy_job_configuration() -> None:
    canonical_store = JobStore()
    legacy_store = JobStore()

    canonical = Pybus(MemoryTransport(), durable_job_store=canonical_store)
    legacy = Pybus(MemoryTransport(), durable_command_store=legacy_store)

    assert canonical.durable_job_store is canonical_store
    assert canonical.durable_command_store is canonical_store
    assert legacy.durable_job_store is legacy_store
    assert legacy.durable_command_store is legacy_store


def test_pybus_legacy_attributes_forward_to_canonical_state() -> None:
    bus = Pybus(MemoryTransport())
    first_store = JobStore()
    second_store = JobStore()
    first_policy = pybus.DurableJobPolicy()
    second_policy = pybus.DurableJobPolicy()

    bus.durable_job_store = first_store
    assert bus.durable_command_store is first_store
    assert bus.listener._durable_job_controller.store is first_store
    bus.durable_command_store = second_store
    assert bus.durable_job_store is second_store
    assert bus.listener._durable_job_controller.store is second_store

    bus.durable_job_policy = first_policy
    assert bus.durable_command_policy is first_policy
    assert bus.listener._durable_job_controller.policy is first_policy
    bus.durable_command_policy = second_policy
    assert bus.durable_job_policy is second_policy
    assert bus.listener._durable_job_controller.policy is second_policy

    bus.durable_command_store = None
    assert bus.durable_job_store is None
    assert bus.listener._durable_job_controller is None
    assert bus.listener._durable_command_controller is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"durable_job_store": None, "durable_command_store": None},
        {
            "durable_job_policy": None,
            "durable_command_policy": None,
        },
    ],
)
def test_pybus_rejects_dual_job_configuration_even_when_explicit_none(kwargs) -> None:
    with pytest.raises(TypeError, match="cannot both be supplied"):
        Pybus(MemoryTransport(), **kwargs)


@pytest.mark.parametrize("use_same_value", [True, False])
def test_pybus_rejects_dual_job_configuration_regardless_of_value_identity(
    use_same_value: bool,
) -> None:
    canonical_store = JobStore()
    legacy_store = canonical_store if use_same_value else JobStore()

    with pytest.raises(TypeError, match="cannot both be supplied"):
        Pybus(
            MemoryTransport(),
            durable_job_store=canonical_store,
            durable_command_store=legacy_store,
        )


def test_bus_configuration_normalizes_factories_and_rejects_dual_names() -> None:
    store = JobStore()
    canonical = BusConfiguration(
        transport_factory=MemoryTransport,
        durable_job_store_factory=lambda: store,
    )
    legacy = BusConfiguration(
        transport_factory=MemoryTransport,
        durable_command_store_factory=lambda: store,
    )

    assert canonical.create().durable_job_store is store
    assert legacy.create().durable_job_store is store
    with pytest.raises(TypeError, match="cannot both be supplied"):
        BusConfiguration(
            transport_factory=MemoryTransport,
            durable_job_store_factory=None,
            durable_command_store_factory=None,
        )


def test_bus_configuration_rejects_dual_names_before_calling_either_factory() -> None:
    calls: list[str] = []

    def canonical_factory() -> JobStore:
        calls.append("canonical")
        return JobStore()

    def legacy_factory() -> JobStore:
        calls.append("legacy")
        return JobStore()

    with pytest.raises(TypeError, match="cannot both be supplied"):
        BusConfiguration(
            transport_factory=MemoryTransport,
            durable_job_store_factory=canonical_factory,
            durable_command_store_factory=legacy_factory,
        )

    assert calls == []


def test_bus_configuration_remains_compatible_with_dataclasses_replace() -> None:
    store = JobStore()

    def factory() -> JobStore:
        return store

    policy = pybus.DurableJobPolicy()
    configurations = [
        BusConfiguration(transport_factory=MemoryTransport),
        BusConfiguration(
            transport_factory=MemoryTransport,
            durable_job_store_factory=factory,
            durable_job_policy=policy,
        ),
        BusConfiguration(
            transport_factory=MemoryTransport,
            durable_command_store_factory=factory,
            durable_command_policy=policy,
        ),
    ]

    for configuration in configurations:
        clone = replace(configuration)
        assert clone is not configuration
        assert (
            clone.durable_job_store_factory is configuration.durable_job_store_factory
        )
        assert clone.durable_command_store_factory is clone.durable_job_store_factory
        assert clone.durable_job_policy is configuration.durable_job_policy
        assert clone.durable_command_policy is clone.durable_job_policy
        assert (
            clone.create().durable_job_store is configuration.create().durable_job_store
        )


def test_bus_configuration_replace_accepts_canonical_and_legacy_updates() -> None:
    first_store = JobStore()
    second_store = JobStore()

    def first_factory() -> JobStore:
        return first_store

    def second_factory() -> JobStore:
        return second_store

    first_policy = pybus.DurableJobPolicy()
    second_policy = pybus.DurableJobPolicy()
    configuration = BusConfiguration(transport_factory=MemoryTransport)

    canonical = replace(
        configuration,
        durable_job_store_factory=first_factory,
        durable_job_policy=first_policy,
    )
    legacy = replace(
        configuration,
        durable_command_store_factory=second_factory,
        durable_command_policy=second_policy,
    )

    assert canonical.create().durable_job_store is first_store
    assert canonical.durable_command_store_factory is first_factory
    assert canonical.durable_command_policy is first_policy
    assert legacy.create().durable_job_store is second_store
    assert legacy.durable_job_store_factory is second_factory
    assert legacy.durable_job_policy is second_policy


def test_worker_bootstrap_is_a_direct_compatibility_alias() -> None:
    assert Pybus.create_durable_command_worker is Pybus.create_durable_job_worker


def test_listener_rejects_dual_controller_configuration() -> None:
    with pytest.raises(TypeError, match="cannot both be supplied"):
        Listener(
            MemoryTransport(),
            durable_job_controller=None,
            durable_command_controller=None,
        )


def test_listener_normalizes_canonical_and_legacy_controller_configuration() -> None:
    canonical_controller = object()
    legacy_controller = object()

    canonical = Listener(MemoryTransport(), durable_job_controller=canonical_controller)
    legacy = Listener(MemoryTransport(), durable_command_controller=legacy_controller)

    assert canonical._durable_job_controller is canonical_controller
    assert canonical._durable_command_controller is canonical_controller
    assert legacy._durable_job_controller is legacy_controller
    assert legacy._durable_command_controller is legacy_controller


def test_job_ontology_does_not_add_a_message_kind_or_scheduling_api() -> None:
    assert not hasattr(pybus, "job")
    assert not hasattr(pybus, "schedule_job")
    assert not hasattr(pybus, "send_job")
