from __future__ import annotations

import pybus
from pybus.listener import (
    DEFAULT_FAILED_QUEUE_NAME,
    DEFAULT_QUEUE_NAME,
)
from pybus.queues import (
    DEFAULT_QUEUE_TOPOLOGY,
    DEFAULT_SLOW_QUEUE_NAME,
    QueueTopology,
    declare_queue,
    declare_queues,
)


def test_default_queue_topology_includes_builtin_queues() -> None:
    topology = QueueTopology()

    assert topology.default_queue == DEFAULT_QUEUE_NAME
    assert topology.slow_queue == DEFAULT_SLOW_QUEUE_NAME
    assert topology.dead_letter_queue == DEFAULT_FAILED_QUEUE_NAME
    assert topology.queues == (
        DEFAULT_QUEUE_NAME,
        DEFAULT_SLOW_QUEUE_NAME,
        DEFAULT_FAILED_QUEUE_NAME,
    )


def test_queue_topology_can_declare_extra_queues_without_losing_defaults() -> None:
    topology = (
        QueueTopology()
        .declare_queue("billing")
        .declare_queues(
            "reports",
            "billing",
        )
    )

    assert topology.has_queue("billing")
    assert topology.has_queue("reports")
    assert topology.queues == (
        DEFAULT_QUEUE_NAME,
        DEFAULT_SLOW_QUEUE_NAME,
        DEFAULT_FAILED_QUEUE_NAME,
        "billing",
        "reports",
    )


def test_top_level_helpers_return_declarative_topologies() -> None:
    topology = declare_queue("billing")
    other_topology = declare_queues("reports", topology=topology)

    assert topology is not DEFAULT_QUEUE_TOPOLOGY
    assert topology.has_queue("billing")
    assert other_topology.has_queue("billing")
    assert other_topology.has_queue("reports")


def test_queue_helpers_are_available_from_the_package_root() -> None:
    assert pybus.DEFAULT_QUEUE_NAME == DEFAULT_QUEUE_NAME
    assert pybus.DEFAULT_FAILED_QUEUE_NAME == DEFAULT_FAILED_QUEUE_NAME
    assert pybus.DEFAULT_SLOW_QUEUE_NAME == DEFAULT_SLOW_QUEUE_NAME
    assert pybus.QueueTopology is QueueTopology
