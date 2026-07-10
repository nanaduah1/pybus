from __future__ import annotations

from dataclasses import dataclass, replace

DEFAULT_QUEUE_NAME = "skuulbe.jobs"
DEFAULT_SLOW_QUEUE_NAME = "skuulbe.jobs.slow"
DEFAULT_FAILED_QUEUE_NAME = "skuulbe.jobs.failed"


@dataclass(frozen=True, slots=True)
class QueueTopology:
    default_queue: str = DEFAULT_QUEUE_NAME
    slow_queue: str = DEFAULT_SLOW_QUEUE_NAME
    dead_letter_queue: str = DEFAULT_FAILED_QUEUE_NAME
    extra_queues: tuple[str, ...] = ()

    def declare_queue(self, queue_name: str) -> "QueueTopology":
        return self.declare_queues(queue_name)

    def declare_queues(self, *queue_names: str) -> "QueueTopology":
        extras = list(self.extra_queues)
        for queue_name in queue_names:
            if self.has_queue(queue_name) or queue_name in extras:
                continue
            extras.append(queue_name)
        return replace(self, extra_queues=tuple(extras))

    @property
    def queues(self) -> tuple[str, ...]:
        return (
            self.default_queue,
            self.slow_queue,
            self.dead_letter_queue,
            *self.extra_queues,
        )

    def has_queue(self, queue_name: str) -> bool:
        return queue_name in self.queues

    def resolve(self, queue_name: str | None = None) -> str:
        return queue_name or self.default_queue


DEFAULT_QUEUE_TOPOLOGY = QueueTopology()


def declare_queue(
    queue_name: str, topology: QueueTopology | None = None
) -> QueueTopology:
    return (topology or DEFAULT_QUEUE_TOPOLOGY).declare_queue(queue_name)


def declare_queues(
    *queue_names: str, topology: QueueTopology | None = None
) -> QueueTopology:
    return (topology or DEFAULT_QUEUE_TOPOLOGY).declare_queues(*queue_names)


__all__ = [
    "DEFAULT_FAILED_QUEUE_NAME",
    "DEFAULT_QUEUE_NAME",
    "DEFAULT_QUEUE_TOPOLOGY",
    "DEFAULT_SLOW_QUEUE_NAME",
    "QueueTopology",
    "declare_queue",
    "declare_queues",
]
