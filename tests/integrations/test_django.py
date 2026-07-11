from __future__ import annotations

from dataclasses import dataclass

from pybus.integrations.django import DjangoBusAdapter


@dataclass
class FakeConnection:
    in_atomic_block: bool


class FakeTransactionModule:
    def __init__(self, *, in_atomic_block: bool) -> None:
        self._connection = FakeConnection(in_atomic_block=in_atomic_block)
        self.callbacks: list[callable] = []

    def get_connection(self) -> FakeConnection:
        return self._connection

    def on_commit(self, callback) -> None:
        self.callbacks.append(callback)


def test_django_bus_adapter_defers_within_atomic_block() -> None:
    published: list[tuple[str, dict, str]] = []
    transaction = FakeTransactionModule(in_atomic_block=True)
    adapter = DjangoBusAdapter(
        lambda event_type, data, queue=None: published.append(
            (event_type, data, queue)
        ),
        transaction_module=transaction,
    )

    adapter.schedule("student.enrolled", {"student_id": "S-1"})

    assert published == []
    assert len(transaction.callbacks) == 1

    transaction.callbacks[0]()

    assert published == [("student.enrolled", {"student_id": "S-1"}, "pybus.jobs")]


def test_django_bus_adapter_publishes_immediately_outside_transaction() -> None:
    published: list[tuple[str, dict, str]] = []
    transaction = FakeTransactionModule(in_atomic_block=False)
    adapter = DjangoBusAdapter(
        lambda event_type, data, queue=None: published.append(
            (event_type, data, queue)
        ),
        transaction_module=transaction,
        default_queue="custom.queue",
    )

    adapter.schedule("student.enrolled", {"student_id": "S-2"})

    assert transaction.callbacks == []
    assert published == [("student.enrolled", {"student_id": "S-2"}, "custom.queue")]
