from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
import math
from collections.abc import Callable
from threading import Event
from typing import Protocol
from uuid import uuid4

from pybus.envelope import MessageEnvelope
from pybus.delivery import CommandDeliveryOutcome, CommandDeliveryStatus
from pybus.exceptions import IndeterminateDeliveryError
from pybus.exceptions import InvalidMessageDefinitionError
from pybus.recurrence import (
    EndRecurrence,
    RecurringCommandStore,
    ScheduleNextOccurrence,
)


DURABLE_RECORD_HEADER = "pybus_durable_record"
DURABLE_GENERATION_HEADER = "pybus_durable_generation"


class DurableCommandState(StrEnum):
    PENDING = "pending"
    CLAIMED = "claimed"
    PUBLISHED = "published"
    RUNNING = "running"
    SETTLING = "settling"
    INDETERMINATE = "indeterminate"
    SUCCEEDED = "succeeded"
    DEAD_LETTERED = "dead_lettered"
    CANCELLED = "cancelled"


class DurableDeliveryDecision(StrEnum):
    PROCEED = "proceed"
    DROP = "drop"
    ABORT = "abort"


@dataclass(frozen=True, slots=True)
class DurableCommandDraft:
    message_id: str
    message_type: str
    version: int
    payload: object
    headers: dict[str, object]
    created_at: datetime
    available_at: datetime
    queue: str
    fingerprint: str
    idempotency_key: str | None = None


@dataclass(frozen=True, slots=True)
class DurableCommandRecord:
    id: str
    message_id: str
    message_type: str
    version: int
    payload: object
    headers: dict[str, object]
    created_at: datetime
    available_at: datetime
    queue: str
    fingerprint: str
    idempotency_key: str | None
    state: DurableCommandState = DurableCommandState.PENDING
    generation: int = 0
    retry_count: int = 0
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    reconciliation_due_at: datetime | None = None
    max_retries: int | None = None
    settlement_status: str | None = None
    settlement_source: str | None = None
    settlement_destination: str | None = None
    last_attempt_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    series_id: str | None = None
    occurrence_number: int | None = None

    @classmethod
    def from_draft(cls, draft: DurableCommandDraft) -> DurableCommandRecord:
        return cls(
            id=str(uuid4()),
            message_id=draft.message_id,
            message_type=draft.message_type,
            version=draft.version,
            payload=draft.payload,
            headers=dict(draft.headers),
            created_at=draft.created_at,
            available_at=draft.available_at,
            queue=draft.queue,
            fingerprint=draft.fingerprint,
            idempotency_key=draft.idempotency_key,
        )


@dataclass(frozen=True, slots=True)
class DurableCommandClaim:
    id: str
    message_id: str
    message_type: str
    version: int
    payload: object
    headers: dict[str, object]
    created_at: datetime
    queue: str
    generation: int
    retry_count: int
    settlement_status: str | None = None
    settlement_source: str | None = None
    last_attempt_at: datetime | None = None

    @classmethod
    def from_record(cls, record: DurableCommandRecord) -> DurableCommandClaim:
        return cls(
            id=record.id,
            message_id=record.message_id,
            message_type=record.message_type,
            version=record.version,
            payload=record.payload,
            headers=dict(record.headers),
            created_at=record.created_at,
            queue=record.queue,
            generation=record.generation,
            retry_count=record.retry_count,
            settlement_status=record.settlement_status,
            settlement_source=record.settlement_source,
            last_attempt_at=record.last_attempt_at,
        )

    def delivery_envelope(self) -> MessageEnvelope:
        headers = dict(self.headers)
        if self.retry_count:
            headers["retries"] = self.retry_count
        if self.last_attempt_at is not None:
            headers["last_attempt"] = self.last_attempt_at.isoformat()
        if self.settlement_status == CommandDeliveryStatus.DEAD_LETTERED.value:
            headers["dead_lettered_from"] = self.settlement_source
        return MessageEnvelope.create(
            message_id=self.message_id,
            message_type=self.message_type,
            message_kind="command",
            version=self.version,
            payload=self.payload,
            headers={
                **headers,
                DURABLE_RECORD_HEADER: self.id,
                DURABLE_GENERATION_HEADER: self.generation,
            },
            created_at=self.created_at,
        )


@dataclass(frozen=True, slots=True)
class DurableCommandHandle:
    id: str
    message_id: str
    state: DurableCommandState
    created_at: datetime
    series_id: str | None = None
    occurrence_number: int | None = None

    @classmethod
    def from_record(cls, record: DurableCommandRecord) -> DurableCommandHandle:
        return cls(
            record.id,
            record.message_id,
            record.state,
            record.created_at,
            record.series_id,
            record.occurrence_number,
        )


class DurableCommandStore(Protocol):
    def schedule(self, draft: DurableCommandDraft) -> DurableCommandRecord: ...

    def claim(
        self, *, worker_id: str, now: datetime, lease_expires_at: datetime
    ) -> DurableCommandClaim | None: ...

    def mark_published(
        self,
        claim: DurableCommandClaim,
        *,
        now: datetime,
        reconciliation_due_at: datetime,
    ) -> None: ...

    def mark_publish_indeterminate(
        self,
        claim: DurableCommandClaim,
        *,
        now: datetime,
        reconciliation_due_at: datetime,
    ) -> None: ...

    def admit_delivery(
        self, admission: DurableDeliveryAdmission
    ) -> DurableDeliveryDecision: ...

    def checkpoint_settlement(self, settlement: DurableSettlement) -> None: ...

    def release_delivery(
        self,
        *,
        record_id: str,
        generation: int,
        reconciliation_due_at: datetime,
    ) -> None: ...

    def apply_outcome(
        self,
        outcome: CommandDeliveryOutcome,
        *,
        reconciliation_due_at: datetime,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class DurableDeliveryAdmission:
    record_id: str
    generation: int
    source_queue: str
    retry_count: int
    max_retries: int
    message_id: str
    message_type: str
    version: int
    payload: object
    application_headers: dict[str, object]
    last_attempt_at: datetime | None = None
    admitted_at: datetime | None = None
    reconciliation_due_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class DurableSettlement:
    record_id: str
    generation: int
    status: CommandDeliveryStatus
    source_queue: str
    destination_queue: str | None
    retry_count: int
    max_retries: int
    reconciliation_due_at: datetime
    last_attempt_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class DurableCommandPolicy:
    lease_duration: timedelta = timedelta(seconds=30)
    reconciliation_delay: timedelta = timedelta(minutes=5)

    def __post_init__(self) -> None:
        if self.lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        if self.reconciliation_delay <= timedelta(0):
            raise ValueError("reconciliation_delay must be positive")


class DurableCommandRunner:
    def __init__(
        self,
        store: DurableCommandStore,
        publisher: Callable[[MessageEnvelope, str], object],
        *,
        policy: DurableCommandPolicy | None = None,
        worker_id: str | None = None,
    ) -> None:
        self.store = store
        self.publisher = publisher
        self.policy = policy or DurableCommandPolicy()
        self.worker_id = worker_id or str(uuid4())

    def run_once(self) -> DurableCommandClaim | None:
        now = datetime.now(timezone.utc)
        claim = self.store.claim(
            worker_id=self.worker_id,
            now=now,
            lease_expires_at=now + self.policy.lease_duration,
        )
        if claim is None:
            return None
        try:
            envelope = claim.delivery_envelope()
            self.publisher(envelope, claim.queue)
        except Exception as exc:
            try:
                self.store.mark_publish_indeterminate(
                    claim,
                    now=now,
                    reconciliation_due_at=datetime.now(timezone.utc)
                    + self.policy.reconciliation_delay,
                )
            except Exception as state_exc:
                raise IndeterminateDeliveryError(
                    f"Durable command publication and state are indeterminate for {claim.id}"
                ) from state_exc
            raise IndeterminateDeliveryError(
                f"Durable command publication is indeterminate for {claim.id}"
            ) from exc
        published_at = datetime.now(timezone.utc)
        try:
            self.store.mark_published(
                claim,
                now=published_at,
                reconciliation_due_at=published_at + self.policy.reconciliation_delay,
            )
        except Exception as exc:
            raise IndeterminateDeliveryError(
                f"Durable command publication state is indeterminate for {claim.id}"
            ) from exc
        return claim


class DurableCommandController:
    def __init__(
        self, store: DurableCommandStore, policy: DurableCommandPolicy
    ) -> None:
        self.store = store
        self.policy = policy

    @staticmethod
    def identity(envelope: MessageEnvelope) -> tuple[str, int] | None:
        record_id = envelope.headers.get(DURABLE_RECORD_HEADER)
        generation = envelope.headers.get(DURABLE_GENERATION_HEADER)
        if record_id is None and generation is None:
            return None
        if (
            not isinstance(record_id, str)
            or not record_id
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation < 1
        ):
            raise ValueError("invalid durable delivery identity")
        return record_id, generation

    def admit(
        self,
        envelope: MessageEnvelope,
        *,
        source_queue: str,
        retry_count: int,
        max_retries: int,
    ) -> DurableDeliveryDecision | None:
        identity = self.identity(envelope)
        if identity is None:
            return None
        if envelope.message_kind != "command":
            raise ValueError("durable delivery must be a command")
        record_id, generation = identity
        now = datetime.now(timezone.utc)
        last_attempt = envelope.headers.get("last_attempt")
        if last_attempt is None:
            last_attempt_at = None
        elif isinstance(last_attempt, str):
            last_attempt_at = datetime.fromisoformat(last_attempt)
            if last_attempt_at.tzinfo is None:
                raise ValueError("durable retry timing must be timezone-aware")
        else:
            raise ValueError("durable retry timing is invalid")
        return self.store.admit_delivery(
            DurableDeliveryAdmission(
                record_id=record_id,
                generation=generation,
                source_queue=source_queue,
                retry_count=retry_count,
                max_retries=max_retries,
                message_id=envelope.message_id,
                message_type=envelope.message_type,
                version=envelope.version,
                payload=envelope.payload,
                application_headers={
                    key: value
                    for key, value in envelope.headers.items()
                    if key
                    not in {
                        DURABLE_RECORD_HEADER,
                        DURABLE_GENERATION_HEADER,
                        "dead_lettered_from",
                        "last_attempt",
                        "retries",
                    }
                },
                last_attempt_at=last_attempt_at,
                admitted_at=now,
                reconciliation_due_at=now + self.policy.reconciliation_delay,
            )
        )

    def checkpoint(
        self,
        envelope: MessageEnvelope,
        *,
        status: CommandDeliveryStatus,
        source_queue: str,
        destination_queue: str | None,
        retry_count: int,
        max_retries: int,
        last_attempt_at: datetime | None = None,
    ) -> None:
        identity = self.identity(envelope)
        if identity is None:
            return
        record_id, generation = identity
        self.store.checkpoint_settlement(
            DurableSettlement(
                record_id=record_id,
                generation=generation,
                status=status,
                source_queue=source_queue,
                destination_queue=destination_queue,
                retry_count=retry_count,
                max_retries=max_retries,
                reconciliation_due_at=datetime.now(timezone.utc)
                + self.policy.reconciliation_delay,
                last_attempt_at=last_attempt_at,
            )
        )

    def release(self, envelope: MessageEnvelope) -> None:
        identity = self.identity(envelope)
        if identity is None:
            return
        record_id, generation = identity
        self.store.release_delivery(
            record_id=record_id,
            generation=generation,
            reconciliation_due_at=datetime.now(timezone.utc)
            + self.policy.reconciliation_delay,
        )

    def apply_outcome(self, outcome: CommandDeliveryOutcome) -> None:
        if outcome.durable_record_id is not None:
            self.store.apply_outcome(
                outcome,
                reconciliation_due_at=datetime.now(timezone.utc)
                + self.policy.reconciliation_delay,
            )

    def complete_success(
        self,
        outcome: CommandDeliveryOutcome,
        handler_result: object,
        *,
        completed_at: datetime,
    ) -> None:
        if isinstance(self.store, RecurringCommandStore):
            handled = self.store.complete_recurring_success(
                outcome,
                handler_result,
                completed_at=completed_at,
            )
            if handled:
                return
        if isinstance(handler_result, (ScheduleNextOccurrence, EndRecurrence)):
            raise InvalidMessageDefinitionError(
                "recurrence results require a recurring durable command"
            )
        self.apply_outcome(outcome)


class DurableCommandPoller:
    """Adapt a durable runner to the regular worker lifecycle."""

    dead_letter_channel = "__pybus_durable_terminal__"

    def __init__(
        self,
        runner: DurableCommandRunner,
        *,
        idle_delay: float = 1.0,
        stop_event: Event | None = None,
    ) -> None:
        if (
            isinstance(idle_delay, bool)
            or not isinstance(idle_delay, (int, float))
            or not math.isfinite(idle_delay)
            or idle_delay < 0
        ):
            raise ValueError("idle_delay must be a non-negative finite number")
        self.runner = runner
        self.idle_delay = float(idle_delay)
        self.stop_event = stop_event or Event()

    def listen_once(self, channel: str | tuple[str, ...]) -> object | None:
        result = self.runner.run_once()
        if result is None:
            self.stop_event.wait(self.idle_delay)
        return result


__all__ = [
    "DURABLE_GENERATION_HEADER",
    "DURABLE_RECORD_HEADER",
    "DurableCommandClaim",
    "DurableCommandController",
    "DurableCommandDraft",
    "DurableCommandHandle",
    "DurableCommandPolicy",
    "DurableCommandPoller",
    "DurableCommandRecord",
    "DurableCommandRunner",
    "DurableCommandState",
    "DurableCommandStore",
    "DurableDeliveryDecision",
    "DurableDeliveryAdmission",
    "DurableSettlement",
]
