from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from django.db import IntegrityError, connections, transaction
from django.db.models import F, Q
from django.utils import timezone

from pybus.delivery import CommandDeliveryOutcome, CommandDeliveryStatus
from pybus.durable import (
    DurableCommandClaim,
    DurableCommandDraft,
    DurableCommandRecord,
    DurableCommandState,
    DurableDeliveryAdmission,
    DurableDeliveryDecision,
    DurableSettlement,
)
from pybus.exceptions import (
    DurableCommandConflictError,
    SerializationError,
    WorkerAbortError,
)
from pybus.integrations.django_durable.models import DurableCommand
from pybus.serializer import JsonSerializer


class DjangoDurableCommandStore:
    """Durable command state stored in an explicitly selected Django database."""

    def __init__(self, *, using: str = "default") -> None:
        connections[using]
        self.using = using

    def schedule(self, draft: DurableCommandDraft) -> DurableCommandRecord:
        values = {
            "id": uuid4(),
            "message_id": draft.message_id,
            "message_type": draft.message_type,
            "version": draft.version,
            "payload": draft.payload,
            "headers": draft.headers,
            "created_at": draft.created_at,
            "available_at": draft.available_at,
            "queue": draft.queue,
            "fingerprint": draft.fingerprint,
            "idempotency_key": draft.idempotency_key,
        }
        try:
            with transaction.atomic(using=self.using):
                model = DurableCommand.objects.using(self.using).create(**values)
        except IntegrityError:
            if draft.idempotency_key is None:
                raise
            model = DurableCommand.objects.using(self.using).get(
                idempotency_key=draft.idempotency_key
            )
            if model.fingerprint != draft.fingerprint:
                raise DurableCommandConflictError(
                    f"idempotency key {draft.idempotency_key!r} already identifies "
                    "a different durable command"
                )
        return self._record(model)

    def claim(
        self, *, worker_id: str, now: datetime, lease_expires_at: datetime
    ) -> DurableCommandClaim | None:
        if connections[self.using].in_atomic_block:
            raise WorkerAbortError(
                "durable command claims must run outside transaction.atomic()"
            )
        with transaction.atomic(using=self.using):
            DurableCommand.objects.using(self.using).filter(
                state=DurableCommandState.RUNNING.value,
                reconciliation_due_at__lte=now,
                max_retries__isnull=False,
                retry_count__gte=F("max_retries"),
            ).update(
                state=DurableCommandState.INDETERMINATE.value,
                reconciliation_due_at=None,
                indeterminate_reason="handler_outcome",
            )
            claimable = (
                Q(state=DurableCommandState.PENDING.value)
                | Q(
                    state=DurableCommandState.CLAIMED.value,
                    lease_expires_at__lt=now,
                )
                | Q(
                    state__in=[
                        DurableCommandState.PUBLISHED.value,
                        DurableCommandState.INDETERMINATE.value,
                    ],
                    reconciliation_due_at__lte=now,
                )
                | Q(
                    state=DurableCommandState.RUNNING.value,
                    reconciliation_due_at__lte=now,
                )
                | Q(
                    state=DurableCommandState.SETTLING.value,
                    reconciliation_due_at__lte=now,
                )
            )
            queryset = (
                DurableCommand.objects.using(self.using)
                .filter(claimable, available_at__lte=now)
                .order_by("created_at", "id")
            )
            if connections[self.using].features.has_select_for_update_skip_locked:
                queryset = queryset.select_for_update(skip_locked=True)
            else:
                queryset = queryset.select_for_update()
            model = queryset.first()
            if model is None:
                return None
            previous_state = model.state
            previous_generation = model.generation
            next_retry_count = model.retry_count
            if previous_state == DurableCommandState.RUNNING.value:
                next_retry_count += 1
            claim_values = {
                "state": DurableCommandState.CLAIMED.value,
                "generation": previous_generation + 1,
                "retry_count": next_retry_count,
                "lease_owner": worker_id,
                "lease_expires_at": lease_expires_at,
                "reconciliation_due_at": None,
                "indeterminate_reason": None,
            }
            if previous_state == DurableCommandState.RUNNING.value:
                claim_values["last_attempt_at"] = now
            if model.settlement_status is None:
                claim_values.update(
                    settlement_status=None,
                    settlement_source=None,
                    settlement_destination=None,
                )
            updated = (
                DurableCommand.objects.using(self.using)
                .filter(
                    id=model.id,
                    state=previous_state,
                    generation=previous_generation,
                )
                .update(**claim_values)
            )
            if updated != 1:
                return None
            model.refresh_from_db(using=self.using)
            return DurableCommandClaim.from_record(self._record(model))

    def mark_published(
        self,
        claim: DurableCommandClaim,
        *,
        now: datetime,
        reconciliation_due_at: datetime,
    ) -> None:
        next_state = DurableCommandState.PUBLISHED.value
        values = {
            "state": next_state,
            "reconciliation_due_at": reconciliation_due_at,
        }
        if claim.settlement_status == CommandDeliveryStatus.DEAD_LETTERED.value:
            values.update(
                state=DurableCommandState.DEAD_LETTERED.value,
                reconciliation_due_at=None,
                lease_owner=None,
                lease_expires_at=None,
                finished_at=now,
                indeterminate_reason=None,
            )
        elif claim.settlement_status is not None:
            values.update(
                settlement_status=None,
                settlement_source=None,
                settlement_destination=None,
            )
        (
            DurableCommand.objects.using(self.using)
            .filter(
                id=claim.id,
                generation=claim.generation,
                state=DurableCommandState.CLAIMED.value,
            )
            .update(**values)
        )

    def mark_publish_indeterminate(
        self,
        claim: DurableCommandClaim,
        *,
        now: datetime,
        reconciliation_due_at: datetime,
    ) -> None:
        state = (
            DurableCommandState.SETTLING.value
            if claim.settlement_status is not None
            else DurableCommandState.INDETERMINATE.value
        )
        indeterminate_reason = (
            None
            if claim.settlement_status is not None
            else "publication_acknowledgement"
        )
        (
            DurableCommand.objects.using(self.using)
            .filter(
                id=claim.id,
                generation=claim.generation,
                state=DurableCommandState.CLAIMED.value,
            )
            .update(
                state=state,
                reconciliation_due_at=reconciliation_due_at,
                indeterminate_reason=indeterminate_reason,
            )
        )

    def admit_delivery(
        self, admission: DurableDeliveryAdmission
    ) -> DurableDeliveryDecision:
        try:
            record_id = UUID(admission.record_id)
        except (TypeError, ValueError):
            return DurableDeliveryDecision.ABORT
        with transaction.atomic(using=self.using):
            model = (
                DurableCommand.objects.using(self.using)
                .select_for_update()
                .filter(id=record_id)
                .first()
            )
            if model is None or admission.generation > model.generation:
                return DurableDeliveryDecision.ABORT
            if admission.generation < model.generation or model.state in {
                DurableCommandState.SUCCEEDED.value,
                DurableCommandState.DEAD_LETTERED.value,
                DurableCommandState.RUNNING.value,
            }:
                return DurableDeliveryDecision.DROP
            if model.state not in {
                DurableCommandState.CLAIMED.value,
                DurableCommandState.PUBLISHED.value,
                DurableCommandState.SETTLING.value,
                DurableCommandState.INDETERMINATE.value,
            }:
                return DurableDeliveryDecision.ABORT
            if (
                model.state == DurableCommandState.INDETERMINATE.value
                and model.indeterminate_reason != "publication_acknowledgement"
            ):
                return DurableDeliveryDecision.DROP
            serializer = JsonSerializer()
            try:
                stored_payload = serializer.dumps(model.payload)
                admitted_payload = serializer.dumps(admission.payload)
                stored_headers = serializer.dumps(model.headers)
                admitted_headers = serializer.dumps(admission.application_headers)
            except SerializationError:
                return DurableDeliveryDecision.ABORT
            if (
                model.message_id != admission.message_id
                or model.message_type != admission.message_type
                or model.version != admission.version
                or stored_payload != admitted_payload
                or stored_headers != admitted_headers
                or model.queue != admission.source_queue
                or model.last_attempt_at != admission.last_attempt_at
            ):
                return DurableDeliveryDecision.ABORT
            if admission.retry_count < model.retry_count:
                return DurableDeliveryDecision.DROP
            if admission.retry_count > model.retry_count:
                return DurableDeliveryDecision.ABORT
            if (
                model.max_retries is not None
                and model.max_retries != admission.max_retries
            ):
                return DurableDeliveryDecision.ABORT
            model.state = DurableCommandState.RUNNING.value
            model.retry_count = admission.retry_count
            if model.max_retries is None:
                model.max_retries = admission.max_retries
            model.queue = admission.source_queue
            model.reconciliation_due_at = admission.reconciliation_due_at
            model.settlement_status = None
            model.settlement_source = None
            model.settlement_destination = None
            model.indeterminate_reason = None
            if model.started_at is None:
                model.started_at = admission.admitted_at or timezone.now()
            model.save(
                using=self.using,
                update_fields=[
                    "state",
                    "retry_count",
                    "max_retries",
                    "queue",
                    "reconciliation_due_at",
                    "settlement_status",
                    "settlement_source",
                    "settlement_destination",
                    "indeterminate_reason",
                    "started_at",
                    "updated_at",
                ],
            )
            return DurableDeliveryDecision.PROCEED

    def checkpoint_settlement(self, settlement: DurableSettlement) -> None:
        values = {
            "state": DurableCommandState.SETTLING.value,
            "retry_count": settlement.retry_count,
            "settlement_status": settlement.status.value,
            "settlement_source": settlement.source_queue,
            "settlement_destination": settlement.destination_queue,
            "queue": settlement.destination_queue or settlement.source_queue,
            "reconciliation_due_at": settlement.reconciliation_due_at,
        }
        if settlement.last_attempt_at is not None:
            values["last_attempt_at"] = settlement.last_attempt_at
        updated = (
            DurableCommand.objects.using(self.using)
            .filter(
                id=settlement.record_id,
                generation=settlement.generation,
                state=DurableCommandState.RUNNING.value,
                retry_count__lte=settlement.retry_count,
                max_retries=settlement.max_retries,
            )
            .update(**values)
        )
        if updated != 1:
            raise RuntimeError(
                "durable settlement no longer owns the active generation"
            )

    def release_delivery(
        self,
        *,
        record_id: str,
        generation: int,
        reconciliation_due_at: datetime,
    ) -> None:
        updated = (
            DurableCommand.objects.using(self.using)
            .filter(
                id=record_id,
                generation=generation,
                state=DurableCommandState.RUNNING.value,
            )
            .update(
                state=DurableCommandState.PUBLISHED.value,
                reconciliation_due_at=reconciliation_due_at,
                started_at=None,
            )
        )
        if updated != 1:
            raise RuntimeError("durable delivery admission could not be released")

    def apply_outcome(
        self,
        outcome: CommandDeliveryOutcome,
        *,
        reconciliation_due_at: datetime,
    ) -> None:
        if outcome.durable_record_id is None or outcome.durable_generation is None:
            return
        filters = {
            "id": outcome.durable_record_id,
            "generation": outcome.durable_generation,
            "message_id": outcome.message_id,
            "message_type": outcome.message_type,
            "version": outcome.version,
        }
        queryset = DurableCommand.objects.using(self.using).filter(**filters)
        if outcome.status == CommandDeliveryStatus.STARTED:
            return
        if outcome.status == CommandDeliveryStatus.SUCCEEDED:
            queryset.filter(state=DurableCommandState.RUNNING.value).update(
                state=DurableCommandState.SUCCEEDED.value,
                lease_owner=None,
                lease_expires_at=None,
                reconciliation_due_at=None,
                finished_at=timezone.now(),
            )
            return
        if outcome.status == CommandDeliveryStatus.DEAD_LETTERED:
            queryset.filter(state=DurableCommandState.SETTLING.value).update(
                state=DurableCommandState.DEAD_LETTERED.value,
                queue=outcome.destination_queue or outcome.source_queue,
                lease_owner=None,
                lease_expires_at=None,
                reconciliation_due_at=None,
                finished_at=timezone.now(),
            )
            return
        if outcome.status in {
            CommandDeliveryStatus.RETRY_SCHEDULED,
            CommandDeliveryStatus.CONTINUED,
        }:
            queryset.filter(state=DurableCommandState.SETTLING.value).update(
                state=DurableCommandState.PUBLISHED.value,
                queue=outcome.destination_queue or outcome.source_queue,
                retry_count=outcome.retry_count,
                settlement_status=None,
                settlement_source=None,
                settlement_destination=None,
                reconciliation_due_at=reconciliation_due_at,
            )

    @staticmethod
    def _record(model: DurableCommand) -> DurableCommandRecord:
        return DurableCommandRecord(
            id=str(model.id),
            message_id=model.message_id,
            message_type=model.message_type,
            version=model.version,
            payload=model.payload,
            headers=dict(model.headers),
            created_at=model.created_at,
            available_at=model.available_at,
            queue=model.queue,
            fingerprint=model.fingerprint,
            idempotency_key=model.idempotency_key,
            state=DurableCommandState(model.state),
            generation=model.generation,
            retry_count=model.retry_count,
            lease_owner=model.lease_owner,
            lease_expires_at=model.lease_expires_at,
            reconciliation_due_at=model.reconciliation_due_at,
            max_retries=model.max_retries,
            settlement_status=model.settlement_status,
            settlement_source=model.settlement_source,
            settlement_destination=model.settlement_destination,
            started_at=model.started_at,
            finished_at=model.finished_at,
            last_attempt_at=model.last_attempt_at,
        )


__all__ = ["DjangoDurableCommandStore"]
