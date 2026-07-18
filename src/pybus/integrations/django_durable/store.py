from __future__ import annotations

from datetime import datetime, timezone as datetime_timezone
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from django.db import IntegrityError, connections, transaction
from django.db.models import F, Q
from django.utils import timezone

from pybus.delivery import CommandDeliveryOutcome, CommandDeliveryStatus
from pybus.durable import (
    DurableJobClaim,
    DurableJobDraft,
    DurableJobRecord,
    DurableJobState,
    DurableDeliveryAdmission,
    DurableDeliveryDecision,
    DurableSettlement,
)
from pybus.exceptions import (
    DurableJobConflictError,
    InvalidMessageDefinitionError,
    JobSeriesNotFoundError,
    SerializationError,
    WorkerAbortError,
)
from pybus.integrations.django_durable.models import (
    DurableJob,
    JobSeries,
)
from pybus.recurrence import (
    EndRecurrence,
    Recurrence,
    RecurrenceCadence,
    JobSeriesDraft,
    JobSeriesRecord,
    JobSeriesState,
    ScheduleNextOccurrence,
    next_cadence_at,
)
from pybus.serializer import JsonSerializer


class DjangoDurableJobStore:
    """Durable job state stored in an explicitly selected Django database."""

    def __init__(self, *, using: str = "default") -> None:
        connections[using]
        self.using = using

    def schedule(self, draft: DurableJobDraft) -> DurableJobRecord:
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
                model = DurableJob.objects.using(self.using).create(**values)
        except IntegrityError:
            if draft.idempotency_key is None:
                raise
            model = DurableJob.objects.using(self.using).get(
                idempotency_key=draft.idempotency_key
            )
            if model.fingerprint != draft.fingerprint:
                raise DurableJobConflictError(
                    f"idempotency key {draft.idempotency_key!r} already identifies "
                    "a different durable job"
                )
        return self._record(model)

    def schedule_recurring(
        self, draft: JobSeriesDraft
    ) -> tuple[JobSeriesRecord, DurableJobRecord]:
        first = draft.first_occurrence
        zone = ZoneInfo(draft.recurrence.timezone)
        series_values = {
            "id": uuid4(),
            "message_type": first.message_type,
            "version": first.version,
            "payload": first.payload,
            "headers": first.headers,
            "queue": first.queue,
            "cadence": draft.recurrence.cadence.value,
            "timezone": draft.recurrence.timezone,
            "starts_at": draft.starts_at,
            "anchor_local": draft.starts_at.astimezone(zone)
            .replace(tzinfo=None)
            .isoformat(),
            "ends_at": draft.recurrence.ends_at,
            "fingerprint": draft.fingerprint,
            "idempotency_key": draft.idempotency_key,
            "created_at": first.created_at,
        }
        try:
            with transaction.atomic(using=self.using):
                series = JobSeries.objects.using(self.using).create(**series_values)
                occurrence = DurableJob.objects.using(self.using).create(
                    id=uuid4(),
                    message_id=first.message_id,
                    message_type=first.message_type,
                    version=first.version,
                    payload=first.payload,
                    headers=first.headers,
                    created_at=first.created_at,
                    available_at=first.available_at,
                    queue=first.queue,
                    fingerprint=first.fingerprint,
                    idempotency_key=first.idempotency_key,
                    series=series,
                    occurrence_number=1,
                )
        except IntegrityError:
            if draft.idempotency_key is None:
                raise
            series = (
                JobSeries.objects.using(self.using)
                .filter(idempotency_key=draft.idempotency_key)
                .first()
            )
            if series is None:
                raise DurableJobConflictError(
                    f"idempotency key {draft.idempotency_key!r} already identifies "
                    "a one-off durable job"
                )
            if series.fingerprint != draft.fingerprint:
                raise DurableJobConflictError(
                    f"idempotency key {draft.idempotency_key!r} already identifies "
                    "a different recurring durable job"
                )
            occurrence = series.occurrences.using(self.using).get(
                occurrence_number=series.latest_occurrence_number
            )
        return self._series_record(series, occurrence), self._record(occurrence)

    def cancel_recurring(
        self, *, series_id: str, cancelled_at: datetime
    ) -> JobSeriesRecord:
        try:
            parsed_id = UUID(series_id)
        except (TypeError, ValueError) as exc:
            raise JobSeriesNotFoundError(series_id) from exc
        with transaction.atomic(using=self.using):
            series = (
                JobSeries.objects.using(self.using)
                .select_for_update()
                .filter(id=parsed_id)
                .first()
            )
            if series is None:
                raise JobSeriesNotFoundError(series_id)
            occurrence = (
                DurableJob.objects.using(self.using)
                .select_for_update()
                .get(
                    series=series,
                    occurrence_number=series.latest_occurrence_number,
                )
            )
            if series.state == JobSeriesState.ACTIVE.value:
                series.state = JobSeriesState.CANCELLED.value
                series.finished_at = cancelled_at
                series.save(
                    using=self.using,
                    update_fields=["state", "finished_at", "updated_at"],
                )
                if occurrence.state in {
                    DurableJobState.PENDING.value,
                    DurableJobState.CLAIMED.value,
                    DurableJobState.PUBLISHED.value,
                    DurableJobState.SETTLING.value,
                    DurableJobState.INDETERMINATE.value,
                }:
                    occurrence.state = DurableJobState.CANCELLED.value
                    occurrence.lease_owner = None
                    occurrence.lease_expires_at = None
                    occurrence.reconciliation_due_at = None
                    occurrence.finished_at = cancelled_at
                    occurrence.save(
                        using=self.using,
                        update_fields=[
                            "state",
                            "lease_owner",
                            "lease_expires_at",
                            "reconciliation_due_at",
                            "finished_at",
                            "updated_at",
                        ],
                    )
            return self._series_record(series, occurrence)

    def complete_recurring_success(
        self,
        outcome: CommandDeliveryOutcome,
        handler_result: object,
        *,
        completed_at: datetime,
    ) -> bool:
        if outcome.durable_record_id is None or outcome.durable_generation is None:
            return False
        snapshot = (
            DurableJob.objects.using(self.using)
            .filter(
                id=outcome.durable_record_id,
                generation=outcome.durable_generation,
                message_id=outcome.message_id,
                message_type=outcome.message_type,
                version=outcome.version,
            )
            .values("series_id")
            .first()
        )
        if snapshot is None or snapshot["series_id"] is None:
            return False
        with transaction.atomic(using=self.using):
            series = (
                JobSeries.objects.using(self.using)
                .select_for_update()
                .get(id=snapshot["series_id"])
            )
            occurrence = (
                DurableJob.objects.using(self.using)
                .select_for_update()
                .filter(
                    id=outcome.durable_record_id,
                    generation=outcome.durable_generation,
                    message_id=outcome.message_id,
                    message_type=outcome.message_type,
                    version=outcome.version,
                )
                .first()
            )
            if occurrence is None:
                return True
            if occurrence.state != DurableJobState.RUNNING.value:
                return True
            if handler_result is not None and not isinstance(
                handler_result, (ScheduleNextOccurrence, EndRecurrence)
            ):
                raise InvalidMessageDefinitionError(
                    "recurring command handlers must return None, "
                    "ScheduleNextOccurrence, EndRecurrence, or ContinueProcessing"
                )

            occurrence.state = DurableJobState.SUCCEEDED.value
            occurrence.lease_owner = None
            occurrence.lease_expires_at = None
            occurrence.reconciliation_due_at = None
            occurrence.finished_at = completed_at
            occurrence.save(
                using=self.using,
                update_fields=[
                    "state",
                    "lease_owner",
                    "lease_expires_at",
                    "reconciliation_due_at",
                    "finished_at",
                    "updated_at",
                ],
            )
            if series.state != JobSeriesState.ACTIVE.value:
                return True
            if isinstance(handler_result, EndRecurrence):
                self._finish_series(
                    series,
                    state=JobSeriesState.COMPLETED,
                    finished_at=completed_at,
                )
                return True

            if isinstance(handler_result, ScheduleNextOccurrence):
                next_at = handler_result.at.astimezone(datetime_timezone.utc)
                if next_at <= completed_at.astimezone(datetime_timezone.utc):
                    raise InvalidMessageDefinitionError(
                        "next occurrence must be strictly after completion"
                    )
                if series.ends_at is not None and next_at >= series.ends_at:
                    self._finish_series(
                        series,
                        state=JobSeriesState.COMPLETED,
                        finished_at=completed_at,
                    )
                    return True
            else:
                next_at = next_cadence_at(
                    self._recurrence(series),
                    starts_at=series.starts_at,
                    after=completed_at,
                )
                if next_at is None:
                    self._finish_series(
                        series,
                        state=JobSeriesState.COMPLETED,
                        finished_at=completed_at,
                    )
                    return True

            next_number = series.latest_occurrence_number + 1
            DurableJob.objects.using(self.using).create(
                id=uuid4(),
                message_id=str(uuid4()),
                message_type=series.message_type,
                version=series.version,
                payload=series.payload,
                headers=series.headers,
                created_at=completed_at,
                available_at=next_at,
                queue=series.queue,
                fingerprint=series.fingerprint,
                idempotency_key=None,
                series=series,
                occurrence_number=next_number,
            )
            series.latest_occurrence_number = next_number
            series.save(
                using=self.using,
                update_fields=["latest_occurrence_number", "updated_at"],
            )
            return True

    def claim(
        self, *, worker_id: str, now: datetime, lease_expires_at: datetime
    ) -> DurableJobClaim | None:
        if connections[self.using].in_atomic_block:
            raise WorkerAbortError(
                "durable job claims must run outside transaction.atomic()"
            )
        with transaction.atomic(using=self.using):
            cancelled_reclaimable = Q(
                state=DurableJobState.CLAIMED.value,
                lease_expires_at__lt=now,
            ) | Q(
                state__in=[
                    DurableJobState.PUBLISHED.value,
                    DurableJobState.INDETERMINATE.value,
                    DurableJobState.RUNNING.value,
                    DurableJobState.SETTLING.value,
                ],
                reconciliation_due_at__lte=now,
            )
            DurableJob.objects.using(self.using).filter(
                cancelled_reclaimable,
                series__state=JobSeriesState.CANCELLED.value,
            ).update(
                state=DurableJobState.CANCELLED.value,
                lease_owner=None,
                lease_expires_at=None,
                reconciliation_due_at=None,
                finished_at=now,
            )
            DurableJob.objects.using(self.using).filter(
                Q(series__isnull=True) | Q(series__state=JobSeriesState.ACTIVE.value),
                state=DurableJobState.RUNNING.value,
                reconciliation_due_at__lte=now,
                max_retries__isnull=False,
                retry_count__gte=F("max_retries"),
            ).update(
                state=DurableJobState.INDETERMINATE.value,
                reconciliation_due_at=None,
                indeterminate_reason="handler_outcome",
            )
            claimable = (
                Q(state=DurableJobState.PENDING.value)
                | Q(
                    state=DurableJobState.CLAIMED.value,
                    lease_expires_at__lt=now,
                )
                | Q(
                    state__in=[
                        DurableJobState.PUBLISHED.value,
                        DurableJobState.INDETERMINATE.value,
                    ],
                    reconciliation_due_at__lte=now,
                )
                | Q(
                    state=DurableJobState.RUNNING.value,
                    reconciliation_due_at__lte=now,
                )
                | Q(
                    state=DurableJobState.SETTLING.value,
                    reconciliation_due_at__lte=now,
                )
            )
            queryset = (
                DurableJob.objects.using(self.using)
                .filter(
                    claimable,
                    Q(series__isnull=True)
                    | Q(series__state=JobSeriesState.ACTIVE.value),
                    available_at__lte=now,
                )
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
            if previous_state == DurableJobState.RUNNING.value:
                next_retry_count += 1
            claim_values = {
                "state": DurableJobState.CLAIMED.value,
                "generation": previous_generation + 1,
                "retry_count": next_retry_count,
                "lease_owner": worker_id,
                "lease_expires_at": lease_expires_at,
                "reconciliation_due_at": None,
                "indeterminate_reason": None,
            }
            if previous_state == DurableJobState.RUNNING.value:
                claim_values["last_attempt_at"] = now
            if model.settlement_status is None:
                claim_values.update(
                    settlement_status=None,
                    settlement_source=None,
                    settlement_destination=None,
                )
            updated = (
                DurableJob.objects.using(self.using)
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
            return DurableJobClaim.from_record(self._record(model))

    def mark_published(
        self,
        claim: DurableJobClaim,
        *,
        now: datetime,
        reconciliation_due_at: datetime,
    ) -> None:
        if claim.settlement_status == CommandDeliveryStatus.DEAD_LETTERED.value:
            self._finish_dead_letter_from_claim(claim, finished_at=now)
            return
        next_state = DurableJobState.PUBLISHED.value
        values = {
            "state": next_state,
            "reconciliation_due_at": reconciliation_due_at,
        }
        if claim.settlement_status is not None:
            values.update(
                settlement_status=None,
                settlement_source=None,
                settlement_destination=None,
            )
        (
            DurableJob.objects.using(self.using)
            .filter(
                id=claim.id,
                generation=claim.generation,
                state=DurableJobState.CLAIMED.value,
            )
            .update(**values)
        )

    def mark_publish_indeterminate(
        self,
        claim: DurableJobClaim,
        *,
        now: datetime,
        reconciliation_due_at: datetime,
    ) -> None:
        state = (
            DurableJobState.SETTLING.value
            if claim.settlement_status is not None
            else DurableJobState.INDETERMINATE.value
        )
        indeterminate_reason = (
            None
            if claim.settlement_status is not None
            else "publication_acknowledgement"
        )
        (
            DurableJob.objects.using(self.using)
            .filter(
                id=claim.id,
                generation=claim.generation,
                state=DurableJobState.CLAIMED.value,
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
        snapshot = (
            DurableJob.objects.using(self.using)
            .filter(id=record_id)
            .values("series_id")
            .first()
        )
        if snapshot is None:
            return DurableDeliveryDecision.ABORT
        with transaction.atomic(using=self.using):
            series = self._lock_series(snapshot["series_id"])
            model = (
                DurableJob.objects.using(self.using)
                .select_for_update()
                .filter(id=record_id)
                .first()
            )
            if model is None or admission.generation > model.generation:
                return DurableDeliveryDecision.ABORT
            if admission.generation < model.generation or model.state in {
                DurableJobState.SUCCEEDED.value,
                DurableJobState.DEAD_LETTERED.value,
                DurableJobState.CANCELLED.value,
                DurableJobState.RUNNING.value,
            }:
                return DurableDeliveryDecision.DROP
            if series is not None and series.state != JobSeriesState.ACTIVE.value:
                if (
                    series.state == JobSeriesState.CANCELLED.value
                    and model.state
                    not in {
                        DurableJobState.SUCCEEDED.value,
                        DurableJobState.DEAD_LETTERED.value,
                        DurableJobState.CANCELLED.value,
                    }
                ):
                    model.state = DurableJobState.CANCELLED.value
                    model.lease_owner = None
                    model.lease_expires_at = None
                    model.reconciliation_due_at = None
                    model.finished_at = timezone.now()
                    model.save(
                        using=self.using,
                        update_fields=[
                            "state",
                            "lease_owner",
                            "lease_expires_at",
                            "reconciliation_due_at",
                            "finished_at",
                            "updated_at",
                        ],
                    )
                return DurableDeliveryDecision.DROP
            if model.state not in {
                DurableJobState.CLAIMED.value,
                DurableJobState.PUBLISHED.value,
                DurableJobState.SETTLING.value,
                DurableJobState.INDETERMINATE.value,
            }:
                return DurableDeliveryDecision.ABORT
            if (
                model.state == DurableJobState.INDETERMINATE.value
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
            model.state = DurableJobState.RUNNING.value
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
            "state": DurableJobState.SETTLING.value,
            "retry_count": settlement.retry_count,
            "settlement_status": settlement.status.value,
            "settlement_source": settlement.source_queue,
            "settlement_destination": settlement.destination_queue,
            "queue": settlement.destination_queue or settlement.source_queue,
            "reconciliation_due_at": settlement.reconciliation_due_at,
        }
        if settlement.last_attempt_at is not None:
            values["last_attempt_at"] = settlement.last_attempt_at
        updated = self._guarded_series_update(
            filters={
                "id": settlement.record_id,
                "generation": settlement.generation,
                "retry_count__lte": settlement.retry_count,
                "max_retries": settlement.max_retries,
            },
            expected_state=DurableJobState.RUNNING,
            values=values,
            cancelled_at=timezone.now(),
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
        updated = self._guarded_series_update(
            filters={"id": record_id, "generation": generation},
            expected_state=DurableJobState.RUNNING,
            values={
                "state": DurableJobState.PUBLISHED.value,
                "reconciliation_due_at": reconciliation_due_at,
                "started_at": None,
            },
            cancelled_at=timezone.now(),
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
        queryset = DurableJob.objects.using(self.using).filter(**filters)
        if outcome.status == CommandDeliveryStatus.STARTED:
            return
        if outcome.status == CommandDeliveryStatus.SUCCEEDED:
            queryset.filter(state=DurableJobState.RUNNING.value).update(
                state=DurableJobState.SUCCEEDED.value,
                lease_owner=None,
                lease_expires_at=None,
                reconciliation_due_at=None,
                finished_at=timezone.now(),
            )
            return
        if outcome.status == CommandDeliveryStatus.DEAD_LETTERED:
            self._finish_dead_letter_from_outcome(outcome)
            return
        if outcome.status in {
            CommandDeliveryStatus.RETRY_SCHEDULED,
            CommandDeliveryStatus.CONTINUED,
        }:
            self._guarded_series_update(
                filters=filters,
                expected_state=DurableJobState.SETTLING,
                values={
                    "state": DurableJobState.PUBLISHED.value,
                    "queue": outcome.destination_queue or outcome.source_queue,
                    "retry_count": outcome.retry_count,
                    "settlement_status": None,
                    "settlement_source": None,
                    "settlement_destination": None,
                    "reconciliation_due_at": reconciliation_due_at,
                },
                cancelled_at=timezone.now(),
            )

    @staticmethod
    def _record(model: DurableJob) -> DurableJobRecord:
        return DurableJobRecord(
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
            state=DurableJobState(model.state),
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
            series_id=(None if model.series_id is None else str(model.series_id)),
            occurrence_number=model.occurrence_number,
        )

    @staticmethod
    def _recurrence(series: JobSeries) -> Recurrence:
        return Recurrence(
            cadence=RecurrenceCadence(series.cadence),
            timezone=series.timezone,
            ends_at=series.ends_at,
        )

    def _series_record(
        self,
        series: JobSeries,
        occurrence: DurableJob,
    ) -> JobSeriesRecord:
        return JobSeriesRecord(
            id=str(series.id),
            state=JobSeriesState(series.state),
            cadence=RecurrenceCadence(series.cadence),
            timezone=series.timezone,
            starts_at=series.starts_at,
            ends_at=series.ends_at,
            fingerprint=series.fingerprint,
            idempotency_key=series.idempotency_key,
            latest_occurrence_number=series.latest_occurrence_number,
            latest_occurrence_id=str(occurrence.id),
            latest_message_id=occurrence.message_id,
            latest_run_at=occurrence.available_at,
            created_at=series.created_at,
            finished_at=series.finished_at,
        )

    def _finish_series(
        self,
        series: JobSeries,
        *,
        state: JobSeriesState,
        finished_at: datetime,
    ) -> None:
        series.state = state.value
        series.finished_at = finished_at
        series.save(
            using=self.using,
            update_fields=["state", "finished_at", "updated_at"],
        )

    def _guarded_series_update(
        self,
        *,
        filters: dict[str, object],
        expected_state: DurableJobState,
        values: dict[str, object],
        cancelled_at: datetime,
    ) -> int:
        snapshot = (
            DurableJob.objects.using(self.using)
            .filter(**filters)
            .values("series_id")
            .first()
        )
        if snapshot is None:
            return 0
        if snapshot["series_id"] is None:
            return (
                DurableJob.objects.using(self.using)
                .filter(
                    **filters,
                    state=expected_state.value,
                )
                .update(**values)
            )
        with transaction.atomic(using=self.using):
            series = self._lock_series(snapshot["series_id"])
            occurrence = (
                DurableJob.objects.using(self.using)
                .select_for_update()
                .filter(
                    **filters,
                    state=expected_state.value,
                )
                .first()
            )
            if occurrence is None:
                return 0
            if series.state == JobSeriesState.CANCELLED.value:
                self._cancel_occurrence(occurrence, cancelled_at=cancelled_at)
                return 1
            if series.state != JobSeriesState.ACTIVE.value:
                return 0
            for field, value in values.items():
                setattr(occurrence, field, value)
            occurrence.save(
                using=self.using,
                update_fields=[*values, "updated_at"],
            )
            return 1

    def _cancel_occurrence(
        self,
        occurrence: DurableJob,
        *,
        cancelled_at: datetime,
    ) -> None:
        occurrence.state = DurableJobState.CANCELLED.value
        occurrence.lease_owner = None
        occurrence.lease_expires_at = None
        occurrence.reconciliation_due_at = None
        occurrence.finished_at = cancelled_at
        occurrence.save(
            using=self.using,
            update_fields=[
                "state",
                "lease_owner",
                "lease_expires_at",
                "reconciliation_due_at",
                "finished_at",
                "updated_at",
            ],
        )

    def _finish_dead_letter_from_claim(
        self,
        claim: DurableJobClaim,
        *,
        finished_at: datetime,
    ) -> None:
        snapshot = (
            DurableJob.objects.using(self.using)
            .filter(id=claim.id)
            .values("series_id")
            .first()
        )
        if snapshot is None:
            return
        with transaction.atomic(using=self.using):
            series = self._lock_series(snapshot["series_id"])
            occurrence = (
                DurableJob.objects.using(self.using)
                .select_for_update()
                .filter(
                    id=claim.id,
                    generation=claim.generation,
                    state=DurableJobState.CLAIMED.value,
                )
                .first()
            )
            if occurrence is None:
                return
            occurrence.state = DurableJobState.DEAD_LETTERED.value
            occurrence.reconciliation_due_at = None
            occurrence.lease_owner = None
            occurrence.lease_expires_at = None
            occurrence.finished_at = finished_at
            occurrence.indeterminate_reason = None
            occurrence.save(
                using=self.using,
                update_fields=[
                    "state",
                    "reconciliation_due_at",
                    "lease_owner",
                    "lease_expires_at",
                    "finished_at",
                    "indeterminate_reason",
                    "updated_at",
                ],
            )
            self._fail_active_series(series, finished_at=finished_at)

    def _finish_dead_letter_from_outcome(self, outcome: CommandDeliveryOutcome) -> None:
        snapshot = (
            DurableJob.objects.using(self.using)
            .filter(
                id=outcome.durable_record_id,
                generation=outcome.durable_generation,
                message_id=outcome.message_id,
                message_type=outcome.message_type,
                version=outcome.version,
            )
            .values("series_id")
            .first()
        )
        if snapshot is None:
            return
        finished_at = timezone.now()
        with transaction.atomic(using=self.using):
            series = self._lock_series(snapshot["series_id"])
            occurrence = (
                DurableJob.objects.using(self.using)
                .select_for_update()
                .filter(
                    id=outcome.durable_record_id,
                    generation=outcome.durable_generation,
                    state=DurableJobState.SETTLING.value,
                )
                .first()
            )
            if occurrence is None:
                return
            occurrence.state = DurableJobState.DEAD_LETTERED.value
            occurrence.queue = outcome.destination_queue or outcome.source_queue
            occurrence.lease_owner = None
            occurrence.lease_expires_at = None
            occurrence.reconciliation_due_at = None
            occurrence.finished_at = finished_at
            occurrence.save(
                using=self.using,
                update_fields=[
                    "state",
                    "queue",
                    "lease_owner",
                    "lease_expires_at",
                    "reconciliation_due_at",
                    "finished_at",
                    "updated_at",
                ],
            )
            self._fail_active_series(series, finished_at=finished_at)

    def _lock_series(self, series_id):
        if series_id is None:
            return None
        return JobSeries.objects.using(self.using).select_for_update().get(id=series_id)

    def _fail_active_series(self, series, *, finished_at: datetime) -> None:
        if series is not None and series.state == JobSeriesState.ACTIVE.value:
            self._finish_series(
                series,
                state=JobSeriesState.FAILED,
                finished_at=finished_at,
            )


DjangoDurableCommandStore = DjangoDurableJobStore

__all__ = ["DjangoDurableCommandStore", "DjangoDurableJobStore"]
