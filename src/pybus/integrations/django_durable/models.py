from __future__ import annotations

from django.db import models

from pybus.durable import DurableCommandState
from pybus.recurrence import RecurringCommandSeriesState


class RecurringCommandSeries(models.Model):
    id = models.UUIDField(primary_key=True, editable=False)
    message_type = models.CharField(max_length=255)
    version = models.PositiveIntegerField()
    payload = models.JSONField()
    headers = models.JSONField(default=dict)
    queue = models.CharField(max_length=255)
    cadence = models.CharField(max_length=16)
    timezone = models.CharField(max_length=255)
    starts_at = models.DateTimeField()
    anchor_local = models.CharField(max_length=32)
    ends_at = models.DateTimeField(null=True)
    fingerprint = models.TextField()
    idempotency_key = models.CharField(max_length=255, null=True, unique=True)
    state = models.CharField(
        max_length=32,
        default=RecurringCommandSeriesState.ACTIVE.value,
    )
    latest_occurrence_number = models.PositiveBigIntegerField(default=1)
    created_at = models.DateTimeField()
    finished_at = models.DateTimeField(null=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "pybus_durable"
        db_table = "pybus_recurring_command_series"


class DurableCommand(models.Model):
    id = models.UUIDField(primary_key=True, editable=False)
    message_id = models.CharField(max_length=255, unique=True)
    message_type = models.CharField(max_length=255)
    version = models.PositiveIntegerField()
    payload = models.JSONField()
    headers = models.JSONField(default=dict)
    created_at = models.DateTimeField()
    available_at = models.DateTimeField()
    queue = models.CharField(max_length=255)
    fingerprint = models.TextField()
    idempotency_key = models.CharField(max_length=255, null=True, unique=True)
    series = models.ForeignKey(
        RecurringCommandSeries,
        null=True,
        on_delete=models.PROTECT,
        related_name="occurrences",
    )
    occurrence_number = models.PositiveBigIntegerField(null=True)
    state = models.CharField(max_length=32, default=DurableCommandState.PENDING.value)
    generation = models.PositiveBigIntegerField(default=0)
    retry_count = models.PositiveIntegerField(default=0)
    max_retries = models.PositiveIntegerField(null=True)
    lease_owner = models.CharField(max_length=255, null=True)
    lease_expires_at = models.DateTimeField(null=True)
    reconciliation_due_at = models.DateTimeField(null=True)
    indeterminate_reason = models.CharField(max_length=32, null=True)
    settlement_status = models.CharField(max_length=32, null=True)
    settlement_source = models.CharField(max_length=255, null=True)
    settlement_destination = models.CharField(max_length=255, null=True)
    last_attempt_at = models.DateTimeField(null=True)
    started_at = models.DateTimeField(null=True)
    finished_at = models.DateTimeField(null=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "pybus_durable"
        db_table = "pybus_durable_command"
        indexes = [
            models.Index(
                fields=["state", "lease_expires_at"],
                name="pybus_durable_claim_idx",
            ),
            models.Index(
                fields=["state", "reconciliation_due_at"],
                name="pybus_durable_recon_idx",
            ),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["series", "occurrence_number"],
                name="pybus_series_occurrence_uniq",
            ),
            models.UniqueConstraint(
                fields=["series"],
                condition=models.Q(
                    state__in=[
                        DurableCommandState.PENDING.value,
                        DurableCommandState.CLAIMED.value,
                        DurableCommandState.PUBLISHED.value,
                        DurableCommandState.RUNNING.value,
                        DurableCommandState.SETTLING.value,
                        DurableCommandState.INDETERMINATE.value,
                    ]
                ),
                name="pybus_series_active_occurrence_uniq",
            ),
        ]
