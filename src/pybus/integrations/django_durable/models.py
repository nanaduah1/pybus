from __future__ import annotations

from django.db import models

from pybus.durable import DurableCommandState


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
