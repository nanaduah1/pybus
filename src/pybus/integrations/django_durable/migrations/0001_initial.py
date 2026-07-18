from django.db import migrations, models


def refuse_nonempty_reverse(apps, schema_editor):
    model = apps.get_model("pybus_durable", "DurableCommand")
    if model.objects.using(schema_editor.connection.alias).exists():
        raise RuntimeError(
            "Refusing to remove the Pybus durable command table while it contains data"
        )


class Migration(migrations.Migration):
    initial = True
    dependencies = []
    operations = [
        migrations.CreateModel(
            name="DurableCommand",
            fields=[
                (
                    "id",
                    models.UUIDField(editable=False, primary_key=True, serialize=False),
                ),
                ("message_id", models.CharField(max_length=255, unique=True)),
                ("message_type", models.CharField(max_length=255)),
                ("version", models.PositiveIntegerField()),
                ("payload", models.JSONField()),
                ("headers", models.JSONField(default=dict)),
                ("created_at", models.DateTimeField()),
                ("available_at", models.DateTimeField()),
                ("queue", models.CharField(max_length=255)),
                ("fingerprint", models.TextField()),
                (
                    "idempotency_key",
                    models.CharField(max_length=255, null=True, unique=True),
                ),
                ("state", models.CharField(default="pending", max_length=32)),
                ("generation", models.PositiveBigIntegerField(default=0)),
                ("retry_count", models.PositiveIntegerField(default=0)),
                ("max_retries", models.PositiveIntegerField(null=True)),
                ("lease_owner", models.CharField(max_length=255, null=True)),
                ("lease_expires_at", models.DateTimeField(null=True)),
                ("reconciliation_due_at", models.DateTimeField(null=True)),
                ("indeterminate_reason", models.CharField(max_length=32, null=True)),
                ("settlement_status", models.CharField(max_length=32, null=True)),
                ("settlement_source", models.CharField(max_length=255, null=True)),
                ("settlement_destination", models.CharField(max_length=255, null=True)),
                ("last_attempt_at", models.DateTimeField(null=True)),
                ("started_at", models.DateTimeField(null=True)),
                ("finished_at", models.DateTimeField(null=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"db_table": "pybus_durable_command"},
        ),
        migrations.AddIndex(
            model_name="durablecommand",
            index=models.Index(
                fields=["state", "lease_expires_at"], name="pybus_durable_claim_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="durablecommand",
            index=models.Index(
                fields=["state", "reconciliation_due_at"],
                name="pybus_durable_recon_idx",
            ),
        ),
        migrations.RunPython(migrations.RunPython.noop, refuse_nonempty_reverse),
    ]
