from django.db import migrations, models
import django.db.models.deletion


def refuse_recurrence_reverse(apps, schema_editor):
    series = apps.get_model("pybus_durable", "RecurringCommandSeries")
    command = apps.get_model("pybus_durable", "DurableCommand")
    using = schema_editor.connection.alias
    if (
        series.objects.using(using).exists()
        or command.objects.using(using).filter(series_id__isnull=False).exists()
    ):
        raise RuntimeError(
            "Refusing to remove Pybus recurring-command storage while it contains data"
        )


class Migration(migrations.Migration):
    dependencies = [("pybus_durable", "0001_initial")]
    operations = [
        migrations.CreateModel(
            name="RecurringCommandSeries",
            fields=[
                (
                    "id",
                    models.UUIDField(editable=False, primary_key=True, serialize=False),
                ),
                ("message_type", models.CharField(max_length=255)),
                ("version", models.PositiveIntegerField()),
                ("payload", models.JSONField()),
                ("headers", models.JSONField(default=dict)),
                ("queue", models.CharField(max_length=255)),
                ("cadence", models.CharField(max_length=16)),
                ("timezone", models.CharField(max_length=255)),
                ("starts_at", models.DateTimeField()),
                ("anchor_local", models.CharField(max_length=32)),
                ("ends_at", models.DateTimeField(null=True)),
                ("fingerprint", models.TextField()),
                (
                    "idempotency_key",
                    models.CharField(max_length=255, null=True, unique=True),
                ),
                ("state", models.CharField(default="active", max_length=32)),
                ("latest_occurrence_number", models.PositiveBigIntegerField(default=1)),
                ("created_at", models.DateTimeField()),
                ("finished_at", models.DateTimeField(null=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"db_table": "pybus_recurring_command_series"},
        ),
        migrations.AddField(
            model_name="durablecommand",
            name="occurrence_number",
            field=models.PositiveBigIntegerField(null=True),
        ),
        migrations.AddField(
            model_name="durablecommand",
            name="series",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="occurrences",
                to="pybus_durable.recurringcommandseries",
            ),
        ),
        migrations.AddConstraint(
            model_name="durablecommand",
            constraint=models.UniqueConstraint(
                fields=("series", "occurrence_number"),
                name="pybus_series_occurrence_uniq",
            ),
        ),
        migrations.AddConstraint(
            model_name="durablecommand",
            constraint=models.UniqueConstraint(
                condition=models.Q(
                    state__in=[
                        "pending",
                        "claimed",
                        "published",
                        "running",
                        "settling",
                        "indeterminate",
                    ]
                ),
                fields=("series",),
                name="pybus_series_active_occurrence_uniq",
            ),
        ),
        migrations.RunPython(migrations.RunPython.noop, refuse_recurrence_reverse),
    ]
