from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [("pybus_durable", "0002_recurring_commands")]
    operations = [
        migrations.RenameModel(
            old_name="DurableCommand",
            new_name="DurableJob",
        ),
        migrations.RenameModel(
            old_name="RecurringCommandSeries",
            new_name="JobSeries",
        ),
    ]
