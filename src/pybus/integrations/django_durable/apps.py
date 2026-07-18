from django.apps import AppConfig


class PybusDurableConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "pybus.integrations.django_durable"
    label = "pybus_durable"
    verbose_name = "Pybus durable commands"
