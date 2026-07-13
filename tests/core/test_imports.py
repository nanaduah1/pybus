from pathlib import Path
import subprocess
import sys
import textwrap


def test_pybus_imports():
    import pybus

    assert pybus is not None
    assert pybus.DEFAULT_QUEUE_NAME == "pybus.jobs"
    assert pybus.DEFAULT_FAILED_QUEUE_NAME == "pybus.jobs.failed"
    assert pybus.DEFAULT_SLOW_QUEUE_NAME == "pybus.jobs.slow"
    assert pybus.Pybus is not None
    assert pybus.PayloadCodec is not None
    assert pybus.PayloadTypeRegistry is not None
    assert pybus.PythonPayloadCodec is not None
    assert pybus.configure_transport is not None
    assert pybus.publish_event is not None
    assert pybus.send_command is not None
    assert pybus.request is not None
    assert pybus.event_handler is not None
    assert pybus.command_handler is not None
    assert pybus.request_handler is not None
    assert pybus.configure_scheduler is not None
    assert pybus.get_scheduler is not None
    assert pybus.scheduled is not None
    assert pybus.Worker is not None
    assert pybus.WorkerHook is not None


def test_core_imports_do_not_load_optional_django_or_redis() -> None:
    source_root = Path(__file__).resolve().parents[2] / "src"
    script = textwrap.dedent(
        f"""
        import sys

        class BlockOptionalImports:
            def find_spec(self, fullname, path=None, target=None):
                if fullname.split('.', 1)[0] in {{'django', 'redis'}}:
                    raise AssertionError(f'optional dependency imported: {{fullname}}')
                return None

        sys.meta_path.insert(0, BlockOptionalImports())
        sys.path.insert(0, {str(source_root)!r})
        import pybus
        import pybus.worker
        assert pybus.Worker is pybus.worker.Worker
        """
    )

    subprocess.run([sys.executable, "-I", "-c", script], check=True)
