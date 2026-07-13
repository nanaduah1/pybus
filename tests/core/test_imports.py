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
