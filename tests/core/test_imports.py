def test_pybus_imports():
    import pybus

    assert pybus is not None
    assert pybus.DEFAULT_QUEUE_NAME == "skuulbe.jobs"
    assert pybus.DEFAULT_FAILED_QUEUE_NAME == "skuulbe.jobs.failed"
    assert pybus.DEFAULT_SLOW_QUEUE_NAME == "skuulbe.jobs.slow"
    assert pybus.event_handler is not None
    assert pybus.command_handler is not None
    assert pybus.request_handler is not None
