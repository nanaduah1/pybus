def test_pybus_imports():
    import pybus

    assert pybus is not None
    assert pybus.DEFAULT_QUEUE_NAME == "skuulbe.jobs"
    assert pybus.DEFAULT_FAILED_QUEUE_NAME == "skuulbe.jobs.failed"
    assert pybus.DEFAULT_SLOW_QUEUE_NAME == "skuulbe.jobs.slow"
