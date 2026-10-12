# File: sdk/python/tests/test_integration_windows.py
# Author: Hadi Cahyadi <cumulus13@gmail.com>
# Date: 2026-10-12
# Description: End-to-end tests of the COM transport against a RUNNING mqcache-server (skipped unless configured).
# License: MIT
"""
Run on Windows with MSMQ and a server already started, e.g.:

    mqcache-server.exe conf\\mqcache.conf --queue private$\\mqcache.pytest
    set MQCACHE_TEST_ENDPOINT=.
    set MQCACHE_TEST_QUEUE=mqcache.pytest
    set MQCACHE_TEST_PASSWORD=...        (optional)
    pytest tests/test_integration_windows.py -v
"""
import os
import sys
import threading

import pytest

from mqcache import CacheClient, ServerError

ENDPOINT = os.environ.get("MQCACHE_TEST_ENDPOINT")
pytestmark = pytest.mark.skipif(sys.platform != "win32" or not ENDPOINT,
                                reason="needs Windows, MSMQ and MQCACHE_TEST_ENDPOINT pointing at a running server")


@pytest.fixture()
def client():
    with CacheClient(ENDPOINT, queue=os.environ.get("MQCACHE_TEST_QUEUE", "mqcache"),
                     password=os.environ.get("MQCACHE_TEST_PASSWORD"), timeout=10.0) as c:
        yield c


def test_ping_set_get_binary(client):
    assert client.ping() >= 0
    blob = bytes(range(256))
    assert client.set("py:bin", blob, px=30_000)
    assert client.get("py:bin") == blob
    client.delete("py:bin")


def test_pipeline_and_errors(client):
    res = client.pipeline([("SET", "py:a", 1), ("INCR", "py:a"), ("GET", "py:a")])
    assert res[1] == 2 and res[2] == b"2"
    client.set("py:s", "text")
    with pytest.raises(ServerError):
        client.incr("py:s")
    client.delete("py:a", "py:s")


def test_concurrent_increments(client):
    client.delete("py:n")
    ts = [threading.Thread(target=lambda: [client.incr("py:n") for _ in range(25)]) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert client.get("py:n") == b"200"
    client.delete("py:n")


def test_pubsub(client):
    with client.subscriber("py:news") as sub:
        import time
        time.sleep(1.0)
        assert client.publish("py:news", "hello") >= 1
        m = sub.get_message(timeout=10)
        assert m is not None and m.text == "hello"
