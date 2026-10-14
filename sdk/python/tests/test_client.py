# File: sdk/python/tests/test_client.py
# Author: Hadi Cahyadi <cumulus13@gmail.com>
# Date: 2026-10-12
# Description: Client behaviour tests against an in-memory MSMQ stand-in and a reference server.
# License: MIT
import threading

import pytest
from fakes import Broker, LoopTransport, RefServer

from mqcache import CacheClient, CacheTimeoutError, Priority, ServerError
from mqcache.msmq_com import body_to_bytes, hresult_of
from mqcache.errors import TransportError
from mqcache.transport import Endpoint


def make(server=None, *, jitter=0.0, report_queue_type=False, **kw):
    server = server or RefServer()
    broker = Broker(server, jitter)
    tr = LoopTransport(broker, report_queue_type)
    kw.setdefault("timeout", 1.0)
    kw.setdefault("retries", 1)
    return CacheClient(".", transport=tr, **kw), server, broker, tr


def test_basic_roundtrip_and_binary_values():
    c, *_ = make()
    assert c.ping() >= 0
    assert c.set("k", b"\x00\xffbin") is True
    assert c.get("k") == b"\x00\xffbin"
    assert c.get("missing") is None
    assert c.incr("n") == 1 and c.incr("n", 9) == 10
    assert c.delete("k", "zzz") == 1
    c.close()


def test_decode_responses():
    c, *_ = make(decode_responses=True)
    c.set("k", "héllo")
    assert c.get("k") == "héllo"
    c.hset("h", mapping={"a": "1", "b": "2"})
    assert c.hgetall("h") == {"a": "1", "b": "2"}


def test_server_errors_raise_with_code():
    c, *_ = make()
    c.hset("h", "f", "v")
    with pytest.raises(ServerError) as ei:
        c.get("h")
    assert ei.value.code == "WRONGTYPE"
    with pytest.raises(ServerError):
        c.execute_command("NOPE")


def test_pipeline_one_message_errors_as_values():
    c, _, _, tr = make()
    before = len(tr.sent)
    res = c.pipeline([("SET", "a", 1), ("INCR", "a"), ("GET", "a"), ("BOGUS",)])
    assert len(tr.sent) == before + 1                       # a single MSMQ message
    assert res[0] == "OK" and res[1] == 2 and res[2] == b"2"
    assert isinstance(res[3], ServerError)
    with pytest.raises(ServerError):
        c.pipeline([("BOGUS",)], raise_on_error=True)
    assert c.pipeline([]) == []


def test_password_enforced():
    srv = RefServer(password="s3cret")
    ok, *_ = make(srv, password="s3cret")
    assert ok.set("k", "v")
    bad, *_ = make(srv, password="nope")
    with pytest.raises(ServerError) as ei:
        bad.ping()
    assert ei.value.code == "WRONGPASS"
    none, *_ = make(srv)
    with pytest.raises(ServerError) as ei:
        none.ping()
    assert ei.value.code == "NOAUTH"


def test_lost_reply_is_retried_without_double_execution():
    c, srv, broker, tr = make(retries=2, timeout=0.3)
    broker.drop_replies = 1                                  # first reply is lost
    assert c.incr("counter") == 1                            # retry returns the cached answer
    assert c.get("counter") == b"1"                          # INCR was applied exactly once
    assert len(tr.sent) >= 3 and tr.sent[0] == tr.sent[1]    # same request id on both attempts


def test_timeout_after_all_retries():
    c, _, broker, tr = make(retries=1, timeout=0.2)
    broker.drop_replies = 99
    with pytest.raises(CacheTimeoutError):
        c.ping()
    assert len(tr.sent) == 2


def test_probing_finds_nontransactional_queue_when_transactional_send_is_silently_dropped():
    c, srv, _, tr = make(RefServer(transactional=False), transactional=True, detect_queue_type=False, timeout=0.3, retries=2)
    assert c.set("k", "v")                                   # attempt 1 (tx) is dropped, attempt 2 (non-tx) works
    assert c._transactional is False and c._mode_confirmed
    before = len(tr.sent)
    assert c.get("k") == b"v"
    assert len(tr.sent) == before + 1                         # mode remembered: no more probing


def test_probing_finds_transactional_queue_when_msmq_rejects_nontransactional_send():
    c, srv, *_ = make(RefServer(transactional=True), transactional=False, detect_queue_type=False)
    assert c.set("k", "v")
    assert c._transactional is True and c._mode_confirmed    # TransactionUsageError triggered an immediate flip
    assert c.get("k") == b"v"


def test_local_queue_type_is_read_not_probed():
    c, _, _, tr = make(RefServer(transactional=False), report_queue_type=True, transactional=True, timeout=0.3)
    assert c._mode_confirmed and c._transactional is False
    assert c.set("k", "v") and len(tr.sent) == 1             # first attempt already uses the right mode


def test_concurrent_callers_get_their_own_replies_out_of_order():
    c, srv, *_ = make(jitter=0.004, timeout=5.0)
    errors = []

    def worker(i):
        try:
            for j in range(25):
                key = f"w{i}"
                c.set(key, f"{i}:{j}")
                assert c.get(key) == f"{i}:{j}".encode()
                c.incr("shared")
        except Exception as ex:  # noqa: BLE001
            errors.append(ex)

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert not errors
    assert c.get("shared") == b"200"


def test_send_no_reply_requests_no_response_queue():
    c, srv, _, tr = make()
    c.send_no_reply("SET", "fire", "forget")
    assert tr.no_response_sends == 1
    assert srv.data[b"fire"] == b"forget"


def test_pubsub_channel_delivery_and_close():
    c, srv, broker, _ = make()
    sub = c.subscriber("news")
    try:
        assert c.publish("news", "hello") == 1
        m = sub.get_message(timeout=2)
        assert m is not None and m.channel == "news" and m.text == "hello"
        assert c.publish("other", "x") == 0
        assert sub.get_message(timeout=0.2) is None
    finally:
        sub.close()
    assert c.publish("news", "after") == 0                   # UNSUBSCRIBE was sent on close


def test_queue_naming_does_not_collide_with_dotnet_sweeper():
    c, _, broker, _ = make()
    sub = c.subscriber("x")
    sub.close()
    assert set(broker.created) == {"mqcache.pyreply.", "mqcache.pysub."}
    for p in broker.created:                                  # .NET sweeps names starting with these prefixes
        assert not p.startswith(("mqcache.reply.", "mqcache.sub."))


def test_close_removes_reply_queue_and_blocks_use():
    c, _, broker, _ = make()
    c.close()
    assert len(broker.closed) == 1
    with pytest.raises(Exception):
        c.ping()


def test_priority_enum_matches_msmq_scale():
    assert (Priority.LOWEST, Priority.NORMAL, Priority.HIGHEST) == (0, 3, 7)


@pytest.mark.parametrize("spec,host,scheme,queue,local", [
    (".", ".", "OS", "mqcache", True),
    ("", "", "OS", "mqcache", True),
    ("tcp:10.0.0.5", "10.0.0.5", "TCP", "mqcache", False),
    ("os:srv01/orders", "srv01", "OS", "orders", False),
    ("srv01/private$\\orders", "srv01", "OS", "orders", False),
])
def test_endpoint_parse(spec, host, scheme, queue, local):
    e = Endpoint.parse(spec)
    assert (e.host, e.scheme, e.queue, e.is_local) == (host, scheme, queue, local)


def test_endpoint_destination_and_errors():
    assert Endpoint.parse(".").destination.value == r".\private$\mqcache"
    assert Endpoint.parse("tcp:10.0.0.5/q").destination.value == r"DIRECT=TCP:10.0.0.5\private$\q"
    with pytest.raises(ValueError):
        Endpoint.parse("ftp:host")


def test_com_helpers():
    assert hresult_of(Exception(-1072824240)) == -1072824240
    assert hresult_of(Exception(-2147352567, "x", (0, "MSMQ", "desc", None, 0, -1072824240), None)) == -1072824240
    assert hresult_of(Exception("no code")) is None
    assert body_to_bytes((0, 255, 1)) == b"\x00\xff\x01"
    assert body_to_bytes(memoryview(b"ab")) == b"ab"
    with pytest.raises(TransportError):
        body_to_bytes("text")
