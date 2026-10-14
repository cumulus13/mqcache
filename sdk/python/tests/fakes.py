# File: sdk/python/tests/fakes.py
# Author: Hadi Cahyadi <cumulus13@gmail.com>
# Date: 2026-10-12
# Description: In-memory MSMQ stand-in plus a tiny reference server speaking wire protocol v1 (test support only).
# License: MIT
import os
import queue
import random
import threading
import uuid
from typing import Callable, Dict, List, Optional

from mqcache.errors import TransactionUsageError
from mqcache.protocol import (PubMessage, Resp, RespType, decode_request, encode_pub, encode_response)
from mqcache.transport import (PUBLISH_LABEL, RESPONSE_LABEL, Destination, InboundQueue, Received, SendOptions,
                               Transport)


def _bulk(b): return Resp(RespType.BULK, b)


NIL = Resp(RespType.NULL)


class RefServer:
    """Subset of the real server: enough to exercise client behaviour (retries, dedupe, auth, pub/sub, errors)."""

    def __init__(self, password=None, transactional=True):
        self.password = password
        self.transactional = transactional
        self.data: Dict[bytes, object] = {}
        self.dedupe: Dict[uuid.UUID, bytes] = {}
        self.subs: Dict[str, set] = {}
        self.lock = threading.Lock()
        self.executed = 0
        self.broker: "Broker" = None  # type: ignore[assignment]

    def handle(self, body: bytes, message_id: bytes, response_format: Optional[str]) -> None:
        req = decode_request(body)
        with self.lock:
            cached = self.dedupe.get(req.request_id)
            if cached is None:
                if self.password is not None and req.auth != self.password:
                    err = "NOAUTH Authentication required." if req.auth is None else "WRONGPASS invalid password"
                    results = [Resp(RespType.ERROR, err)] * len(req.commands)
                else:
                    results = [self._exec(c, response_format) for c in req.commands]
                    self.executed += 1
                cached = encode_response(req.request_id, results)
                self.dedupe[req.request_id] = cached
        if not req.no_reply and response_format:
            self.broker.deliver(response_format, Received(RESPONSE_LABEL, cached, message_id, os.urandom(20)))

    def _exec(self, c, response_format) -> Resp:
        a, n = c.args, c.name
        if n == "PING":
            return Resp(RespType.BULK, a[0]) if a else Resp(RespType.STATUS, "PONG")
        if n == "SET":
            self.data[a[0]] = a[1]
            return Resp(RespType.STATUS, "OK")
        if n == "GET":
            v = self.data.get(a[0])
            if isinstance(v, dict):
                return Resp(RespType.ERROR, "WRONGTYPE Operation against a key holding the wrong kind of value")
            return NIL if v is None else _bulk(v)
        if n in ("INCR", "INCRBY"):
            cur = int(self.data.get(a[0], b"0"))
            cur += int(a[1]) if n == "INCRBY" else 1
            self.data[a[0]] = str(cur).encode()
            return Resp(RespType.INTEGER, cur)
        if n == "DEL":
            return Resp(RespType.INTEGER, sum(1 for k in a if self.data.pop(k, None) is not None))
        if n == "HSET":
            h = self.data.setdefault(a[0], {})
            added = 0
            for i in range(1, len(a), 2):
                added += a[i] not in h
                h[a[i]] = a[i + 1]
            return Resp(RespType.INTEGER, added)
        if n == "HGETALL":
            h = self.data.get(a[0], {})
            return Resp(RespType.ARRAY, tuple(_bulk(x) for kv in h.items() for x in kv))
        if n == "SUBSCRIBE":
            for ch in a:
                self.subs.setdefault(ch.decode(), set()).add(response_format)
            return Resp(RespType.ARRAY, tuple(Resp(RespType.ARRAY, (_bulk(b"subscribe"), _bulk(ch), Resp(RespType.INTEGER, 1))) for ch in a))
        if n == "UNSUBSCRIBE":
            for s in self.subs.values():
                s.discard(response_format)
            return Resp(RespType.INTEGER, 0)
        if n == "PUBLISH":
            ch = a[0].decode()
            targets = list(self.subs.get(ch, ()))
            for t in targets:
                self.broker.deliver(t, Received(PUBLISH_LABEL, encode_pub(PubMessage(ch, None, a[1])), b"\0" * 20, os.urandom(20)))
            return Resp(RespType.INTEGER, len(targets))
        return Resp(RespType.ERROR, f"ERR unknown command '{n}'")


class Broker:
    def __init__(self, server: RefServer, jitter: float = 0.0):
        self.server = server
        server.broker = self
        self.queues: Dict[str, "queue.Queue[Received]"] = {}
        self.drop_replies = 0          # swallow this many replies (simulates lost replies)
        self.jitter = jitter
        self.created: List[str] = []   # queue name prefixes, to assert naming
        self.closed: List[str] = []
        self._lock = threading.Lock()

    def deliver(self, fmt: str, msg: Received) -> None:
        with self._lock:
            if msg.label == RESPONSE_LABEL and self.drop_replies > 0:
                self.drop_replies -= 1
                return
            q = self.queues.get(fmt)
        if q is None:
            return
        if self.jitter:
            threading.Timer(random.random() * self.jitter, q.put, args=(msg,)).start()
        else:
            q.put(msg)


class _Inbound(InboundQueue):
    def __init__(self, broker: Broker, fmt: str):
        self.format_name = fmt
        self._b = broker
        self._q: "queue.Queue[Received]" = queue.Queue()
        broker.queues[fmt] = self._q

    def receive(self, timeout: float):
        try:
            return self._q.get(timeout=timeout)
        except queue.Empty:
            return None

    def close(self):
        self._b.queues.pop(self.format_name, None)
        self._b.closed.append(self.format_name)


class LoopTransport(Transport):
    def __init__(self, broker: Broker, report_queue_type: bool = False):
        self.broker = broker
        self.sent: List[bytes] = []
        self.no_response_sends = 0
        self.report_queue_type = report_queue_type     # emulate a local queue whose flag can be read

    def is_transactional(self, destination):
        return self.broker.server.transactional if self.report_queue_type else None

    def open_inbound(self, prefix, scheme, host):
        fmt = f"DIRECT={scheme.upper()}:{host or 'testhost'}\\private$\\{prefix}{uuid.uuid4().hex[:8]}"
        self.broker.created.append(prefix)
        return _Inbound(self.broker, fmt)

    def send(self, destination: Destination, body: bytes, options: SendOptions, response_format, transactional):
        server_tx = self.broker.server.transactional
        if not transactional and server_tx:
            raise TransactionUsageError("non-transactional message sent to a transactional queue")   # MSMQ rejects this one
        self.sent.append(body)
        if transactional and not server_tx:
            return os.urandom(20)                      # MSMQ accepts it and then silently drops it
        if response_format is None:
            self.no_response_sends += 1
        mid = os.urandom(20)
        self.broker.server.handle(body, mid, response_format)
        return mid
