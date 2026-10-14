# File: sdk/python/src/mqcache/client.py
# Author: Hadi Cahyadi <cumulus13@gmail.com>
# Date: 2026-10-12
# Description: Thread-safe MQCache client: request/reply over MSMQ with retries, pipelines, helpers and pub/sub.
# License: MIT
from __future__ import annotations

import enum
import threading
import time
import uuid
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple, Union

from .errors import (CacheTimeoutError, MQCacheError, ServerError, TransactionUsageError, TransportError,
                     WireFormatError)
from .protocol import (Arg, Command, PubMessage, Resp, RespType, decode_pub, decode_response, encode_request, to_bytes)
from .transport import (PUBLISH_LABEL, PY_REPLY_PREFIX, PY_SUB_PREFIX, Endpoint, InboundQueue, Received, SendOptions,
                        Transport)


class Priority(enum.IntEnum):
    """MSMQ message priority (same numeric scale as System.Messaging.MessagePriority)."""

    LOWEST = 0
    VERY_LOW = 1
    LOW = 2
    NORMAL = 3
    ABOVE_NORMAL = 4
    HIGH = 5
    VERY_HIGH = 6
    HIGHEST = 7


class _ReplyDemux:
    """
    Many threads share one reply queue and each wants only its own reply. MSMQ's COM API has no selective receive,
    so one waiter at a time reads the queue and stashes replies for the others (keyed by correlation id).
    """

    _STASH_TTL = 120.0
    _STASH_MAX = 4096

    def __init__(self, inbound: InboundQueue) -> None:
        self._inbound = inbound
        self._cv = threading.Condition()
        self._stash: Dict[bytes, Tuple[float, bytes]] = {}
        self._reading = False

    def wait(self, correlation_id: bytes, timeout: float) -> Optional[bytes]:
        deadline = time.monotonic() + timeout
        with self._cv:
            while True:
                hit = self._stash.pop(correlation_id, None)
                if hit is not None:
                    return hit[1]
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                if self._reading:
                    self._cv.wait(remaining)
                    continue
                self._reading = True
                break
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                msg = self._inbound.receive(min(remaining, 0.5))
                if msg is None:
                    continue
                if msg.correlation_id == correlation_id:
                    return msg.body
                with self._cv:
                    self._put(msg)
                    self._cv.notify_all()
        finally:
            with self._cv:
                self._reading = False
                self._cv.notify_all()

    def _put(self, msg: Received) -> None:
        now = time.monotonic()
        if len(self._stash) >= self._STASH_MAX or (self._stash and len(self._stash) % 64 == 0):
            for k in [k for k, (t, _) in self._stash.items() if now - t > self._STASH_TTL]:
                del self._stash[k]
            while len(self._stash) >= self._STASH_MAX:
                self._stash.pop(next(iter(self._stash)))
        self._stash[msg.correlation_id] = (now, msg.body)


class CacheClient:
    """
    >>> c = CacheClient("tcp:10.0.0.5", password="secret")      # doctest: +SKIP
    >>> c.set("k", "v", ex=60); c.get("k")                       # doctest: +SKIP
    b'v'

    Thread-safe: share one instance per process. ``close()`` (or ``with``) removes the private reply queue.
    """

    def __init__(self, endpoint: str = ".", *, queue: str = "mqcache", password: Optional[str] = None,
                 timeout: float = 5.0, retries: int = 2, priority: int = Priority.NORMAL, recoverable: bool = True,
                 transactional: bool = True, detect_queue_type: bool = True, use_journal: bool = False, use_dead_letter: bool = True,
                 reply_host: Optional[str] = None, reply_scheme: str = "os", decode_responses: bool = False,
                 subscription_renew: float = 30.0, transport: Optional[Transport] = None) -> None:
        if transport is None:
            from .msmq_com import ComTransport
            transport = ComTransport()
        self._transport = transport
        self._endpoint = Endpoint.parse(endpoint, queue)
        self._dest = self._endpoint.destination
        self.password = password
        self.timeout = timeout
        self.retries = retries
        self.priority = int(priority)
        self.recoverable = recoverable
        self.use_journal = use_journal
        self.use_dead_letter = use_dead_letter
        self.decode_responses = decode_responses
        self.subscription_renew = subscription_renew
        self._reply_scheme = reply_scheme
        self._reply_host = reply_host
        self._transactional = transactional
        self._mode_confirmed = False     # True once known: detected, a reply arrived, or MSMQ rejected the other kind
        if detect_queue_type:
            detected = transport.is_transactional(self._dest)
            if detected is not None:
                self._transactional = detected
                self._mode_confirmed = True
        self._closed = False
        self._inbound = transport.open_inbound(PY_REPLY_PREFIX, reply_scheme, reply_host)
        self._demux = _ReplyDemux(self._inbound)

    # ----------------------------------------------------------------- core
    def _options(self, timeout: float, retries: int, priority: Optional[int], recoverable: Optional[bool]) -> SendOptions:
        return SendOptions(priority=int(self.priority if priority is None else priority),
                           ttl_seconds=max(1, int(timeout * (retries + 1) + 0.999)),
                           recoverable=self.recoverable if recoverable is None else recoverable,
                           journal=self.use_journal, dead_letter=self.use_dead_letter)

    def _send(self, body: bytes, opts: SendOptions, response_format: Optional[str],
              mode: Optional[bool] = None) -> Tuple[bytes, bool]:
        """Returns (message id, send mode actually used)."""
        mode = self._transactional if mode is None else mode
        try:
            return self._transport.send(self._dest, body, opts, response_format, mode), mode
        except TransactionUsageError:
            mode = not mode                       # MSMQ rejected this kind: the other one is right; flip and resend once
            self._transactional = mode
            self._mode_confirmed = True
            return self._transport.send(self._dest, body, opts, response_format, mode), mode

    def _roundtrip(self, commands: Sequence[Command], atomic: bool, timeout: Optional[float], retries: Optional[int],
                   priority: Optional[int], recoverable: Optional[bool]) -> Tuple[Resp, ...]:
        if self._closed:
            raise MQCacheError("client is closed")
        timeout = self.timeout if timeout is None else timeout
        retries = self.retries if retries is None else retries
        rid = uuid.uuid4()    # same id on every retry => the server answers from its dedupe cache instead of re-executing
        body = encode_request(rid, commands, auth=self.password, atomic=atomic)
        opts = self._options(timeout, retries, priority, recoverable)
        initial = self._transactional
        for attempt in range(retries + 1):
            # MSMQ silently drops a transactional send to a non-transactional queue, so until the queue kind is
            # confirmed the attempts alternate between the two modes (same request id: the server de-duplicates).
            mode = self._transactional if self._mode_confirmed else (initial if attempt % 2 == 0 else not initial)
            msg_id, mode = self._send(body, opts, self._inbound.format_name, mode)
            reply = self._demux.wait(msg_id, timeout)
            if reply is None:
                continue
            if not self._mode_confirmed:
                self._transactional, self._mode_confirmed = mode, True
            resp = decode_response(reply)
            if len(resp.results) == len(commands):
                return resp.results
            if len(resp.results) == 1 and resp.results[0].is_error:
                raise ServerError(str(resp.results[0].value))
            raise WireFormatError(f"expected {len(commands)} results, got {len(resp.results)}")
        raise CacheTimeoutError(f"no reply from '{self._dest.value}' after {retries + 1} attempt(s) of {timeout:g}s")

    def _convert(self, r: Resp) -> Any:
        if r.is_error:
            raise ServerError(str(r.value))
        return r.to_python(self.decode_responses)

    def execute_command(self, *args: Arg, timeout: Optional[float] = None, retries: Optional[int] = None,
                        priority: Optional[int] = None) -> Any:
        """Runs one command; raises :class:`ServerError` on an error reply."""
        return self._convert(self._roundtrip([Command.of(*args)], False, timeout, retries, priority, None)[0])

    def pipeline(self, commands: Iterable[Sequence[Arg]], *, atomic: bool = True, raise_on_error: bool = False,
                 timeout: Optional[float] = None, retries: Optional[int] = None, priority: Optional[int] = None) -> List[Any]:
        """Sends several commands in ONE MSMQ message. Error replies are returned as ServerError instances
        (or the first one is raised when ``raise_on_error``)."""
        cmds = [Command.of(*c) for c in commands]
        if not cmds:
            return []
        out = []
        for r in self._roundtrip(cmds, atomic, timeout, retries, priority, None):
            v = r.to_python(self.decode_responses)
            if raise_on_error and isinstance(v, ServerError):
                raise v
            out.append(v)
        return out

    def send_no_reply(self, *args: Arg, priority: Optional[int] = None) -> None:
        """Fire-and-forget: no reply is requested and none is awaited."""
        body = encode_request(uuid.uuid4(), [Command.of(*args)], auth=self.password, no_reply=True)
        self._send(body, self._options(self.timeout, self.retries, priority, None), None)

    def send_no_reply_to(self, response_format: str, *args: Arg) -> None:
        """Internal: sends a no-reply command whose ResponseQueue is ``response_format`` (used for subscriptions)."""
        body = encode_request(uuid.uuid4(), [Command.of(*args)], auth=self.password, no_reply=True)
        opts = self._options(30.0, 0, None, None)
        self._send(body, opts, response_format)

    # ------------------------------------------------------------- commands
    def ping(self) -> float:
        t = time.perf_counter()
        self.execute_command("PING")
        return time.perf_counter() - t

    def get(self, key: str):
        return self.execute_command("GET", key)

    def set(self, key: str, value: Arg, *, ex: Optional[float] = None, px: Optional[int] = None, nx: bool = False,
            xx: bool = False, keepttl: bool = False) -> bool:
        a: List[Arg] = ["SET", key, value]
        if ex is not None:
            a += ["EX", int(ex)]
        if px is not None:
            a += ["PX", int(px)]
        if nx:
            a.append("NX")
        if xx:
            a.append("XX")
        if keepttl:
            a.append("KEEPTTL")
        return self.execute_command(*a) is not None

    def delete(self, *keys: str) -> int: return self.execute_command("DEL", *keys)
    def exists(self, *keys: str) -> int: return self.execute_command("EXISTS", *keys)
    def incr(self, key: str, amount: int = 1) -> int: return self.execute_command("INCRBY", key, amount)
    def decr(self, key: str, amount: int = 1) -> int: return self.execute_command("DECRBY", key, amount)
    def append(self, key: str, value: Arg) -> int: return self.execute_command("APPEND", key, value)
    def expire(self, key: str, seconds: int) -> bool: return bool(self.execute_command("EXPIRE", key, seconds))
    def pexpire(self, key: str, ms: int) -> bool: return bool(self.execute_command("PEXPIRE", key, ms))
    def ttl(self, key: str) -> int: return self.execute_command("TTL", key)
    def pttl(self, key: str) -> int: return self.execute_command("PTTL", key)
    def persist(self, key: str) -> bool: return bool(self.execute_command("PERSIST", key))
    def type(self, key: str) -> str: return self.execute_command("TYPE", key)
    def keys(self, pattern: str = "*") -> List[Any]: return self.execute_command("KEYS", pattern)
    def mget(self, *keys: str) -> List[Any]: return self.execute_command("MGET", *keys)
    def dbsize(self) -> int: return self.execute_command("DBSIZE")
    def flushall(self) -> bool: return self.execute_command("FLUSHALL") == "OK"
    def save(self) -> bool: return self.execute_command("SAVE") == "OK"
    def rename(self, src: str, dst: str) -> bool: return self.execute_command("RENAME", src, dst) == "OK"

    def mset(self, mapping: Mapping[str, Arg]) -> bool:
        flat: List[Arg] = []
        for k, v in mapping.items():
            flat += [k, v]
        return self.execute_command("MSET", *flat) == "OK"

    def hset(self, key: str, field: Optional[str] = None, value: Optional[Arg] = None,
             mapping: Optional[Mapping[str, Arg]] = None) -> int:
        flat: List[Arg] = []
        if field is not None:
            flat += [field, value if value is not None else ""]
        for k, v in (mapping or {}).items():
            flat += [k, v]
        if not flat:
            raise ValueError("hset needs field/value or mapping")
        return self.execute_command("HSET", key, *flat)

    def hget(self, key: str, field: str): return self.execute_command("HGET", key, field)
    def hdel(self, key: str, *fields: str) -> int: return self.execute_command("HDEL", key, *fields)
    def hlen(self, key: str) -> int: return self.execute_command("HLEN", key)
    def hexists(self, key: str, field: str) -> bool: return bool(self.execute_command("HEXISTS", key, field))
    def hincrby(self, key: str, field: str, amount: int = 1) -> int: return self.execute_command("HINCRBY", key, field, amount)

    def hgetall(self, key: str) -> Dict[Any, Any]:
        flat = self.execute_command("HGETALL", key)
        return {flat[i]: flat[i + 1] for i in range(0, len(flat), 2)}

    def lpush(self, key: str, *values: Arg) -> int: return self.execute_command("LPUSH", key, *values)
    def rpush(self, key: str, *values: Arg) -> int: return self.execute_command("RPUSH", key, *values)
    def lpop(self, key: str): return self.execute_command("LPOP", key)
    def rpop(self, key: str): return self.execute_command("RPOP", key)
    def llen(self, key: str) -> int: return self.execute_command("LLEN", key)
    def lrange(self, key: str, start: int = 0, stop: int = -1) -> List[Any]: return self.execute_command("LRANGE", key, start, stop)
    def lindex(self, key: str, index: int): return self.execute_command("LINDEX", key, index)

    def sadd(self, key: str, *members: str) -> int: return self.execute_command("SADD", key, *members)
    def srem(self, key: str, *members: str) -> int: return self.execute_command("SREM", key, *members)
    def smembers(self, key: str) -> List[Any]: return self.execute_command("SMEMBERS", key)
    def sismember(self, key: str, member: str) -> bool: return bool(self.execute_command("SISMEMBER", key, member))
    def scard(self, key: str) -> int: return self.execute_command("SCARD", key)

    def publish(self, channel: str, message: Arg) -> int:
        """Returns the number of subscriber queues the server delivered to."""
        return self.execute_command("PUBLISH", channel, message)

    def info(self) -> str:
        v = self.execute_command("INFO")
        return v.decode("utf-8", "replace") if isinstance(v, bytes) else v

    def config_get(self, pattern: str = "*") -> Dict[Any, Any]:
        flat = self.execute_command("CONFIG", "GET", pattern)
        return {flat[i]: flat[i + 1] for i in range(0, len(flat), 2)}

    # --------------------------------------------------------------- pub/sub
    def subscriber(self, *channels: str, patterns: Sequence[str] = ()) -> "Subscriber":
        return Subscriber(self, channels, patterns)

    # ------------------------------------------------------------- lifecycle
    def _open_inbound(self, prefix: str) -> InboundQueue:
        return self._transport.open_inbound(prefix, self._reply_scheme, self._reply_host)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._inbound.close()
        self._transport.close()

    def __enter__(self) -> "CacheClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class Subscriber:
    """
    Owns a private queue; the server delivers published messages to it (at-most-once). A background thread renews the
    subscription lease every ``subscription_renew`` seconds. Use as a context manager or call :meth:`close`.
    """

    def __init__(self, client: CacheClient, channels: Sequence[str], patterns: Sequence[str]) -> None:
        self._client = client
        self._channels = set(channels)
        self._patterns = set(patterns)
        self._inbound = client._open_inbound(PY_SUB_PREFIX)
        self._inbox: "list[PubMessage]" = []
        self._cv = threading.Condition()
        self._stop = threading.Event()
        self._closed = False
        self._rx = threading.Thread(target=self._receive_loop, name="mqcache-subscriber", daemon=True)
        self._rx.start()
        self._renew_thread = threading.Thread(target=self._renew_loop, name="mqcache-renew", daemon=True)
        self._renew_thread.start()
        self._send_all()

    # --------------------------------------------------------------- control
    def subscribe(self, *channels: str) -> None:
        self._channels.update(channels)
        self._client.send_no_reply_to(self._inbound.format_name, "SUBSCRIBE", *channels)

    def psubscribe(self, *patterns: str) -> None:
        self._patterns.update(patterns)
        self._client.send_no_reply_to(self._inbound.format_name, "PSUBSCRIBE", *patterns)

    def unsubscribe(self, *channels: str) -> None:
        self._channels.difference_update(channels)
        self._client.send_no_reply_to(self._inbound.format_name, "UNSUBSCRIBE", *channels)

    # ---------------------------------------------------------------- reading
    def get_message(self, timeout: Optional[float] = None) -> Optional[PubMessage]:
        """Next message, or None after ``timeout`` seconds (``None`` waits until closed)."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cv:
            while not self._inbox:
                if self._closed:
                    return None
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return None
                self._cv.wait(remaining)
            return self._inbox.pop(0)

    def listen(self) -> Iterator[PubMessage]:
        """Blocking iterator that ends when the subscriber is closed."""
        while True:
            m = self.get_message(timeout=1.0)
            if m is not None:
                yield m
            elif self._closed:
                return

    # ---------------------------------------------------------------- threads
    def _send_all(self) -> None:
        try:
            if self._channels:
                self._client.send_no_reply_to(self._inbound.format_name, "SUBSCRIBE", *sorted(self._channels))
            if self._patterns:
                self._client.send_no_reply_to(self._inbound.format_name, "PSUBSCRIBE", *sorted(self._patterns))
        except TransportError:
            pass    # retried at the next renewal

    def _renew_loop(self) -> None:
        while not self._stop.wait(self._client.subscription_renew):
            self._send_all()

    def _receive_loop(self) -> None:
        while not self._stop.is_set():
            try:
                m = self._inbound.receive(1.0)
            except TransportError:
                if self._stop.wait(1.0):
                    return
                continue
            if m is None or m.label != PUBLISH_LABEL:
                continue
            try:
                pm = decode_pub(m.body)
            except WireFormatError:
                continue
            with self._cv:
                if len(self._inbox) >= 100_000:
                    self._inbox.pop(0)      # drop oldest when the consumer is too slow
                self._inbox.append(pm)
                self._cv.notify_all()

    def close(self) -> None:
        if self._closed:
            return
        for name in ("UNSUBSCRIBE", "PUNSUBSCRIBE"):
            try:
                self._client.send_no_reply_to(self._inbound.format_name, name)
            except MQCacheError:
                pass
        self._stop.set()
        with self._cv:
            self._closed = True
            self._cv.notify_all()
        self._rx.join(3.0)
        self._inbound.close()

    def __enter__(self) -> "Subscriber":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
