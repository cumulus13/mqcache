# File: sdk/python/src/mqcache/transport.py
# Author: Hadi Cahyadi <cumulus13@gmail.com>
# Date: 2026-10-12
# Description: Transport abstraction (what the client needs from MSMQ) and endpoint parsing.
# License: MIT
from __future__ import annotations

import abc
import os
import socket
from dataclasses import dataclass
from typing import Optional

REQUEST_LABEL = "mqcache.req"
RESPONSE_LABEL = "mqcache.resp"
PUBLISH_LABEL = "mqcache.pub"
PROTOCOL_ID = 1

# Queue name prefixes of this SDK. They deliberately differ from the .NET client's "mqcache.reply." / "mqcache.sub."
# because the .NET orphan sweeper deletes those whose name does not encode a live .NET process.
PY_REPLY_PREFIX = "mqcache.pyreply."
PY_SUB_PREFIX = "mqcache.pysub."


@dataclass(frozen=True)
class Destination:
    kind: str   # "path" (local, e.g. .\private$\mqcache) or "format" (e.g. DIRECT=TCP:10.0.0.5\private$\mqcache)
    value: str


@dataclass(frozen=True)
class Endpoint:
    host: str
    scheme: str
    queue: str

    @property
    def is_local(self) -> bool:
        h = self.host.lower()
        return h in ("", ".", "localhost") or h == socket.gethostname().lower()

    @property
    def destination(self) -> Destination:
        if self.is_local:
            return Destination("path", rf".\private$\{self.queue}")
        return Destination("format", rf"DIRECT={self.scheme}:{self.host}\private$\{self.queue}")

    @staticmethod
    def parse(spec: Optional[str], default_queue: str = "mqcache") -> "Endpoint":
        """``.`` | ``host`` | ``os:host`` | ``tcp:10.0.0.5`` with optional ``/queue`` (same grammar as the .NET client)."""
        spec = (spec or "").strip()
        queue = default_queue
        if "/" in spec:
            spec, _, q = spec.partition("/")
            queue = strip_private_prefix(q)
        scheme = "OS"
        if ":" in spec and spec.index(":") > 0:
            scheme, _, spec = spec.partition(":")
            scheme = scheme.upper()
            if scheme not in ("OS", "TCP"):
                raise ValueError(f"unsupported endpoint scheme '{scheme}' (use os: or tcp:)")
        if not queue:
            raise ValueError("empty queue name")
        return Endpoint(spec, scheme, queue)


def strip_private_prefix(name: str) -> str:
    name = name.strip().lstrip(".\\/")
    for p in ("private$\\", "private$/"):
        if name.lower().startswith(p):
            return name[len(p):]
    return name


@dataclass
class SendOptions:
    priority: int = 3            # 0 (lowest) .. 7 (highest); 3 = normal
    ttl_seconds: int = 30        # MSMQ TimeToBeReceived
    recoverable: bool = True     # persistent message
    journal: bool = False        # source journaling
    dead_letter: bool = True     # undeliverable/expired -> dead-letter queue


@dataclass(frozen=True)
class Received:
    label: str
    body: bytes
    correlation_id: bytes
    message_id: bytes


class InboundQueue(abc.ABC):
    """A private queue owned by this process (reply queue or subscriber queue). Deleted on close()."""

    format_name: str   # what the server uses to reach it (DIRECT=OS:host\private$\name)

    @abc.abstractmethod
    def receive(self, timeout: float) -> Optional[Received]:
        """Next message or None on timeout. May be called from several threads."""

    @abc.abstractmethod
    def close(self) -> None: ...


class Transport(abc.ABC):
    @abc.abstractmethod
    def open_inbound(self, prefix: str, scheme: str, host: Optional[str]) -> InboundQueue: ...

    @abc.abstractmethod
    def send(self, destination: Destination, body: bytes, options: SendOptions,
             response_format: Optional[str], transactional: bool) -> bytes:
        """Sends one message and returns its 20-byte MSMQ message id.

        Should raise :class:`~mqcache.errors.TransactionUsageError` when MSMQ rejects the send because the queue kind does
        not match ``transactional``. Note that MSMQ only rejects a *non-transactional* send to a transactional queue; a
        transactional send to a non-transactional queue is silently dropped, which the client handles by probing.
        """

    def is_transactional(self, destination: Destination) -> Optional[bool]:
        """Whether the destination queue is transactional, or None when it cannot be determined (remote queues)."""
        return None

    def close(self) -> None:  # pragma: no cover - optional
        pass


def default_reply_host(host: Optional[str]) -> str:
    return host or os.environ.get("COMPUTERNAME") or socket.gethostname()
