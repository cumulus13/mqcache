# File: sdk/python/src/mqcache/msmq_com.py
# Author: Hadi Cahyadi <cumulus13@gmail.com>
# Date: 2026-10-12
# Description: MSMQ transport using the MSMQ COM automation objects through pywin32 (Windows only).
# License: MIT
from __future__ import annotations

import os
import threading
import uuid
from typing import Any, Dict, Optional

from .errors import MsmqUnavailableError, TransactionUsageError, TransportError
from .transport import (PROTOCOL_ID, REQUEST_LABEL, Destination, InboundQueue, Received, SendOptions,
                        Transport, default_reply_host)

# MSMQ constants (mqoa.h)
MQ_RECEIVE_ACCESS = 1
MQ_SEND_ACCESS = 2
MQ_DENY_NONE = 0
MQ_NO_TRANSACTION = 0
MQ_SINGLE_MESSAGE = 3
MQMSG_DELIVERY_EXPRESS = 0
MQMSG_DELIVERY_RECOVERABLE = 1
MQMSG_DEADLETTER = 1
MQMSG_JOURNAL = 2
VT_ARRAY_UI1 = 0x2011

MQ_ERROR_TRANSACTION_USAGE = -1072824240    # 0xC00E0050
MQ_ERROR_SERVICE_NOT_AVAILABLE = -1072824309  # 0xC00E000B
DISP_E_EXCEPTION = -2147352567


def hresult_of(exc: BaseException) -> Optional[int]:
    """HRESULT of a pywintypes.com_error (unwraps EXCEPINFO scode when raised from inside a COM method)."""
    args = getattr(exc, "args", ())
    if not args or not isinstance(args[0], int):
        return None
    if args[0] == DISP_E_EXCEPTION and len(args) > 2 and args[2]:
        try:
            return int(args[2][5])
        except (IndexError, TypeError, ValueError):
            return args[0]
    return args[0]


def body_to_bytes(body: Any) -> bytes:
    """COM returns a SAFEARRAY(UI1); pywin32 hands it over as bytes or a sequence of ints."""
    if isinstance(body, (bytes, bytearray, memoryview)):
        return bytes(body)
    if isinstance(body, (tuple, list)):
        return bytes(body)
    raise TransportError(f"unexpected message body type {type(body).__name__}; the sender must use BodyType VT_ARRAY|VT_UI1")


class ComTransport(Transport):
    """
    Uses ``MSMQ.MSMQQueueInfo`` / ``MSMQ.MSMQMessage`` late-bound COM objects.
    COM objects are created per thread (each thread joins the MTA), so the transport is thread-safe.
    """

    def __init__(self) -> None:
        try:
            import pythoncom  # noqa: F401
            import pywintypes  # noqa: F401
            import win32com.client  # noqa: F401
        except ImportError as ex:  # pragma: no cover - platform dependent
            raise MsmqUnavailableError("pywin32 is required for the MSMQ COM transport: pip install pywin32 (Windows only)") from ex
        self._tls = threading.local()
        self._probe()

    # ------------------------------------------------------------- plumbing
    def _t(self):
        tls = self._tls
        if not getattr(tls, "ready", False):
            import pythoncom
            pythoncom.CoInitializeEx(pythoncom.COINIT_MULTITHREADED)
            tls.ready = True
            tls.send = {}
            tls.recv = {}
        return tls

    def _new(self, prog_id: str):
        import pywintypes
        import win32com.client
        self._t()
        try:
            return win32com.client.Dispatch(prog_id)
        except pywintypes.com_error as ex:
            raise MsmqUnavailableError(
                f"cannot create COM object {prog_id}: {ex}. Install the Windows feature MSMQ-Server and start the 'Message Queuing' service.") from ex

    def _probe(self) -> None:
        self._new("MSMQ.MSMQQueueInfo")

    def _info(self, destination: Destination):
        qi = self._new("MSMQ.MSMQQueueInfo")
        if destination.kind == "path":
            qi.PathName = destination.value
        else:
            qi.FormatName = destination.value
        return qi

    # ---------------------------------------------------------------- send
    def send(self, destination: Destination, body: bytes, options: SendOptions,
             response_format: Optional[str], transactional: bool) -> bytes:
        import pywintypes
        tls = self._t()
        key = (destination.kind, destination.value)
        try:
            q = tls.send.get(key)
            if q is None:
                q = self._info(destination).Open(MQ_SEND_ACCESS, MQ_DENY_NONE)
                tls.send[key] = q

            msg = self._new("MSMQ.MSMQMessage")
            msg.Label = REQUEST_LABEL
            msg.AppSpecific = PROTOCOL_ID
            msg.Priority = int(options.priority)
            msg.Delivery = MQMSG_DELIVERY_RECOVERABLE if options.recoverable else MQMSG_DELIVERY_EXPRESS
            if options.ttl_seconds > 0:
                msg.MaxTimeToReceive = int(options.ttl_seconds)
            msg.Journal = (MQMSG_DEADLETTER if options.dead_letter else 0) | (MQMSG_JOURNAL if options.journal else 0)
            if response_format:
                ri = self._new("MSMQ.MSMQQueueInfo")
                ri.FormatName = response_format
                msg.ResponseQueueInfo = ri
            msg.Body = body
            msg.Send(q, MQ_SINGLE_MESSAGE if transactional else MQ_NO_TRANSACTION)
            return body_to_bytes(msg.Id)
        except pywintypes.com_error as ex:
            hr = hresult_of(ex)
            tls.send.pop(key, None)
            if hr == MQ_ERROR_TRANSACTION_USAGE:
                raise TransactionUsageError("queue transactional flag does not match") from ex
            raise TransportError(f"MSMQ send failed (HRESULT {hr:#x} if known): {ex}" if hr is not None
                                 else f"MSMQ send failed: {ex}") from ex

    def is_transactional(self, destination: Destination) -> Optional[bool]:
        """Exact answer for local queues (path names); remote DIRECT format names cannot be queried."""
        if destination.kind != "path":
            return None
        try:
            qi = self._info(destination)
            qi.Refresh()
            return bool(qi.IsTransactional)
        except Exception:  # noqa: BLE001 - queue missing, access denied...: fall back to probing
            return None

    # ------------------------------------------------------------- inbound
    def open_inbound(self, prefix: str, scheme: str, host: Optional[str]) -> InboundQueue:
        import pywintypes
        name = f"{prefix}{os.getpid()}.{uuid.uuid4().hex[:10]}"
        path = rf".\private$\{name}"
        qi = self._new("MSMQ.MSMQQueueInfo")
        qi.PathName = path
        qi.Label = "mqcache python inbound"
        try:
            qi.Create(False, False)      # non-transactional, not world readable
        except pywintypes.com_error as ex:
            raise TransportError(f"cannot create queue {path}: {ex}") from ex
        fmt = rf"DIRECT={scheme.upper()}:{default_reply_host(host)}\private$\{name}"
        return _ComInbound(self, path, fmt)


class _ComInbound(InboundQueue):
    def __init__(self, transport: ComTransport, path: str, format_name: str) -> None:
        self._transport = transport
        self._path = path
        self.format_name = format_name
        self._closed = False

    def receive(self, timeout: float) -> Optional[Received]:
        import pywintypes
        if self._closed:
            return None
        tls = self._transport._t()
        try:
            h = tls.recv.get(self._path)
            if h is None:
                qi = self._transport._new("MSMQ.MSMQQueueInfo")
                qi.PathName = self._path
                h = qi.Open(MQ_RECEIVE_ACCESS, MQ_DENY_NONE)
                tls.recv[self._path] = h
            # Receive(Transaction, WantDestinationQueue, WantBody, ReceiveTimeout[ms]); returns None on timeout
            m = h.Receive(MQ_NO_TRANSACTION, False, True, max(0, int(timeout * 1000)))
            if m is None:
                return None
            return Received(str(m.Label), body_to_bytes(m.Body), body_to_bytes(m.CorrelationId), body_to_bytes(m.Id))
        except pywintypes.com_error as ex:
            tls.recv.pop(self._path, None)
            if self._closed:
                return None
            raise TransportError(f"MSMQ receive failed: {ex}") from ex

    def close(self) -> None:
        import pywintypes
        if self._closed:
            return
        self._closed = True
        try:
            qi = self._transport._new("MSMQ.MSMQQueueInfo")
            qi.PathName = self._path
            qi.Delete()
        except (pywintypes.com_error, TransportError):
            pass   # already gone
