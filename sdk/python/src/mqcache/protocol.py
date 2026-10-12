# File: sdk/python/src/mqcache/protocol.py
# Author: Hadi Cahyadi <cumulus13@gmail.com>
# Date: 2026-10-12
# Description: Wire protocol v1 (request/response envelopes, publications). Pure Python, byte-compatible with the .NET WireCodec.
# License: MIT
from __future__ import annotations

import enum
import struct
import uuid
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple, Union

from .errors import WireFormatError

PROTOCOL_VERSION = 1
MAX_COMMANDS = 100_000
MAX_ARGS_PER_COMMAND = 65_536
MAX_DEPTH = 16

_FLAG_ATOMIC = 1
_FLAG_NOREPLY = 2

Arg = Union[str, bytes, bytearray, memoryview, int, float]


class RespType(enum.IntEnum):
    STATUS = 1
    ERROR = 2
    INTEGER = 3
    BULK = 4
    NULL = 5
    ARRAY = 6


@dataclass(frozen=True)
class Resp:
    """One reply value. ``value`` is str (status/error), int, bytes, None or a tuple of Resp."""

    type: RespType
    value: object = None

    @property
    def is_error(self) -> bool:
        return self.type == RespType.ERROR

    def to_python(self, decode: bool = False):
        """Converts to native types. Error replies become :class:`~mqcache.errors.ServerError` *instances* (not raised)."""
        from .errors import ServerError

        if self.type in (RespType.STATUS, RespType.INTEGER, RespType.NULL):
            return self.value
        if self.type == RespType.ERROR:
            return ServerError(str(self.value))
        if self.type == RespType.BULK:
            return self.value.decode("utf-8", "replace") if decode else self.value  # type: ignore[union-attr]
        return [item.to_python(decode) for item in self.value]  # type: ignore[union-attr]


def to_bytes(arg: Arg) -> bytes:
    if isinstance(arg, bytes):
        return arg
    if isinstance(arg, (bytearray, memoryview)):
        return bytes(arg)
    if isinstance(arg, str):
        return arg.encode("utf-8")
    if isinstance(arg, bool):
        raise TypeError("bool is not a valid command argument; use 1/0 or 'yes'/'no'")
    if isinstance(arg, int):
        return str(arg).encode("ascii")
    if isinstance(arg, float):
        return repr(arg).encode("ascii")
    raise TypeError(f"unsupported argument type: {type(arg).__name__}")


@dataclass(frozen=True)
class Command:
    name: str
    args: Tuple[bytes, ...] = ()

    @staticmethod
    def of(*parts: Arg) -> "Command":
        if not parts:
            raise ValueError("empty command")
        name = to_bytes(parts[0]).decode("utf-8").upper()
        return Command(name, tuple(to_bytes(p) for p in parts[1:]))

    def __str__(self) -> str:
        return " ".join([self.name, *(a.decode("utf-8", "replace") for a in self.args)])


@dataclass(frozen=True)
class Request:
    request_id: uuid.UUID
    auth: Optional[str]
    atomic: bool
    no_reply: bool
    commands: Tuple[Command, ...]


@dataclass(frozen=True)
class Response:
    request_id: uuid.UUID
    results: Tuple[Resp, ...]


@dataclass(frozen=True)
class PubMessage:
    channel: str
    pattern: Optional[str]
    payload: bytes

    @property
    def text(self) -> str:
        return self.payload.decode("utf-8", "replace")


class _Reader:
    __slots__ = ("_b", "_p")

    def __init__(self, data: bytes) -> None:
        self._b = data
        self._p = 0

    @property
    def remaining(self) -> int:
        return len(self._b) - self._p

    def _need(self, n: int) -> None:
        if n < 0 or n > self.remaining:
            raise WireFormatError("truncated or malformed message")

    def byte(self) -> int:
        self._need(1)
        v = self._b[self._p]
        self._p += 1
        return v

    def int32(self) -> int:
        self._need(4)
        (v,) = struct.unpack_from("<i", self._b, self._p)
        self._p += 4
        return v

    def int64(self) -> int:
        self._need(8)
        (v,) = struct.unpack_from("<q", self._b, self._p)
        self._p += 8
        return v

    def raw(self, n: int) -> bytes:
        self._need(n)
        v = bytes(self._b[self._p:self._p + n])
        self._p += n
        return v

    def lp(self) -> bytes:
        return self.raw(self.int32())


def _lp(b: bytes) -> bytes:
    return struct.pack("<i", len(b)) + b


# ---------------------------------------------------------------- requests
def encode_request(request_id: uuid.UUID, commands: Sequence[Command], *, auth: Optional[str] = None,
                   atomic: bool = False, no_reply: bool = False) -> bytes:
    flags = (_FLAG_ATOMIC if atomic else 0) | (_FLAG_NOREPLY if no_reply else 0)
    out = [bytes([PROTOCOL_VERSION, flags]), request_id.bytes_le, _lp((auth or "").encode("utf-8")),
           struct.pack("<i", len(commands))]
    for c in commands:
        out.append(struct.pack("<i", len(c.args) + 1))
        out.append(_lp(c.name.encode("utf-8")))
        out.extend(_lp(a) for a in c.args)
    return b"".join(out)


def decode_request(data: bytes) -> Request:
    r = _Reader(data)
    version = r.byte()
    if version != PROTOCOL_VERSION:
        raise WireFormatError(f"unsupported protocol version {version}")
    flags = r.byte()
    rid = uuid.UUID(bytes_le=r.raw(16))
    auth = r.lp().decode("utf-8")
    count = r.int32()
    if count < 1 or count > MAX_COMMANDS:
        raise WireFormatError("invalid command count")
    commands: List[Command] = []
    for _ in range(count):
        argc = r.int32()
        if argc < 1 or argc > MAX_ARGS_PER_COMMAND:
            raise WireFormatError("invalid argument count")
        name = r.lp().decode("utf-8")
        commands.append(Command(name.upper(), tuple(r.lp() for _ in range(argc - 1))))
    if r.remaining:
        raise WireFormatError("trailing bytes in request")
    return Request(rid, auth or None, bool(flags & _FLAG_ATOMIC), bool(flags & _FLAG_NOREPLY), tuple(commands))


# --------------------------------------------------------------- responses
def _encode_value(v: Resp, out: List[bytes]) -> None:
    out.append(bytes([int(v.type)]))
    t = v.type
    if t in (RespType.STATUS, RespType.ERROR):
        out.append(_lp(str(v.value).encode("utf-8")))
    elif t == RespType.INTEGER:
        out.append(struct.pack("<q", int(v.value)))  # type: ignore[arg-type]
    elif t == RespType.BULK:
        out.append(_lp(bytes(v.value)))  # type: ignore[arg-type]
    elif t == RespType.NULL:
        pass
    else:
        items = v.value  # type: ignore[assignment]
        out.append(struct.pack("<i", len(items)))  # type: ignore[arg-type]
        for i in items:  # type: ignore[union-attr]
            _encode_value(i, out)


def encode_response(request_id: uuid.UUID, results: Iterable[Resp]) -> bytes:
    results = list(results)
    out = [bytes([PROTOCOL_VERSION]), request_id.bytes_le, struct.pack("<i", len(results))]
    for v in results:
        _encode_value(v, out)
    return b"".join(out)


def _decode_value(r: _Reader, depth: int) -> Resp:
    if depth > MAX_DEPTH:
        raise WireFormatError("nesting too deep")
    try:
        t = RespType(r.byte())
    except ValueError:
        raise WireFormatError("unknown value type") from None
    if t == RespType.STATUS:
        return Resp(t, r.lp().decode("utf-8"))
    if t == RespType.ERROR:
        return Resp(t, r.lp().decode("utf-8"))
    if t == RespType.INTEGER:
        return Resp(t, r.int64())
    if t == RespType.BULK:
        return Resp(t, r.lp())
    if t == RespType.NULL:
        return Resp(t, None)
    n = r.int32()
    if n < 0 or n > r.remaining:
        raise WireFormatError("invalid array length")
    return Resp(t, tuple(_decode_value(r, depth + 1) for _ in range(n)))


def decode_response(data: bytes) -> Response:
    r = _Reader(data)
    version = r.byte()
    if version != PROTOCOL_VERSION:
        raise WireFormatError(f"unsupported protocol version {version}")
    rid = uuid.UUID(bytes_le=r.raw(16))
    count = r.int32()
    if count < 0 or count > MAX_COMMANDS:
        raise WireFormatError("invalid result count")
    results = tuple(_decode_value(r, 0) for _ in range(count))
    if r.remaining:
        raise WireFormatError("trailing bytes in response")
    return Response(rid, results)


# ------------------------------------------------------------ publications
def encode_pub(m: PubMessage) -> bytes:
    return b"".join([bytes([PROTOCOL_VERSION, 1 if m.pattern is None else 2]),
                     _lp((m.pattern or "").encode("utf-8")), _lp(m.channel.encode("utf-8")), _lp(m.payload)])


def decode_pub(data: bytes) -> PubMessage:
    r = _Reader(data)
    if r.byte() != PROTOCOL_VERSION:
        raise WireFormatError("unsupported publish version")
    kind = r.byte()
    pattern = r.lp().decode("utf-8")
    channel = r.lp().decode("utf-8")
    payload = r.lp()
    return PubMessage(channel, pattern if kind == 2 else None, payload)
