# File: sdk/python/tests/test_protocol.py
# Author: Hadi Cahyadi <cumulus13@gmail.com>
# Date: 2026-10-12
# Description: Codec tests, including byte-for-byte golden vectors produced by the .NET implementation.
# License: MIT
import json
import pathlib
import random
import uuid

import pytest

from mqcache.errors import WireFormatError
from mqcache.protocol import (Command, PubMessage, Resp, RespType, decode_pub, decode_request, decode_response,
                              encode_pub, encode_request, encode_response)

GOLDEN = json.loads((pathlib.Path(__file__).resolve().parents[2] / "testdata" / "golden.json").read_text())


def _spec_to_resp(spec) -> Resp:
    kind = spec[0]
    if kind == "status":
        return Resp(RespType.STATUS, spec[1])
    if kind == "error":
        return Resp(RespType.ERROR, spec[1])
    if kind == "int":
        return Resp(RespType.INTEGER, int(spec[1]))
    if kind == "bulk":
        return Resp(RespType.BULK, bytes.fromhex(spec[1]))
    if kind == "null":
        return Resp(RespType.NULL, None)
    return Resp(RespType.ARRAY, tuple(_spec_to_resp(s) for s in spec[1]))


@pytest.mark.parametrize("v", GOLDEN["requests"], ids=lambda v: v["name"])
def test_request_matches_dotnet_bytes(v):
    cmds = [Command(bytes.fromhex(c[0]).decode(), tuple(bytes.fromhex(a) for a in c[1:])) for c in v["commands"]]
    mine = encode_request(uuid.UUID(v["id"]), cmds, auth=v["auth"], atomic=v["atomic"], no_reply=v["no_reply"])
    assert mine.hex() == v["hex"]
    back = decode_request(bytes.fromhex(v["hex"]))
    assert back.request_id == uuid.UUID(v["id"])
    assert back.auth == v["auth"]
    assert (back.atomic, back.no_reply) == (v["atomic"], v["no_reply"])
    assert [(c.name, c.args) for c in back.commands] == [(c.name.upper(), c.args) for c in cmds]


@pytest.mark.parametrize("v", GOLDEN["responses"], ids=lambda v: v["name"])
def test_response_matches_dotnet_bytes(v):
    expected = tuple(_spec_to_resp(s) for s in v["spec"])
    assert encode_response(uuid.UUID(v["id"]), expected).hex() == v["hex"]
    back = decode_response(bytes.fromhex(v["hex"]))
    assert back.request_id == uuid.UUID(v["id"])
    assert back.results == expected


@pytest.mark.parametrize("v", GOLDEN["pubs"], ids=lambda v: v["channel"])
def test_publication_matches_dotnet_bytes(v):
    m = PubMessage(v["channel"], v["pattern"], bytes.fromhex(v["payload"]))
    assert encode_pub(m).hex() == v["hex"]
    assert decode_pub(bytes.fromhex(v["hex"])) == m


def test_command_helpers():
    c = Command.of("set", "k", 5, b"\x00", 1.5)
    assert c.name == "SET" and c.args == (b"k", b"5", b"\x00", b"1.5")
    with pytest.raises(TypeError):
        Command.of("SET", "k", True)
    with pytest.raises(ValueError):
        Command.of()


def test_to_python_conversion():
    r = Resp(RespType.ARRAY, (Resp(RespType.BULK, b"a"), Resp(RespType.NULL), Resp(RespType.INTEGER, 3),
                              Resp(RespType.ERROR, "ERR x")))
    out = r.to_python(decode=True)
    assert out[0] == "a" and out[1] is None and out[2] == 3
    assert out[3].message == "ERR x" and out[3].code == "ERR"


def test_truncation_and_garbage_never_crash():
    good = encode_request(uuid.uuid4(), [Command.of("PING")])
    for cut in range(len(good)):
        with pytest.raises(WireFormatError):
            decode_request(good[:cut])
    rnd = random.Random(7)
    for _ in range(2000):
        junk = bytes(rnd.getrandbits(8) for _ in range(rnd.randint(0, 80)))
        for fn in (decode_request, decode_response, decode_pub):
            try:
                fn(junk)
            except WireFormatError:
                pass


def test_huge_length_and_trailing_bytes_rejected():
    good = bytearray(encode_request(uuid.uuid4(), [Command.of("PING")]))
    huge = bytearray(good)
    huge[18:22] = (0x7FFFFFFF).to_bytes(4, "little")   # auth length
    with pytest.raises(WireFormatError):
        decode_request(bytes(huge))
    with pytest.raises(WireFormatError):
        decode_request(bytes(good) + b"\x00")
    deep = encode_response(uuid.uuid4(), [Resp(RespType.NULL)])
    nested = Resp(RespType.NULL)
    for _ in range(20):
        nested = Resp(RespType.ARRAY, (nested,))
    with pytest.raises(WireFormatError):
        decode_response(encode_response(uuid.uuid4(), [nested]))
    assert decode_response(deep).results[0].type == RespType.NULL
