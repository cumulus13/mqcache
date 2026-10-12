# File: sdk/python/src/mqcache/cli.py
# Author: Hadi Cahyadi <cumulus13@gmail.com>
# Date: 2026-10-12
# Description: Minimal command line: python -m mqcache [-e endpoint] [-q queue] [-a password] COMMAND [args...]
# License: MIT
from __future__ import annotations

import argparse
import shlex
import sys
from typing import Any, List

from .client import CacheClient
from .errors import MQCacheError, ServerError


def format_value(v: Any, indent: int = 0) -> str:
    pad = " " * indent
    if v is None:
        return pad + "(nil)"
    if isinstance(v, ServerError):
        return pad + "(error) " + v.message
    if isinstance(v, bool):
        return pad + ("(integer) 1" if v else "(integer) 0")
    if isinstance(v, int):
        return pad + f"(integer) {v}"
    if isinstance(v, bytes):
        v = v.decode("utf-8", "replace")
    if isinstance(v, str):
        return pad + v if v in ("OK", "PONG") else pad + '"' + v.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n") + '"'
    if isinstance(v, list):
        if not v:
            return pad + "(empty array)"
        return "\n".join(f"{pad}{i}) " + format_value(x, 0 if not isinstance(x, list) else indent + 3).lstrip()
                         for i, x in enumerate(v, 1))
    return pad + repr(v)


def _run(client: CacheClient, parts: List[str]) -> bool:
    try:
        print(format_value(client.execute_command(*parts)))
        return True
    except ServerError as ex:
        print("(error) " + ex.message)
        return False
    except MQCacheError as ex:
        print("error: " + str(ex), file=sys.stderr)
        return False


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="mqcache-py", description="MQCache command line client (Python SDK)")
    ap.add_argument("-e", "--endpoint", default=".", help='"." | host | os:host | tcp:10.0.0.5')
    ap.add_argument("-q", "--queue", default="mqcache")
    ap.add_argument("-a", "--auth", default=None, help="password (requirepass)")
    ap.add_argument("-t", "--timeout", type=float, default=5.0, help="seconds per attempt")
    ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--no-tx", action="store_true", help="server queue is NOT transactional (otherwise auto-detected)")
    ap.add_argument("command", nargs=argparse.REMAINDER)
    ns = ap.parse_args(argv)
    try:
        client = CacheClient(ns.endpoint, queue=ns.queue, password=ns.auth, timeout=ns.timeout, retries=ns.retries,
                             transactional=not ns.no_tx, decode_responses=True)
    except MQCacheError as ex:
        print("error: " + str(ex), file=sys.stderr)
        return 1
    with client:
        if ns.command:
            return 0 if _run(client, ns.command) else 1
        while True:
            try:
                line = input(f"{ns.endpoint}/{ns.queue}> ")
            except (EOFError, KeyboardInterrupt):
                print()
                return 0
            parts = shlex.split(line)
            if not parts:
                continue
            if parts[0].upper() in ("QUIT", "EXIT"):
                return 0
            _run(client, parts)


if __name__ == "__main__":
    raise SystemExit(main())
