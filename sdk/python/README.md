# mqcache (Python SDK)

Python client for [MQCache](../../README.md), the Redis-like cache served over Microsoft Message Queuing. **Windows only** (MSMQ).

```powershell
pip install mqcache          # installs pywin32 on Windows
```

```python
from mqcache import CacheClient

with CacheClient("tcp:10.0.0.5", password="secret", decode_responses=True) as c:   # "." = this machine
    c.set("user:1", "alice", ex=300)
    print(c.get("user:1"))                       # alice
    print(c.incr("hits"))
    c.hset("session:9", mapping={"ip": "10.0.0.1", "ua": "x"})
    print(c.pipeline([("SET", "a", 1), ("INCR", "a"), ("GET", "a")]))   # ONE MSMQ message, atomic

    with c.subscriber("news") as sub:            # own queue, lease renewed automatically
        c.publish("news", "hello")
        print(sub.get_message(timeout=5).text)   # hello
```

CLI: `python -m mqcache -e tcp:10.0.0.5 -a secret GET user:1` (or `mqcache-py`).

## Behaviour

* Same semantics as the .NET client: request id reused on retries (server de-duplicates), transactional/non-transactional auto-detection, priorities (`Priority.HIGH`...), TTL, persistent messages, dead-letter/journal flags, password.
* Thread-safe; one reply queue per client (`mqcache.pyreply.<pid>.<id>`), removed by `close()`. Replies are demultiplexed in-process because the MSMQ COM API has no selective receive.
* Errors: `ServerError` (with `.code`), `CacheTimeoutError`, `TransportError`, `MsmqUnavailableError`.
* Replies: bytes by default, `decode_responses=True` for `str`.

## Not in 0.1

Acknowledgment waiting (`Send(..., waitForReceiveAck)`), MSMQ multicast subscribers, and automatic cleanup of orphaned queues left by a crashed process (delete `mqcache.pyreply.*` / `mqcache.pysub.*` queues by hand, e.g. `Get-MsmqQueue -QueueType Private | ? Name -like '*mqcache.py*'`).

## Testing status (read this)

* The wire codec is verified **byte for byte** against vectors produced by the .NET implementation (`sdk/testdata/golden.json`).
* Client logic (retries, dedupe, demux, pipelines, pub/sub, endpoint parsing) is tested against an in-memory MSMQ stand-in and a reference server: `pytest` (runs on any OS).
* The **MSMQ COM transport** (`mqcache/msmq_com.py`) could not be exercised without Windows+MSMQ when it was written. Verify it with `tests/test_integration_windows.py` against a running `mqcache-server` (instructions in the file) before relying on it.

## Architecture

`protocol.py` (codec) - `transport.py` (abstraction + endpoints) - `msmq_com.py` (pywin32 COM) - `client.py` (client, subscriber). Provide your own `Transport` (for example a ctypes `mqrt.dll` one) via `CacheClient(..., transport=...)`.
