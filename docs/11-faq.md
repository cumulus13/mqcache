# FAQ

**Is this compatible with Redis clients?** No. It mimics the command set and reply types, not the RESP/TCP protocol. Use `MQCache.Client` or implement chapter 8.

**Why not just use Redis?** Use Redis if you can. MQCache exists for estates standardised on MSMQ where opening TCP services is undesirable, or where store-and-forward behaviour for cache writes is valuable.

**Can several servers share one queue?** Not for one dataset (each holds its own memory). Run separate servers with different `queue` names and shard in the client.

**Does it work on Linux?** No. MSMQ is Windows-only. `MQCache.Core` builds anywhere and is reusable.

**What happens if the client sends while the server is down?** The message waits (persistent) until its TTL expires, then moves to the dead-letter queue; the client sees a timeout after its retries.

**How do I run two environments side by side?** Use different `queue` names, `dir`, and `workers`; clients choose with `QueueName`.
