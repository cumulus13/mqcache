# Client library (.NET)

```powershell
dotnet add package MQCache.Client
```

Target: `net8.0-windows`. The client is **thread-safe**; share one `CacheClient` per process. Each client owns a private reply queue (removed on `Dispose`).

## Connecting

```csharp
using MQCache.Client;

using var cache = CacheClient.Connect("tcp:10.0.0.5");                       // endpoint
using var cache2 = CacheClient.Connect("endpoint=tcp:10.0.0.5;password=s3;timeout=3000;retries=1");
using var local = new CacheClient(new CacheClientOptions { QueueName = "mqcache", Password = "s3" });
```

Endpoint grammar: `.` (local), `host`, `os:host`, `tcp:ip`, optionally `/queue` (`tcp:10.0.0.5/orders-cache`).

## Options

| Option | Default | Meaning |
|---|---|---|
| `Endpoint`, `QueueName` | `.`, `mqcache` | Where the server queue lives. |
| `Transactional` | `true` | Initial guess; auto-corrected if the server queue is the other kind. |
| `Password` | - | Sent with each request. |
| `Timeout` | 5 s | Per attempt. |
| `Retries` | 2 | Re-sends with the **same request id** after a timeout (safe for `INCR` etc.). |
| `Priority` | Normal | MSMQ priority of requests. |
| `Recoverable` | `true` | Persistent request messages. |
| `UseJournal`, `UseDeadLetter`, `UseTracing` | `false`, `true`, `false` | MSMQ message flags. |
| `ReplyHost`, `ReplyScheme` | machine name, `os` | How the server addresses your reply queue. |
| `SweepOrphanQueues` | `true` | Delete reply/subscriber queues of dead processes at start. |

## Typed helpers

```csharp
cache.Set("user:1", "alice", ttl: TimeSpan.FromMinutes(5));
cache.Set("lock:x", "me", TimeSpan.FromSeconds(10), SetWhen.NotExists);   // SET NX PX
string? v = cache.GetString("user:1");
long n = cache.Increment("hits");
cache.HashSet("session:9", "ip", "10.0.0.1");
var all = cache.HashGetAll("session:9");
cache.ListPush("jobs", "a"); string? job = cache.ListPop("jobs");
cache.SetAdd("tags", "x", "y");
TimeSpan rtt = cache.Ping();
```

Error replies (`WRONGTYPE`, `OOM`, `NOAUTH`...) throw `CacheServerException`; timeouts throw `CacheTimeoutException`; MSMQ problems throw `CacheException`.

## Raw commands, pipelines, per-call options

```csharp
RespValue r = cache.Execute("SET", "k", "v", "EX", "10");            // does not throw on error replies; check r.IsError

var results = cache.Pipeline(new[]
{
    Command.Of("SET", "a", "1"),
    Command.Of("INCR", "a"),
    Command.Of("GET", "a"),
});                                                                    // one MSMQ round trip, atomic

cache.Execute(Command.Of("PING"), new CallOptions { Priority = MessagePriority.Highest, Timeout = TimeSpan.FromSeconds(1) });
```

## Fire-and-forget with acknowledgment

```csharp
cache.Send(Command.Of("SET", "k", "v"));                               // no reply expected
cache.Send(Command.Of("SET", "k", "v"), waitForReceiveAck: true);       // MSMQ confirms the server consumed it
```

## Pub/Sub

```csharp
using var sub = cache.Subscribe("news");           // or cache.PSubscribe("news.*")
sub.MessageReceived += m => Console.WriteLine($"{m.Channel}: {m.PayloadText}");
// or pull-style:
foreach (var m in sub.ReadAll(token)) { /* ... */ }

cache.Publish("news", "hello");                    // returns number of subscriber queues reached
```

A subscriber owns its queue and renews its lease every `SubscriptionRenewInterval` (30 s); disposing it unsubscribes. Messages are at-most-once. For MSMQ multicast fan-out configure `multicast-channel news 234.1.1.1:8001` on the server and `cache.SubscribeMulticast("234.1.1.1:8001")` on clients (requires the MSMQ multicast feature).

## Patterns

* **Cache-aside**: `GetString` then `Set(..., ttl)`.
* **Distributed lock**: `Set(key, token, ttl, SetWhen.NotExists)`; release with `Delete` after checking the token.
* **Rate limit**: `Pipeline(INCR key, EXPIRE key 60)`.
* **High-throughput writes**: batch into pipelines (one message per batch) and keep `Recoverable = false` if persistence in MSMQ is not needed.
