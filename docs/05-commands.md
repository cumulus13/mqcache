# Command reference

Command names are case-insensitive. Keys, fields and set members are UTF-8 text; values are binary-safe. Replies follow Redis conventions (status, error, integer, bulk, nil, array). Errors carry Redis-style prefixes (`ERR`, `WRONGTYPE`, `OOM`, `NOAUTH`, `WRONGPASS`).

## Strings

| Command | Description |
|---|---|
| `GET key` | Value or nil. |
| `SET key value [EX s] [PX ms] [NX] [XX] [KEEPTTL]` | `OK`, or nil when the NX/XX condition fails. |
| `SETNX key value` | `1` if set. |
| `SETEX key seconds value` / `PSETEX key ms value` | Set with TTL. |
| `GETSET key value` | Set and return the old value. |
| `APPEND key value` | New length. |
| `STRLEN key` | Length. |
| `INCR` / `DECR key`, `INCRBY` / `DECRBY key n` | 64-bit integer arithmetic; overflow is an error. |
| `MGET key...` / `MSET key value...` | Multiple get/set. |

## Keys and expiry

| Command | Description |
|---|---|
| `DEL key...` / `EXISTS key...` | Counts. |
| `TYPE key` | `string`, `hash`, `list`, `set`, `none`. |
| `KEYS pattern` | Glob (`* ? [abc] [a-z] [^x] \`). Capped by `keys-max-results`. O(N). |
| `RENAME src dst` | Keeps TTL; overwrites `dst`. |
| `EXPIRE key s` / `PEXPIRE key ms` | Non-positive TTL deletes the key. |
| `TTL key` / `PTTL key` | `-2` missing, `-1` no TTL. |
| `PERSIST key` | Remove TTL. |
| `DBSIZE`, `FLUSHALL`, `FLUSHDB` | Single database; both flush commands are equivalent. |

## Hashes

`HSET key field value [field value...]` (returns new fields), `HMSET`, `HGET`, `HMGET`, `HDEL`, `HGETALL`, `HKEYS`, `HVALS`, `HLEN`, `HEXISTS`, `HINCRBY key field n`.

## Lists

`LPUSH`, `RPUSH key value...`, `LPOP`, `RPOP` (single element), `LLEN`, `LRANGE key start stop` (negative indexes allowed), `LINDEX key index`. Index access is O(n).

## Sets

`SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`. Empty hashes/lists/sets are deleted automatically.

## Server

| Command | Description |
|---|---|
| `PING [msg]`, `ECHO msg`, `TIME` | Connectivity and clock. |
| `INFO` | Server, stats, memory, persistence, keyspace and `# Msmq` sections. |
| `SAVE` / `BGSAVE` / `LASTSAVE` | Snapshot now / in background / last save (unix seconds). |
| `CONFIG GET pattern`, `CONFIG SET maxmemory|maxmemory-policy v` | See chapter 4. |

## Pub/Sub

| Command | Description |
|---|---|
| `PUBLISH channel message` | Returns the number of subscriber queues the message was sent to. |
| `SUBSCRIBE` / `PSUBSCRIBE ch...` | Register the request's reply queue. Normally used through `CacheSubscriber`, which owns a dedicated queue and renews the lease. |
| `UNSUBSCRIBE` / `PUNSUBSCRIBE [ch...]` | Remove subscriptions (all when no names). |
| `PUBSUB CHANNELS [pattern]`, `PUBSUB NUMSUB ch...` | Introspection. |

Delivery is **at-most-once** (express messages with a TTL). A subscriber that cannot be reached three times in a row is dropped.

## MSMQ administration

| Command | Description |
|---|---|
| `QUEUE INFO` | Depth of request/poison/journal/dead-letter queues, subscriptions. |
| `DEADLETTER COUNT` / `PEEK [n]` / `PURGE` | System dead-letter queues; **only messages labelled `mqcache.*` are counted, shown or removed**. |
| `JOURNAL COUNT` / `PURGE` | Request-queue journal (requires `queue-journal yes`). |
| `POISON COUNT` / `PEEK [n]` / `PURGE` / `REQUEUE [n]` | Park management; `REQUEUE` moves messages back atomically. |

## Pipelines and atomicity

Several commands travel in **one MSMQ message** (`CacheClient.Pipeline`). With `atomic: true` (default) the server runs them while holding the store lock, so no other client's command interleaves; there is no rollback (like Redis `MULTI`, an error in one command does not undo the others). The whole pipeline is idempotent under retry because it shares one request id.

## Not supported

Blocking commands, `WATCH/MULTI/EXEC`, `SCAN`, sorted sets, streams, Lua, multiple databases, replication/cluster, `AUTH` as a command (the password is part of each request).
