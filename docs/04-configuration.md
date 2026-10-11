# Configuration reference

`mqcache.conf` is `redis.conf` style: `directive value [value...]`, `#` starts a comment, use double quotes for values containing spaces or `#`. Unknown directives are an **error** (typos are caught at startup). Any directive may be overridden on the command line: `mqcache-server mqcache.conf --workers 8 --maxmemory 512mb`.

## MSMQ

| Directive | Default | Description |
|---|---|---|
| `queue` | `private$\mqcache` | Request queue (private, local). Clients address it as `<host>/<name>`. |
| `queue-transactional` | `yes` | Transactional queue (exactly-once receive, redelivery). Cannot be changed on an existing queue. |
| `queue-journal` | `no` | Keep a copy of consumed requests in the queue journal (audit). |
| `queue-journal-max-kb` | `0` | `MaximumJournalSize` (0 = MSMQ default). |
| `queue-max-size-kb` | `0` | `MaximumQueueSize` (0 = MSMQ default). |
| `queue-authenticate` | `no` | Require authenticated messages (needs AD/certificates). |
| `queue-encryption` | `none` | `none`, `optional`, `body` (needs AD/certificates). |
| `queue-allow` | - | Account with SEND rights (repeatable). |
| `queue-admin` | - | Account with full control (repeatable). |
| `workers` | `4` | Parallel receive loops (1..64). `1` gives strict global request ordering. |
| `receive-timeout-ms` | `1000` | Receive poll timeout; also the shutdown latency. |
| `reply-ttl-seconds` | `60` | `TimeToBeReceived` of replies/publications. |
| `reply-recoverable` | `no` | Persist replies to disk. |
| `poison-queue` | `private$\mqcache.poison` | Where failing requests are parked. |
| `poison-max-attempts` | `3` | Attempts before a failing request is parked. |
| `dedupe-window-seconds` | `60` | Idempotency window (0 = off). |
| `dedupe-max-entries` | `100000` | Bound of the idempotency cache. |

## Store

| Directive | Default | Description |
|---|---|---|
| `maxmemory` | `0` | Memory limit for data (accounted by the store, not process RSS). `64kb`, `512mb`, `2gb`. |
| `maxmemory-policy` | `noeviction` | `noeviction`, `allkeys-lru`, `volatile-lru`, `volatile-ttl`. |
| `max-value-bytes` | `1048576` | Largest argument of a write (max 3670016 because of the 4 MiB MSMQ limit). |
| `max-batch-commands` | `1000` | Commands per pipeline. |
| `keys-max-results` | `10000` | Cap of `KEYS`. |
| `active-expire-interval-ms` | `100` | Expired-key sweep period. |

With `noeviction`, write commands that add memory return `-OOM` once the limit is reached; reads and deletes keep working. With an eviction policy, least-recently-used keys are evicted until usage fits.

## Security

| Directive | Description |
|---|---|
| `requirepass` | Password required in every request (compared in constant time). It travels inside the message: combine with queue ACLs, `queue-encryption` (domain) or a trusted network. |

## Persistence

| Directive | Default | Description |
|---|---|---|
| `dir` | `./data` | Snapshot directory. |
| `dbfilename` | `dump.mqdb` | Snapshot file name. |
| `save <seconds> <changes>` | `900 1`, `300 100`, `60 10000` | Repeatable. `save ""` disables snapshots. |
| `save-on-shutdown` | `yes` | Final snapshot on clean stop. |

## Pub/Sub

| Directive | Default | Description |
|---|---|---|
| `pubsub-subscription-ttl-seconds` | `120` | Subscriber lease; clients renew every 30 s. |
| `multicast-channel <channel> <ip:port>` | - | Also publish to an MSMQ multicast group (repeatable). |

## Logging

| Directive | Default | Description |
|---|---|---|
| `loglevel` | `info` | `debug`, `info`, `warn`, `error`. |
| `logfile` | - | Daily rolling file `name-yyyyMMdd.log`. As a service the default is `.\logs\mqcache-server.log`. |

## `CONFIG` command

`CONFIG GET <pattern>` returns effective values (`requirepass` is masked). `CONFIG SET maxmemory|maxmemory-policy <value>` changes those two at runtime.

## Exit codes of `mqcache-server`

`0` clean stop, `1` fatal error (for example MSMQ unavailable), `2` configuration error.
