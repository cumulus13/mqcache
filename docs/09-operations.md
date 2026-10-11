# Operations guide

## Monitoring

* `INFO` (via `mqcache-cli INFO`): keyspace hits/misses, evictions, memory, last save, and the `# Msmq` section (`requests_received`, `requests_redelivered`, `requests_poisoned`, `dedupe_hits`, `reply_failures`, `pubsub_subscriptions`).
* `QUEUE INFO`: `request_queue_depth` is the backlog; a growing value means the server is down or overloaded. Alert on it.
* Windows Performance Monitor: *MSMQ Queue* and *MSMQ Service* counters; Computer Management shows messages per queue, journal and dead-letter queues.
* Logs: `logs\mqcache-server-yyyyMMdd.log` (service) or console. Use `loglevel debug` temporarily.

## Persistence and backup

Data lives in memory. A snapshot (`dump.mqdb`) is written atomically (`.tmp` then replace, previous file kept as `.bak`) and carries a SHA-256 trailer. Snapshots are taken by `save` rules, `SAVE`/`BGSAVE`, and on clean shutdown. On start the file is loaded; if it is corrupt it is renamed `dump.mqdb.corrupt-<time>` and the server starts empty (logged as an error).

Backup = copy `dump.mqdb` (it is always a complete, consistent file). Restore = stop the service, place the file, start.

MSMQ itself is durable: persistent (`Recoverable`) requests that arrived but were not yet processed survive a restart of MSMQ or the machine. Applied state between snapshots does not.

## Capacity and eviction

Set `maxmemory` below the machine's free RAM (accounting is approximate: key and value bytes plus per-item overhead; allow ~1.5x headroom). Choose `allkeys-lru` for a pure cache, `volatile-lru`/`volatile-ttl` when only keys with TTL may be dropped, `noeviction` when you prefer errors to data loss.

Also bound MSMQ: `queue-max-size-kb` limits the request queue so a dead server cannot fill the disk; remember the machine-wide MSMQ quota (default 1 GB of persistent messages) applies to all queues, journal and dead-letter included.

## Dead-letter, journal and poison queues

* **Dead-letter**: requests that expired before being consumed (for example the server was down for longer than the client's TTL). `DEADLETTER COUNT/PEEK/PURGE` only touch `mqcache.*` messages, because the dead-letter queue is shared with other applications.
* **Journal**: with `queue-journal yes`, every consumed request is copied to `private$\mqcache\Journal$`. It is an audit trail and grows until `queue-journal-max-kb` is reached; purge regularly (`JOURNAL PURGE`).
* **Poison**: undecodable messages and requests that fail `poison-max-attempts` times. Inspect with `POISON PEEK`, fix the cause, then `POISON REQUEUE` or `POISON PURGE`.

## Security hardening checklist

1. Set `requirepass` (long random value). Remember it is carried in the message.
2. Restrict queue access with `queue-allow` / `queue-admin` (domain groups) and firewall MSMQ ports to known clients.
3. In a domain: `queue-authenticate yes` and `queue-encryption body`.
4. Run the service under a low-privilege account (default NetworkService); grant it write access only to `dir` and the log directory.
5. Keep `max-value-bytes` and `max-batch-commands` at the smallest values you need.
6. Treat the cache as untrusted-input facing: keys/values are data, never executed; all inputs are length-checked.
7. Do not expose MSMQ to the internet. Use a VPN or private network.

See also `SECURITY.md`.

## Performance tuning

* Latency is dominated by MSMQ. Non-transactional queue + `Recoverable = false` gives the lowest overhead; transactional + recoverable the highest durability.
* Batch with pipelines: one message for many commands.
* `workers`: 2-8 is enough; the store lock serializes commands, extra workers only overlap MSMQ I/O.
* Keep reply queues local to the client (default) and avoid DNS lookups: use `tcp:<ip>`.
* Large values cost more than many small ones; keep values small.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `MSMQ is not available (ServiceNotAvailable)` | Install the MSMQ feature and start the *Message Queuing* service. |
| Client times out, server log is quiet | Wrong queue name/endpoint; message sits in an *outgoing queue* (look in Computer Management > Outgoing Queues) because the remote host is unreachable or the firewall blocks TCP 1801. |
| Requests arrive but replies never do | Server cannot reach the client's reply queue: set `ReplyHost`, check name resolution/firewall, or use `tcp:` + IP. Check `reply_failures` in `INFO`. |
| `Queue ... exists with transactional=...` at start | Config does not match the existing queue. Drain and delete the queue, or change the config back. |
| `Access is denied` creating queues | The service account lacks rights; run once elevated, or grant the account permissions in MSMQ. |
| `-OOM command not allowed` | `maxmemory` reached with `noeviction`; raise the limit or choose an eviction policy. |
| `-NOAUTH` / `-WRONGPASS` | Client password missing/incorrect. |
| Orphan `mqcache.reply.*` queues | Crashed clients; the next client start removes them (local machine only). |
| Snapshot `...corrupt-...` appears | Disk or shutdown problem; restore from backup. |

## Known limitations

* Subscriptions are leases; a subscriber that is offline longer than the lease misses messages (pub/sub is at-most-once).
* The dedupe cache is in memory only; after a server restart a retried request may execute again.
* `LRANGE`/`LINDEX` are O(n); `KEYS` is O(total keys) and holds the store lock while scanning.
* Memory accounting is an estimate.
