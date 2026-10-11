# MSMQ feature matrix

The design started from a survey of MSMQ capabilities (Microsoft Learn, the Message Queuing 4.0 feature list, and the .NET `System.Messaging` API). This chapter lists each capability, how MQCache uses it, and what is deliberately not used.

| MSMQ capability | Used? | How / where | Config or API |
|---|---|---|---|
| Private queues | Yes | Request, poison, reply and subscriber queues | `queue`, `poison-queue` |
| Public queues (Active Directory) | No | Needs AD; the .NET Core port excludes AD integration | - |
| Direct format names (`OS:`, `TCP:`) | Yes | Remote servers/clients: `FormatName:DIRECT=TCP:10.0.0.5\private$\mqcache` | `endpoint tcp:10.0.0.5` |
| HTTP/HTTPS format names | No | Not exposed; possible by passing a custom endpoint in a fork | - |
| Transactional queues | Yes | Exactly-once receive, redelivery on failure | `queue-transactional` |
| Non-transactional queues | Yes | Lower overhead, replies, pub/sub | `queue-transactional no` |
| Transaction-type auto-detection | Yes | Client retries with the other send type on `TransactionUsage` | automatic |
| Recoverable (persistent) vs express messages | Yes | Requests persistent by default; replies/publications express | `CacheClientOptions.Recoverable`, `reply-recoverable` |
| Message priority | Yes | 8 levels; `PING`/admin requests can jump the backlog | `CacheClientOptions.Priority`, `CallOptions.Priority` |
| `TimeToBeReceived` | Yes | Requests, replies and publications expire instead of piling up | client `Timeout`, `reply-ttl-seconds` |
| `TimeToReachQueue` | Not set | MSMQ default (follows TTBR) | - |
| Dead-letter queue (`UseDeadLetterQueue`) | Yes | Expired/undeliverable requests; admin via `DEADLETTER` | `CacheClientOptions.UseDeadLetter` |
| Transactional dead-letter queue | Yes | Same, for transactional messages | `DEADLETTER` checks both |
| Journal queue (source/target) | Yes | Target journal of the request queue = audit trail; message journal flag | `queue-journal`, `UseJournal` |
| Journal quota | Yes | `MaximumJournalSize` | `queue-journal-max-kb` |
| Queue quota | Yes | `MaximumQueueSize` | `queue-max-size-kb` |
| Acknowledgments (`AcknowledgeType`, admin queue) | Yes | `Send(cmd, waitForReceiveAck: true)` | API |
| Selective receive by correlation id | Yes | Request/reply matching, shared reply queue | automatic |
| Cursors / enumerators (peek without removing) | Yes | `QUEUE INFO`, `POISON PEEK`, `DEADLETTER PEEK`, selective purge | admin commands |
| Message labels | Yes | `mqcache.req`, `mqcache.resp`, `mqcache.pub`, `mqcache.poison:*` (visible in MMC); used to touch only our messages in shared system queues | - |
| Poison-message handling | Yes (own) | MSMQ 4.0 retry sub-queues are WCF-driven; MQCache implements its own attempt counter and poison queue | `poison-max-attempts`, `POISON *` |
| Sub-queues (MSMQ 4.0) | No | Not exposed by the .NET Core port; the poison queue is a normal queue | - |
| Authentication (`Authenticate`, sender cert) | Optional | Queue flag; requires AD/certificates | `queue-authenticate` |
| Encryption (`EncryptionRequired`) | Optional | `none/optional/body`; requires AD/certificates | `queue-encryption` |
| Queue ACLs | Yes | `SetPermissions` for send / full-control accounts | `queue-allow`, `queue-admin` |
| Multicast (PGM) | Optional | Server publishes selected channels to a multicast group; clients can join | `multicast-channel`, `SubscribeMulticast` |
| Tracing / report queue | Flag only | `UseTracing` can be set per client; report queue must be configured in MSMQ | `CacheClientOptions.UseTracing` |
| Queue creation with properties, labels, categories | Yes | Created and (re)configured at start | - |
| Orphan-queue hygiene | Yes | Reply queues named after pid+start time, swept by later clients | `SweepOrphanQueues` |
| Distribution lists, queue aliases, MQIS/AD lookup | No | Require Active Directory | - |
| Message routing servers | N/A | Provided by MSMQ itself between machines | - |
| Per-application dead-letter queue (MSMQ 4.0, `PROPID_M_DEADLETTER_QUEUE`) | No | Not exposed by `System.Messaging` | - |
| Transactional remote receive (MSMQ 4.0) | No | Clients receive replies from a local queue | - |

## Platform notes

* The .NET 8 API is the community port `Experimental.System.Messaging` 1.2.0 (unofficial, **Ms-RL** licensed). MQCache itself is MIT; the dependency is a package reference only.
* Microsoft documents MSMQ as a feature that must be installed (Windows feature `MSMQ-Server`) and only recommends it for existing estates; evaluate this against your own support policy.
