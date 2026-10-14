# Queue-type detection: documentation wording to paste

**docs/02-msmq-feature-matrix.md**, row "Transaction-type auto-detection":

| Transaction-type detection | Yes | Local server queue: the client reads the queue's `Transactional` flag. Remote queue (not queryable): on the first request the client alternates between the two send modes. MSMQ rejects a non-transactional send to a transactional queue (`TransactionUsage`) but silently drops a transactional send to a non-transactional queue, so that direction shows up as a timeout. | `CacheClientOptions.DetectQueueType` |

**docs/06-client-library.md** options table: `DetectQueueType` (default `true`) - read the transactional flag of a local server queue; remote queues are probed on the first request.

**docs/12-python-sdk.md** options: `detect_queue_type` (default `True`) - same behaviour.

**CHANGELOG.md** (next version, Fixed): Client (.NET and Python): a transactional send to a non-transactional queue is silently dropped by MSMQ, which made the first request time out. The client now reads the flag of local queues and otherwise alternates send modes until a reply confirms the queue kind. Found by the real-MSMQ integration job.
