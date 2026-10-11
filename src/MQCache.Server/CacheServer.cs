// File: src/MQCache.Server/CacheServer.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Server engine: MSMQ request workers (transactional receive), dedupe, poison handling, replies, persistence.
// License: MIT

using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using Experimental.System.Messaging;
using MQCache.Core;
using MQCache.Core.Commands;
using MQCache.Core.Config;
using MQCache.Core.Protocol;
using MQCache.Core.Storage;
using MQCache.Messaging;

namespace MQCache.Server;

public sealed class CacheServer : IDisposable
{
    private sealed record ReplyPlan(byte[]? Body, string? Format, string CorrelationId, MessagePriority Priority);

    private readonly ServerConfig _cfg;
    private readonly ILog _log;
    private readonly KeyspaceStore _store;
    private readonly CommandProcessor _processor;
    private readonly SnapshotService _snapshots;
    private readonly ResponseCache _dedupe;
    private readonly SubscriptionRegistry _subs;
    private readonly ServerCounters _counters = new();
    private readonly ConcurrentDictionary<string, int> _attempts = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly List<Thread> _workers = new();
    private readonly string _requestPath, _poisonPath;
    private StoreMaintenance? _maintenance;
    private Timer? _pruneTimer;
    private bool _started;

    public CacheServer(ServerConfig cfg, ILog log)
    {
        _cfg = cfg; _log = log;
        _store = new KeyspaceStore(new StoreOptions { MaxMemory = cfg.MaxMemory, Policy = cfg.MaxMemoryPolicy });
        _snapshots = new SnapshotService(_store, cfg, log);
        _processor = new CommandProcessor(_store, cfg, _snapshots, log) { ExtraInfo = BuildMsmqInfo };
        _dedupe = new ResponseCache(TimeSpan.FromSeconds(cfg.DedupeWindowSeconds), cfg.DedupeMaxEntries);
        _subs = new SubscriptionRegistry(TimeSpan.FromSeconds(cfg.PubSubSubscriptionTtlSeconds));
        _requestPath = MsmqPaths.LocalPrivate(cfg.Queue);
        _poisonPath = MsmqPaths.LocalPrivate(cfg.PoisonQueue);
    }

    public KeyspaceStore Store => _store;
    public CommandProcessor Processor => _processor;
    public ServerCounters Counters => _counters;

    public void Start()
    {
        if (_started) return;
        QueueAdmin.EnsureMsmqAvailable();

        QueueAdmin.EnsureLocal(_requestPath, _cfg.QueueTransactional, new QueueSettings(
            "mqcache requests", _cfg.QueueJournal, _cfg.QueueJournalMaxKb, _cfg.QueueMaxSizeKb,
            _cfg.QueueAuthenticate, _cfg.QueueEncryption, _cfg.QueueAllow, _cfg.QueueAdmin), _log);
        QueueAdmin.EnsureLocal(_poisonPath, _cfg.QueueTransactional, new QueueSettings("mqcache poison", Admin: _cfg.QueueAdmin), _log);

        _snapshots.LoadIfPresent();
        _snapshots.Start();
        _maintenance = new StoreMaintenance(_store, _cfg.ActiveExpireIntervalMs, _log);
        _pruneTimer = new Timer(_ => _subs.Prune(), null, 5000, 5000);

        for (var i = 0; i < _cfg.Workers; i++)
        {
            var t = new Thread(() => WorkerLoop(_cts.Token)) { Name = $"mqcache-worker-{i}", IsBackground = false };
            _workers.Add(t);
            t.Start();
        }
        _started = true;
        _log.Info($"mqcache server listening on {_requestPath} (transactional={_cfg.QueueTransactional}, workers={_cfg.Workers})");
    }

    public void Stop()
    {
        if (!_started) return;
        _started = false;
        _log.Info("shutting down...");
        _cts.Cancel();
        foreach (var t in _workers)
            if (!t.Join(TimeSpan.FromSeconds(15))) _log.Warn($"worker {t.Name} did not stop in time");
        _pruneTimer?.Dispose();
        _maintenance?.Dispose();
        _snapshots.Dispose();
        if (_cfg.SaveOnShutdown && _cfg.Save.Count > 0)
        {
            try { _snapshots.Save(); }
            catch (Exception ex) { _log.Error("final snapshot failed", ex); }
        }
        _log.Info("stopped");
    }

    public void Dispose()
    {
        Stop();
        _cts.Dispose();
    }

    // ------------------------------------------------------------------ worker
    private void WorkerLoop(CancellationToken ct)
    {
        var backoff = 250;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                RunWorker(ct);
                return;
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                _log.Error($"worker crashed, restarting in {backoff} ms", ex);
                ct.WaitHandle.WaitOne(backoff);
                backoff = Math.Min(backoff * 2, 10_000);
            }
        }
    }

    private void RunWorker(CancellationToken ct)
    {
        using var requests = QueueAdmin.Open(_requestPath, QueueAccessMode.Receive);
        using var poison = QueueAdmin.Open(_poisonPath, QueueAccessMode.Send);
        using var sendCache = new SendQueueCache();
        var router = new ServerCommandRouter(_processor, _cfg, _subs, sendCache, _counters, _log);
        var timeout = TimeSpan.FromMilliseconds(_cfg.ReceiveTimeoutMs);
        while (!ct.IsCancellationRequested)
            ProcessOne(requests, poison, sendCache, router, timeout);
    }

    private void ProcessOne(MessageQueue requests, MessageQueue poison, SendQueueCache sendCache, ServerCommandRouter router, TimeSpan timeout)
    {
        using var tx = _cfg.QueueTransactional ? new MessageQueueTransaction() : null;
        tx?.Begin();
        Message msg;
        try { msg = tx is null ? requests.Receive(timeout) : requests.Receive(timeout, tx); }
        catch (MessageQueueException ex) when (ex.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
        {
            tx?.Abort();
            return;
        }

        using (msg)
        {
            Interlocked.Increment(ref _counters.Requests);
            ReplyPlan? plan = null;
            string? poisonReason = null;
            try { plan = Handle(msg, router); }
            catch (Exception ex) when (ex is WireFormatException or InvalidDataException)
            {
                poisonReason = "malformed: " + ex.Message;
            }
            catch (Exception ex)
            {
                var attempt = _attempts.AddOrUpdate(msg.Id, 1, (_, v) => v + 1);
                if (_attempts.Count > 10_000) _attempts.Clear();
                if (tx is not null && attempt < _cfg.PoisonMaxAttempts)
                {
                    _log.Warn($"request {msg.Id} failed (attempt {attempt}/{_cfg.PoisonMaxAttempts}); will be redelivered", ex);
                    Interlocked.Increment(ref _counters.Redeliveries);
                    tx.Abort();
                    Thread.Sleep(Math.Min(50 * attempt, 1000));
                    return;
                }
                poisonReason = "failed: " + ex.GetType().Name;
                _log.Error($"request {msg.Id} moved to poison queue", ex);
            }

            if (poisonReason is not null)
            {
                MoveToPoison(poison, msg, poisonReason, tx);
                Interlocked.Increment(ref _counters.Poisoned);
                var err = WireCodec.EncodeResponse(new ResponseEnvelope(Guid.Empty, new[] { RespValue.Error("ERR request rejected: " + poisonReason) }));
                plan = new ReplyPlan(err, FormatOf(msg), msg.Id, msg.Priority);
            }

            try { tx?.Commit(); }
            catch (MessageQueueException ex)
            {
                _log.Error("commit failed; request will be redelivered (dedupe cache protects against double execution)", ex);
                return;
            }
            _attempts.TryRemove(msg.Id, out _);
            if (plan?.Body is not null) SendReply(plan, sendCache);
        }
    }

    private ReplyPlan? Handle(Message msg, ServerCommandRouter router)
    {
        var body = MessageFactory.ReadBody(msg, MessageFactory.MaxBodyBytes);
        var req = WireCodec.DecodeRequest(body);
        var format = FormatOf(msg);

        byte[] responseBytes;
        if (_dedupe.TryGet(req.RequestId, out var cached))
        {
            Interlocked.Increment(ref _counters.DedupeHits);
            responseBytes = cached;
        }
        else
        {
            var results = Execute(req, router, new RequestContext(format));
            responseBytes = WireCodec.EncodeResponse(new ResponseEnvelope(req.RequestId, results));
            _dedupe.Put(req.RequestId, responseBytes);
        }
        return req.NoReply ? null : new ReplyPlan(responseBytes, format, msg.Id, msg.Priority);
    }

    private IReadOnlyList<RespValue> Execute(RequestEnvelope req, ServerCommandRouter router, RequestContext ctx)
    {
        if (!AuthOk(req.Auth))
            return Enumerable.Repeat(RespValue.Error(req.Auth is null ? "NOAUTH Authentication required." : "WRONGPASS invalid password"), req.Commands.Count).ToList();
        if (req.Commands.Count > _cfg.MaxBatchCommands)
            return Enumerable.Repeat(RespValue.Error($"ERR batch too large (max-batch-commands {_cfg.MaxBatchCommands})"), req.Commands.Count).ToList();

        List<RespValue> Run() => req.Commands.Select(c => router.Route(c, ctx)).ToList();
        return req.Atomic ? _store.Atomically(Run) : Run();
    }

    private bool AuthOk(string? supplied)
    {
        if (_cfg.RequirePass is null) return true;
        if (supplied is null) return false;
        return CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(supplied), Encoding.UTF8.GetBytes(_cfg.RequirePass));
    }

    private static string? FormatOf(Message msg)
    {
        try { return msg.ResponseQueue?.FormatName; }
        catch (Exception ex) when (ex is InvalidOperationException or MessageQueueException) { return null; }
    }

    private void MoveToPoison(MessageQueue poison, Message original, string reason, MessageQueueTransaction? tx)
    {
        using var copy = MessageFactory.Poison(original, reason);
        if (tx is null) poison.Send(copy, MessageQueueTransactionType.None); else poison.Send(copy, tx);
    }

    private void SendReply(ReplyPlan plan, SendQueueCache cache)
    {
        if (plan.Format is null || plan.Body is null) return;
        var ttl = TimeSpan.FromSeconds(_cfg.ReplyTtlSeconds);
        try
        {
            var q = cache.Get(plan.Format);
            using var reply = MessageFactory.Response(plan.Body, plan.CorrelationId, plan.Priority, ttl, _cfg.ReplyRecoverable);
            try { q.Send(reply, MessageQueueTransactionType.None); }
            catch (MessageQueueException ex) when (ex.MessageQueueErrorCode == MessageQueueErrorCode.TransactionUsage)
            {
                // The client's reply queue is transactional: resend as a single-message transaction.
                q.Send(reply, MessageQueueTransactionType.Single);
            }
        }
        catch (MessageQueueException ex)
        {
            cache.Invalidate(plan.Format);
            Interlocked.Increment(ref _counters.ReplyFailures);
            _log.Debug($"reply to {plan.Format} failed: {ex.MessageQueueErrorCode}");
        }
    }

    private string BuildMsmqInfo()
    {
        var sb = new StringBuilder();
        sb.Append("# Msmq\r\n")
          .Append("request_queue:").Append(_requestPath).Append("\r\n")
          .Append("transactional:").Append(_cfg.QueueTransactional ? "yes" : "no").Append("\r\n")
          .Append("workers:").Append(_cfg.Workers).Append("\r\n")
          .Append("requests_received:").Append(Interlocked.Read(ref _counters.Requests)).Append("\r\n")
          .Append("commands_routed:").Append(Interlocked.Read(ref _counters.Commands)).Append("\r\n")
          .Append("requests_redelivered:").Append(Interlocked.Read(ref _counters.Redeliveries)).Append("\r\n")
          .Append("requests_poisoned:").Append(Interlocked.Read(ref _counters.Poisoned)).Append("\r\n")
          .Append("dedupe_hits:").Append(Interlocked.Read(ref _counters.DedupeHits)).Append("\r\n")
          .Append("dedupe_entries:").Append(_dedupe.Count).Append("\r\n")
          .Append("reply_failures:").Append(Interlocked.Read(ref _counters.ReplyFailures)).Append("\r\n")
          .Append("pubsub_subscriptions:").Append(_subs.TotalSubscriptions).Append("\r\n")
          .Append("messages_published:").Append(Interlocked.Read(ref _counters.Published)).Append("\r\n")
          .Append("messages_delivered:").Append(Interlocked.Read(ref _counters.Delivered)).Append("\r\n");
        return sb.ToString();
    }
}
