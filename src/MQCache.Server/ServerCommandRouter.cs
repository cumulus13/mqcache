// File: src/MQCache.Server/ServerCommandRouter.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Routes MSMQ-aware commands (pub/sub, queue admin) and delegates everything else to the CommandProcessor.
// License: MIT

using System.Text;
using Experimental.System.Messaging;
using MQCache.Core;
using MQCache.Core.Commands;
using MQCache.Core.Config;
using MQCache.Core.Protocol;
using MQCache.Messaging;

namespace MQCache.Server;

public sealed record RequestContext(string? ResponseFormat);

public sealed class ServerCounters
{
    public long Requests, Commands, Poisoned, ReplyFailures, Published, Delivered, DedupeHits, Redeliveries;
}

/// <summary>One router per worker thread: it owns a (non-thread-safe) cache of MSMQ send queues.</summary>
public sealed class ServerCommandRouter
{
    private static readonly HashSet<string> Own = new(StringComparer.Ordinal)
    {
        "PUBLISH", "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PUBSUB", "QUEUE", "DEADLETTER", "JOURNAL", "POISON",
    };

    private readonly CommandProcessor _processor;
    private readonly ServerConfig _cfg;
    private readonly SubscriptionRegistry _subs;
    private readonly SendQueueCache _send;
    private readonly ServerCounters _counters;
    private readonly ILog _log;
    private readonly string _requestPath, _poisonPath, _journalPath;

    public ServerCommandRouter(CommandProcessor processor, ServerConfig cfg, SubscriptionRegistry subs,
        SendQueueCache send, ServerCounters counters, ILog log)
    {
        _processor = processor; _cfg = cfg; _subs = subs; _send = send; _counters = counters; _log = log;
        _requestPath = MsmqPaths.LocalPrivate(cfg.Queue);
        _poisonPath = MsmqPaths.LocalPrivate(cfg.PoisonQueue);
        _journalPath = MsmqPaths.Journal(_requestPath);
    }

    public RespValue Route(Command c, RequestContext ctx)
    {
        Interlocked.Increment(ref _counters.Commands);
        if (!Own.Contains(c.Name)) return _processor.Execute(c);
        try
        {
            return c.Name switch
            {
                "PUBLISH" => Publish(c),
                "SUBSCRIBE" => Subscribe(c, ctx, pattern: false),
                "PSUBSCRIBE" => Subscribe(c, ctx, pattern: true),
                "UNSUBSCRIBE" => Unsubscribe(c, ctx, pattern: false),
                "PUNSUBSCRIBE" => Unsubscribe(c, ctx, pattern: true),
                "PUBSUB" => PubSub(c),
                "QUEUE" => QueueInfo(c),
                "DEADLETTER" => DeadLetter(c),
                "JOURNAL" => Journal(c),
                _ => Poison(c),
            };
        }
        catch (MessageQueueException ex)
        {
            return RespValue.Error($"ERR msmq: {ex.MessageQueueErrorCode}: {ex.Message}");
        }
    }

    // ------------------------------------------------------------------ pub/sub
    private RespValue Publish(Command c)
    {
        if (c.Count != 2) return Arity(c);
        var channel = c.Str(0);
        var ttl = TimeSpan.FromSeconds(_cfg.ReplyTtlSeconds);
        var bodies = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        var delivered = 0;
        foreach (var (format, pattern) in _subs.Targets(channel))
        {
            var key = pattern ?? "\0";
            if (!bodies.TryGetValue(key, out var body))
                bodies[key] = body = PubSubCodec.Encode(new PubMessage(channel, pattern, c.Args[1]));
            if (Deliver(format, body, ttl)) delivered++;
        }
        foreach (var mc in _cfg.MulticastChannels.Where(m => m.Channel == channel))
            Deliver("MULTICAST=" + mc.Address, PubSubCodec.Encode(new PubMessage(channel, null, c.Args[1])), ttl);
        Interlocked.Increment(ref _counters.Published);
        Interlocked.Add(ref _counters.Delivered, delivered);
        return RespValue.Int(delivered);
    }

    private bool Deliver(string format, byte[] body, TimeSpan ttl)
    {
        try
        {
            var q = _send.Get(format);
            using var m = MessageFactory.Publication(body, ttl);
            q.Send(m, MessageQueueTransactionType.None);
            _subs.ReportSuccess(format);
            return true;
        }
        catch (MessageQueueException ex)
        {
            _send.Invalidate(format);
            _subs.ReportFailure(format);
            _log.Debug($"publish to {format} failed: {ex.MessageQueueErrorCode}");
            return false;
        }
    }

    private RespValue Subscribe(Command c, RequestContext ctx, bool pattern)
    {
        if (c.Count < 1) return Arity(c);
        if (ctx.ResponseFormat is null) return RespValue.Error("ERR subscriptions need a response queue");
        var names = Strs(c);
        var n = _subs.Subscribe(ctx.ResponseFormat, names, pattern);
        var kind = pattern ? "psubscribe" : "subscribe";
        return RespValue.Arr(names.Select(x => RespValue.Arr(new[] { RespValue.Bulk(kind), RespValue.Bulk(x), RespValue.Int(n) })));
    }

    private RespValue Unsubscribe(Command c, RequestContext ctx, bool pattern)
    {
        if (ctx.ResponseFormat is null) return RespValue.Error("ERR subscriptions need a response queue");
        return RespValue.Int(_subs.Unsubscribe(ctx.ResponseFormat, Strs(c), pattern));
    }

    private RespValue PubSub(Command c)
    {
        if (c.Count < 1) return Arity(c);
        switch (c.Str(0).ToUpperInvariant())
        {
            case "CHANNELS": return RespValue.BulkArray(_subs.Channels(c.Count > 1 ? c.Str(1) : "*"));
            case "NUMSUB":
                return RespValue.Arr(Strs(c, 1).SelectMany(ch => new[] { RespValue.Bulk(ch), RespValue.Int(_subs.NumSub(ch)) }));
            default: return RespValue.Error("ERR PUBSUB CHANNELS [pattern] | NUMSUB [channel ...]");
        }
    }

    // ------------------------------------------------------------- queue admin
    private string[] DeadLetterPaths => new[] { @".\Deadletter$", @".\XactDeadletter$" };

    private RespValue QueueInfo(Command c)
    {
        if (c.Count != 1 || !c.Str(0).Equals("INFO", StringComparison.OrdinalIgnoreCase))
            return RespValue.Error("ERR QUEUE INFO");
        var sb = new StringBuilder();
        sb.Append("request_queue:").Append(_requestPath).Append("\r\n")
          .Append("request_queue_depth:").Append(QueueAdmin.Count(_requestPath)).Append("\r\n")
          .Append("poison_queue_depth:").Append(QueueAdmin.Count(_poisonPath)).Append("\r\n");
        if (_cfg.QueueJournal)
            sb.Append("journal_depth:").Append(QueueAdmin.Count(_journalPath)).Append("\r\n");
        sb.Append("deadletter_depth:")
          .Append(DeadLetterPaths.Sum(p => SafeCount(p, QueueAdmin.LabelPrefix))).Append("\r\n")
          .Append("subscriptions:").Append(_subs.TotalSubscriptions).Append("\r\n");
        return RespValue.Bulk(sb.ToString());
    }

    private static int SafeCount(string path, string? prefix)
    {
        try { return QueueAdmin.Count(path, 100_000, prefix); }
        catch (MessageQueueException) { return 0; }
    }

    private RespValue DeadLetter(Command c)
    {
        if (c.Count < 1) return Arity(c);
        switch (c.Str(0).ToUpperInvariant())
        {
            case "COUNT": return RespValue.Int(DeadLetterPaths.Sum(p => SafeCount(p, QueueAdmin.LabelPrefix)));
            case "PEEK":
                var n = c.Count > 1 && c.TryLong(1, out var v) ? (int)Math.Clamp(v, 1, 1000) : 10;
                return RespValue.BulkArray(DeadLetterPaths.SelectMany(p => PeekSafe(p, n)).Take(n));
            case "PURGE":
                return RespValue.Int(QueueAdmin.Purge(@".\Deadletter$", false, QueueAdmin.LabelPrefix) +
                                     QueueAdmin.Purge(@".\XactDeadletter$", true, QueueAdmin.LabelPrefix));
            default: return RespValue.Error("ERR DEADLETTER COUNT | PEEK [n] | PURGE (only mqcache.* messages are touched)");
        }
    }

    private static IEnumerable<string> PeekSafe(string path, int n)
    {
        try { return QueueAdmin.Peek(path, n, QueueAdmin.LabelPrefix); }
        catch (MessageQueueException) { return Array.Empty<string>(); }
    }

    private RespValue Journal(Command c)
    {
        if (!_cfg.QueueJournal) return RespValue.Error("ERR journaling is disabled (queue-journal no)");
        if (c.Count < 1) return Arity(c);
        return c.Str(0).ToUpperInvariant() switch
        {
            "COUNT" => RespValue.Int(QueueAdmin.Count(_journalPath)),
            "PURGE" => RespValue.Int(QueueAdmin.Purge(_journalPath, _cfg.QueueTransactional, QueueAdmin.LabelPrefix)),
            _ => RespValue.Error("ERR JOURNAL COUNT | PURGE"),
        };
    }

    private RespValue Poison(Command c)
    {
        if (c.Count < 1) return Arity(c);
        switch (c.Str(0).ToUpperInvariant())
        {
            case "COUNT": return RespValue.Int(QueueAdmin.Count(_poisonPath));
            case "PEEK":
                var n = c.Count > 1 && c.TryLong(1, out var v) ? (int)Math.Clamp(v, 1, 1000) : 10;
                return RespValue.BulkArray(QueueAdmin.Peek(_poisonPath, n, null));
            case "PURGE": return RespValue.Int(QueueAdmin.Purge(_poisonPath, _cfg.QueueTransactional, "mqcache."));
            case "REQUEUE":
                var max = c.Count > 1 && c.TryLong(1, out var m) ? (int)Math.Clamp(m, 1, 10_000) : 100;
                return RespValue.Int(Requeue(max));
            default: return RespValue.Error("ERR POISON COUNT | PEEK [n] | PURGE | REQUEUE [n]");
        }
    }

    /// <summary>Moves poison messages back to the request queue (atomically when the queues are transactional).</summary>
    private int Requeue(int max)
    {
        using var poison = QueueAdmin.Open(_poisonPath, QueueAccessMode.Receive);
        using var request = QueueAdmin.Open(_requestPath, QueueAccessMode.Send);
        var moved = 0;
        while (moved < max)
        {
            using var tx = _cfg.QueueTransactional ? new MessageQueueTransaction() : null;
            tx?.Begin();
            Message m;
            try { m = tx is null ? poison.Receive(TimeSpan.Zero) : poison.Receive(TimeSpan.Zero, tx); }
            catch (MessageQueueException ex) when (ex.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout) { tx?.Abort(); break; }
            using (m)
            {
                var copy = MessageFactory.Request(MessageFactory.ReadBody(m, MessageFactory.MaxBodyBytes),
                    new SendOptions { Priority = m.Priority, Ttl = TimeSpan.FromMinutes(5) }, null, null);
                if (tx is null) request.Send(copy, MessageQueueTransactionType.None); else request.Send(copy, tx);
            }
            tx?.Commit();
            moved++;
        }
        return moved;
    }

    private static string[] Strs(Command c, int from = 0) => c.Args.Skip(from).Select(a => Encoding.UTF8.GetString(a)).ToArray();

    private static RespValue Arity(Command c) =>
        RespValue.Error($"ERR wrong number of arguments for '{c.Name.ToLowerInvariant()}' command");
}
