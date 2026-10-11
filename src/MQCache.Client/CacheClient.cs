// File: src/MQCache.Client/CacheClient.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Thread-safe client: request/reply over MSMQ using correlation-id selective receive, retries, pipelines, acks.
// License: MIT

using System.Globalization;
using System.Text;
using Experimental.System.Messaging;
using MQCache.Core.Protocol;
using MQCache.Messaging;

namespace MQCache.Client;

public sealed class CacheClient : IDisposable
{
    private readonly CacheClientOptions _o;
    private readonly MsmqEndpoint _server;
    private readonly string _replyName, _replyFormat;
    private readonly QueuePool _sendPool, _recvPool;
    private volatile bool _transactional;
    private bool _disposed;

    public CacheClient(CacheClientOptions options)
    {
        _o = options;
        QueueAdmin.EnsureMsmqAvailable();
        _server = MsmqEndpoint.Parse(options.Endpoint, options.QueueName);
        if (options.SweepOrphanQueues) ReplyQueues.Sweep();

        _replyName = ReplyQueues.NewName(ReplyQueues.ReplyPrefix);
        var path = ReplyQueues.LocalPath(_replyName);
        using (var q = MessageQueue.Create(path, false)) q.Label = "mqcache reply";
        _replyFormat = ReplyQueues.Format(options.ReplyScheme, options.ReplyHost, _replyName);

        _transactional = options.Transactional;
        _sendPool = new QueuePool(() => QueueAdmin.Open(_server.QueueSpec, QueueAccessMode.Send));
        _recvPool = new QueuePool(() => QueueAdmin.Open(path, QueueAccessMode.Receive));
    }

    public static CacheClient Connect(string connectionStringOrEndpoint)
    {
        var o = connectionStringOrEndpoint.Contains('=')
            ? CacheClientOptions.FromConnectionString(connectionStringOrEndpoint)
            : new CacheClientOptions { Endpoint = connectionStringOrEndpoint };
        return new CacheClient(o);
    }

    public CacheClientOptions Options => _o;

    // ------------------------------------------------------------------ core API
    public RespValue Execute(string name, params string[] args) =>
        Execute(Command.Of(new[] { name }.Concat(args).ToArray()));

    public RespValue Execute(Command command, CallOptions? call = null) =>
        Roundtrip(new[] { command }, atomic: false, call)[0];

    /// <summary>Sends several commands in ONE MSMQ message. With <paramref name="atomic"/> the server runs them without interleaving.</summary>
    public IReadOnlyList<RespValue> Pipeline(IEnumerable<Command> commands, bool atomic = true, CallOptions? call = null)
    {
        var list = commands.ToArray();
        if (list.Length == 0) return Array.Empty<RespValue>();
        return Roundtrip(list, atomic, call);
    }

    /// <summary>
    /// Fire-and-forget. With <paramref name="waitForReceiveAck"/> the call blocks until MSMQ confirms (acknowledgment message)
    /// that the server consumed the request, and throws if it expired or was rejected.
    /// </summary>
    public void Send(Command command, bool waitForReceiveAck = false, CallOptions? call = null)
    {
        var req = new RequestEnvelope(Guid.NewGuid(), _o.Password, false, true, new[] { command });
        var so = BuildOptions(call);
        if (waitForReceiveAck) so.Acknowledge = AcknowledgeTypes.FullReceive;
        var id = SendRaw(WireCodec.EncodeRequest(req), so, null, waitForReceiveAck ? _replyFormat : null);
        if (!waitForReceiveAck) return;

        var q = _recvPool.Rent();
        try
        {
            using var ack = q.ReceiveByCorrelationId(id, call?.Timeout ?? _o.Timeout);
            if (ack.Acknowledgment != Acknowledgment.Receive)
                throw new CacheException("request was not consumed by the server: " + ack.Acknowledgment);
        }
        catch (MessageQueueException ex) when (ex.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
        {
            throw new CacheTimeoutException("no acknowledgment from server in time", ex);
        }
        finally { _recvPool.Return(q); }
    }

    private IReadOnlyList<RespValue> Roundtrip(IReadOnlyList<Command> commands, bool atomic, CallOptions? call)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var requestId = Guid.NewGuid();   // same id on every retry => server returns the cached answer instead of re-executing
        var body = WireCodec.EncodeRequest(new RequestEnvelope(requestId, _o.Password, atomic, false, commands));
        var so = BuildOptions(call);
        var timeout = call?.Timeout ?? _o.Timeout;
        var retries = call?.Retries ?? _o.Retries;
        Exception? last = null;

        for (var attempt = 0; attempt <= retries; attempt++)
        {
            var id = SendRaw(body, so, _replyFormat, null);
            var q = _recvPool.Rent();
            try
            {
                using var reply = q.ReceiveByCorrelationId(id, timeout);
                var env = WireCodec.DecodeResponse(MessageFactory.ReadBody(reply, MessageFactory.MaxBodyBytes));
                if (env.Results.Count == commands.Count) return env.Results;
                if (env.Results.Count == 1 && env.Results[0].IsError) throw new CacheServerException(env.Results[0].Text ?? "error");
                throw new CacheException($"protocol error: expected {commands.Count} results, got {env.Results.Count}");
            }
            catch (MessageQueueException ex) when (ex.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
            {
                last = ex;
            }
            finally { _recvPool.Return(q); }
        }
        throw new CacheTimeoutException($"no reply from '{_server.QueueSpec}' after {retries + 1} attempt(s) of {timeout.TotalMilliseconds:0} ms", last);
    }

    internal string SendRaw(byte[] body, SendOptions so, string? responseFormat, string? adminFormat)
    {
        var q = _sendPool.Rent();
        try
        {
            using var m = MessageFactory.Request(body, so, responseFormat, adminFormat);
            try { q.Send(m, _transactional ? MessageQueueTransactionType.Single : MessageQueueTransactionType.None); }
            catch (MessageQueueException ex) when (ex.MessageQueueErrorCode == MessageQueueErrorCode.TransactionUsage)
            {
                // Server queue is of the other kind: remember and resend once.
                _transactional = !_transactional;
                q.Send(m, _transactional ? MessageQueueTransactionType.Single : MessageQueueTransactionType.None);
            }
            return m.Id;
        }
        catch (MessageQueueException ex)
        {
            throw new CacheException($"cannot send to '{_server.QueueSpec}': {ex.MessageQueueErrorCode}: {ex.Message}", ex);
        }
        finally { _sendPool.Return(q); }
    }

    internal void SendNoReply(Command command, string responseFormat)
    {
        var req = new RequestEnvelope(Guid.NewGuid(), _o.Password, false, true, new[] { command });
        var so = BuildOptions(null);
        so.Ttl = TimeSpan.FromSeconds(30);
        SendRaw(WireCodec.EncodeRequest(req), so, responseFormat, null);
    }

    internal string HostForSubscribers => _o.ReplyHost ?? Environment.MachineName;
    internal CacheClientOptions OptionsInternal => _o;

    private SendOptions BuildOptions(CallOptions? call) => new()
    {
        Priority = call?.Priority ?? _o.Priority,
        Ttl = (call?.Timeout ?? _o.Timeout) * ((call?.Retries ?? _o.Retries) + 1),
        Recoverable = call?.Recoverable ?? _o.Recoverable,
        UseJournal = _o.UseJournal,
        UseDeadLetter = _o.UseDeadLetter,
        UseTracing = _o.UseTracing,
    };

    // ------------------------------------------------------------- typed helpers
    private static RespValue Ok(RespValue v) => v.IsError ? throw new CacheServerException(v.Text ?? "error") : v;
    private RespValue Call(params string[] parts) => Ok(Execute(parts[0], parts.Skip(1).ToArray()));

    /// <summary>Round-trip latency (also verifies connectivity and credentials).</summary>
    public TimeSpan Ping()
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        Call("PING");
        return sw.Elapsed;
    }

    public string? GetString(string key) => Call("GET", key).AsString();
    public byte[]? GetBytes(string key) => Call("GET", key).Bytes;

    public bool Set(string key, string value, TimeSpan? ttl = null, SetWhen when = SetWhen.Always) =>
        Set(key, Encoding.UTF8.GetBytes(value), ttl, when);

    public bool Set(string key, byte[] value, TimeSpan? ttl = null, SetWhen when = SetWhen.Always)
    {
        var args = new List<byte[]> { Encoding.UTF8.GetBytes(key), value };
        if (ttl is { } t) { args.Add("PX"u8.ToArray()); args.Add(Encoding.ASCII.GetBytes(((long)t.TotalMilliseconds).ToString(CultureInfo.InvariantCulture))); }
        if (when == SetWhen.NotExists) args.Add("NX"u8.ToArray());
        if (when == SetWhen.Exists) args.Add("XX"u8.ToArray());
        return !Ok(Execute(new Command("SET", args.ToArray()))).IsNil;
    }

    public long Delete(params string[] keys) => Call(new[] { "DEL" }.Concat(keys).ToArray()).Integer;
    public bool Exists(string key) => Call("EXISTS", key).Integer > 0;
    public long Increment(string key, long by = 1) => Call("INCRBY", key, by.ToString(CultureInfo.InvariantCulture)).Integer;
    public long Decrement(string key, long by = 1) => Increment(key, -by);
    public bool Expire(string key, TimeSpan ttl) => Call("PEXPIRE", key, ((long)ttl.TotalMilliseconds).ToString(CultureInfo.InvariantCulture)).Integer == 1;

    public TimeSpan? GetTtl(string key)
    {
        var ms = Call("PTTL", key).Integer;
        return ms < 0 ? null : TimeSpan.FromMilliseconds(ms);
    }

    public IReadOnlyList<string> Keys(string pattern = "*") =>
        Call("KEYS", pattern).Items!.Select(i => i.AsString()!).ToList();

    public bool HashSet(string key, string field, string value) => Call("HSET", key, field, value).Integer == 1;
    public string? HashGet(string key, string field) => Call("HGET", key, field).AsString();

    public IReadOnlyDictionary<string, string> HashGetAll(string key)
    {
        var items = Call("HGETALL", key).Items!;
        var d = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i + 1 < items.Count; i += 2) d[items[i].AsString()!] = items[i + 1].AsString()!;
        return d;
    }

    public long ListPush(string key, string value, bool left = false) => Call(left ? "LPUSH" : "RPUSH", key, value).Integer;
    public string? ListPop(string key, bool left = true) => Call(left ? "LPOP" : "RPOP", key).AsString();
    public IReadOnlyList<string> ListRange(string key, long start = 0, long stop = -1) =>
        Call("LRANGE", key, start.ToString(CultureInfo.InvariantCulture), stop.ToString(CultureInfo.InvariantCulture)).Items!.Select(i => i.AsString()!).ToList();

    public long SetAdd(string key, params string[] members) => Call(new[] { "SADD", key }.Concat(members).ToArray()).Integer;
    public IReadOnlyList<string> SetMembers(string key) => Call("SMEMBERS", key).Items!.Select(i => i.AsString()!).ToList();

    /// <summary>Publishes to all current subscribers; returns the number of subscriber queues that received it.</summary>
    public long Publish(string channel, string message) => Call("PUBLISH", channel, message).Integer;

    public string Info() => Call("INFO").AsString()!;
    public void Save() => Call("SAVE");

    public CacheSubscriber Subscribe(params string[] channels) => new CacheSubscriber(this, channels, Array.Empty<string>(), null);
    public CacheSubscriber PSubscribe(params string[] patterns) => new CacheSubscriber(this, Array.Empty<string>(), patterns, null);

    /// <summary>Joins an MSMQ multicast group (no server registration); the server must have a matching multicast-channel.</summary>
    public CacheSubscriber SubscribeMulticast(string address) => new CacheSubscriber(this, Array.Empty<string>(), Array.Empty<string>(), address);

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _sendPool.Dispose();
        _recvPool.Dispose();
        ReplyQueues.DeleteQuietly(_replyName);
    }
}
