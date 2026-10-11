// File: src/MQCache.Client/CacheSubscriber.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Pub/sub subscriber: dedicated MSMQ queue, lease renewal, optional multicast.
// License: MIT

using System.Collections.Concurrent;
using Experimental.System.Messaging;
using MQCache.Core.Protocol;
using MQCache.Messaging;

namespace MQCache.Client;

/// <summary>
/// Receives published messages on its own queue. The server holds a lease per subscription which this object
/// renews periodically; Dispose unsubscribes and deletes the queue. Delivery is at-most-once (express messages).
/// </summary>
public sealed class CacheSubscriber : IDisposable
{
    private readonly CacheClient _client;
    private readonly string _name, _path, _format;
    private readonly HashSet<string> _channels, _patterns;
    private readonly BlockingCollection<PubMessage> _inbox = new(boundedCapacity: 100_000);
    private readonly CancellationTokenSource _cts = new();
    private readonly Thread _thread;
    private readonly Timer? _renew;
    private readonly object _gate = new();
    private bool _disposed;

    public event Action<PubMessage>? MessageReceived;

    internal CacheSubscriber(CacheClient client, IEnumerable<string> channels, IEnumerable<string> patterns, string? multicastAddress)
    {
        _client = client;
        _channels = new HashSet<string>(channels, StringComparer.Ordinal);
        _patterns = new HashSet<string>(patterns, StringComparer.Ordinal);
        _name = ReplyQueues.NewName(ReplyQueues.SubPrefix);
        _path = ReplyQueues.LocalPath(_name);
        _format = ReplyQueues.Format(client.OptionsInternal.ReplyScheme, client.OptionsInternal.ReplyHost, _name);

        using (var q = MessageQueue.Create(_path, false))
        {
            q.Label = "mqcache subscriber";
            if (multicastAddress is not null) q.MulticastAddress = multicastAddress;
        }

        _thread = new Thread(ReceiveLoop) { IsBackground = true, Name = "mqcache-subscriber" };
        _thread.Start();

        if (multicastAddress is null)
        {
            SendSubscriptions();
            var interval = client.OptionsInternal.SubscriptionRenewInterval;
            _renew = new Timer(_ => SendSubscriptions(), null, interval, interval);
        }
    }

    public void Subscribe(params string[] channels)
    {
        lock (_gate) foreach (var c in channels) _channels.Add(c);
        _client.SendNoReply(Command.Of(new[] { "SUBSCRIBE" }.Concat(channels).ToArray()), _format);
    }

    public void PSubscribe(params string[] patterns)
    {
        lock (_gate) foreach (var p in patterns) _patterns.Add(p);
        _client.SendNoReply(Command.Of(new[] { "PSUBSCRIBE" }.Concat(patterns).ToArray()), _format);
    }

    public void Unsubscribe(params string[] channels)
    {
        lock (_gate) foreach (var c in channels) _channels.Remove(c);
        _client.SendNoReply(Command.Of(new[] { "UNSUBSCRIBE" }.Concat(channels).ToArray()), _format);
    }

    /// <summary>Blocks up to <paramref name="timeout"/> for the next message.</summary>
    public bool TryRead(TimeSpan timeout, out PubMessage? message)
    {
        var ok = _inbox.TryTake(out var m, timeout);
        message = m;
        return ok;
    }

    /// <summary>Blocking enumeration, ends when the token is cancelled or the subscriber is disposed.</summary>
    public IEnumerable<PubMessage> ReadAll(CancellationToken ct = default)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);
        IEnumerable<PubMessage> e;
        try { e = _inbox.GetConsumingEnumerable(linked.Token); }
        catch (OperationCanceledException) { yield break; }
        using var it = e.GetEnumerator();
        while (true)
        {
            try { if (!it.MoveNext()) yield break; }
            catch (OperationCanceledException) { yield break; }
            yield return it.Current;
        }
    }

    private void SendSubscriptions()
    {
        string[] ch, pat;
        lock (_gate) { ch = _channels.ToArray(); pat = _patterns.ToArray(); }
        try
        {
            if (ch.Length > 0) _client.SendNoReply(Command.Of(new[] { "SUBSCRIBE" }.Concat(ch).ToArray()), _format);
            if (pat.Length > 0) _client.SendNoReply(Command.Of(new[] { "PSUBSCRIBE" }.Concat(pat).ToArray()), _format);
        }
        catch (CacheException) { /* retried on the next renewal tick */ }
    }

    private void ReceiveLoop()
    {
        try
        {
            using var q = QueueAdmin.Open(_path, QueueAccessMode.Receive);
            while (!_cts.IsCancellationRequested)
            {
                Message m;
                try { m = q.Receive(TimeSpan.FromSeconds(1)); }
                catch (MessageQueueException ex) when (ex.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout) { continue; }
                using (m)
                {
                    if (m.Label != MessageFactory.PublishLabel) continue;
                    PubMessage pm;
                    try { pm = PubSubCodec.Decode(MessageFactory.ReadBody(m, MessageFactory.MaxBodyBytes)); }
                    catch (Exception ex) when (ex is WireFormatException or InvalidDataException) { continue; }
                    if (!_inbox.TryAdd(pm)) { _inbox.TryTake(out _); _inbox.TryAdd(pm); } // drop oldest when full
                    MessageReceived?.Invoke(pm);
                }
            }
        }
        catch (Exception) when (_cts.IsCancellationRequested || _disposed) { }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _renew?.Dispose();
        try { _client.SendNoReply(Command.Of("UNSUBSCRIBE"), _format); }
        catch (CacheException) { }
        try { _client.SendNoReply(Command.Of("PUNSUBSCRIBE"), _format); }
        catch (CacheException) { }
        _cts.Cancel();
        _thread.Join(TimeSpan.FromSeconds(3));
        _inbox.CompleteAdding();
        ReplyQueues.DeleteQuietly(_name);
        _cts.Dispose();
    }
}
