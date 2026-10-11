// File: src/MQCache.Messaging/Pools.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Small thread-safe helpers: object pool and a bounded format-name -> send-queue cache.
// License: MIT

using System.Collections.Concurrent;
using Experimental.System.Messaging;

namespace MQCache.Messaging;

/// <summary>MessageQueue instances are not thread-safe, so callers rent one per operation.</summary>
public sealed class QueuePool : IDisposable
{
    private readonly ConcurrentBag<MessageQueue> _bag = new();
    private readonly Func<MessageQueue> _factory;
    private volatile bool _disposed;

    public QueuePool(Func<MessageQueue> factory) => _factory = factory;

    public MessageQueue Rent()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _bag.TryTake(out var q) ? q : _factory();
    }

    public void Return(MessageQueue q)
    {
        if (_disposed) q.Dispose(); else _bag.Add(q);
    }

    public void Dispose()
    {
        _disposed = true;
        while (_bag.TryTake(out var q)) q.Dispose();
    }
}

/// <summary>Not thread-safe by design: one instance per worker thread.</summary>
public sealed class SendQueueCache : IDisposable
{
    private readonly Dictionary<string, (MessageQueue Queue, long Used)> _map = new(StringComparer.OrdinalIgnoreCase);
    private readonly int _capacity;
    private long _tick;

    public SendQueueCache(int capacity = 256) => _capacity = capacity;

    public MessageQueue Get(string formatName)
    {
        if (_map.TryGetValue(formatName, out var hit))
        {
            _map[formatName] = (hit.Queue, ++_tick);
            return hit.Queue;
        }
        if (_map.Count >= _capacity)
        {
            var oldest = _map.MinBy(kv => kv.Value.Used).Key;
            Invalidate(oldest);
        }
        var q = new MessageQueue("FormatName:" + formatName, false, true, QueueAccessMode.Send);
        _map[formatName] = (q, ++_tick);
        return q;
    }

    public void Invalidate(string formatName)
    {
        if (_map.Remove(formatName, out var e)) e.Queue.Dispose();
    }

    public void Dispose()
    {
        foreach (var e in _map.Values) e.Queue.Dispose();
        _map.Clear();
    }
}
