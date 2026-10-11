// File: src/MQCache.Core/Storage/Services.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Background services: active expiry, snapshot scheduling, and the idempotency (dedupe) response cache.
// License: MIT

using MQCache.Core.Config;

namespace MQCache.Core.Storage;

/// <summary>Periodically removes expired keys.</summary>
public sealed class StoreMaintenance : IDisposable
{
    private readonly Timer _timer;
    public StoreMaintenance(KeyspaceStore store, int intervalMs, ILog log)
    {
        _timer = new Timer(_ =>
        {
            try { store.ActiveExpire(); }
            catch (Exception ex) { log.Error("active expire failed", ex); }
        }, null, intervalMs, intervalMs);
    }
    public void Dispose() => _timer.Dispose();
}

public interface ISnapshotService
{
    /// <summary>Synchronous save; returns the number of keys written.</summary>
    long Save();
    /// <summary>Starts a save on the thread pool; false if one is already running.</summary>
    bool SaveInBackground();
    long LastSaveUnixSeconds { get; }
}

/// <summary>Loads the snapshot at startup and saves according to "save N M" rules, on SAVE/BGSAVE and on shutdown.</summary>
public sealed class SnapshotService : ISnapshotService, IDisposable
{
    private readonly KeyspaceStore _store;
    private readonly ServerConfig _cfg;
    private readonly ILog _log;
    private readonly object _saveGate = new();
    private readonly Timer _timer;
    private long _lastSaveMs;
    private int _running;

    public SnapshotService(KeyspaceStore store, ServerConfig cfg, ILog log)
    {
        _store = store; _cfg = cfg; _log = log;
        _lastSaveMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _timer = new Timer(_ => Tick(), null, Timeout.Infinite, Timeout.Infinite);
    }

    public long LastSaveUnixSeconds => Interlocked.Read(ref _lastSaveMs) / 1000;

    /// <summary>Restores data from disk; a corrupt file is renamed to *.corrupt and the server starts empty.</summary>
    public void LoadIfPresent()
    {
        var path = _cfg.SnapshotPath;
        if (!File.Exists(path)) return;
        try
        {
            var entries = SnapshotFile.Load(path);
            _store.Restore(entries);
            _store.MarkSaved(long.MaxValue);
            _log.Info($"snapshot loaded: {entries.Count} keys from {path}");
        }
        catch (Exception ex) when (ex is SnapshotCorruptException or IOException or InvalidCastException or EndOfStreamException)
        {
            var bad = path + ".corrupt-" + DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture);
            try { File.Move(path, bad); } catch (IOException) { /* best effort */ }
            _log.Error($"snapshot unreadable ({ex.Message}); moved to {bad}; starting empty", ex);
        }
    }

    public void Start()
    {
        if (_cfg.Save.Count > 0) _timer.Change(1000, 1000);
    }

    public long Save()
    {
        lock (_saveGate)
        {
            var entries = _store.Capture(out var dirty);
            SnapshotFile.Save(_cfg.SnapshotPath, entries, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            _store.MarkSaved(dirty);
            Interlocked.Exchange(ref _lastSaveMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            _log.Info($"snapshot saved: {entries.Count} keys to {_cfg.SnapshotPath}");
            return entries.Count;
        }
    }

    public bool SaveInBackground()
    {
        if (Interlocked.CompareExchange(ref _running, 1, 0) != 0) return false;
        Task.Run(() =>
        {
            try { Save(); }
            catch (Exception ex) { _log.Error("background save failed", ex); }
            finally { Interlocked.Exchange(ref _running, 0); }
        });
        return true;
    }

    private void Tick()
    {
        try
        {
            var dirty = _store.Stats().Dirty;
            if (dirty <= 0) return;
            var elapsed = (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - Interlocked.Read(ref _lastSaveMs)) / 1000;
            foreach (var r in _cfg.Save)
            {
                if (elapsed >= r.Seconds && dirty >= r.Changes) { SaveInBackground(); return; }
            }
        }
        catch (Exception ex) { _log.Error("snapshot scheduler failed", ex); }
    }

    public void Dispose() => _timer.Dispose();
}

/// <summary>
/// Bounded time-windowed cache of encoded responses keyed by the client's request id. A retried or redelivered
/// request returns the original answer instead of re-executing (exactly-once effect inside the window).
/// </summary>
public sealed class ResponseCache
{
    private readonly object _gate = new();
    private readonly Dictionary<Guid, LinkedListNode<(Guid Id, byte[] Body, long At)>> _map = new();
    private readonly LinkedList<(Guid Id, byte[] Body, long At)> _order = new();
    private readonly TimeSpan _window;
    private readonly int _max;
    private readonly Func<long> _nowMs;

    public ResponseCache(TimeSpan window, int maxEntries, Func<long>? clockMs = null)
    {
        _window = window; _max = maxEntries;
        _nowMs = clockMs ?? (() => Environment.TickCount64);
    }

    public bool Enabled => _window > TimeSpan.Zero && _max > 0;

    public bool TryGet(Guid id, out byte[] body)
    {
        body = Array.Empty<byte>();
        if (!Enabled) return false;
        lock (_gate)
        {
            if (!_map.TryGetValue(id, out var node)) return false;
            if (_nowMs() - node.Value.At > (long)_window.TotalMilliseconds) { Remove(node); return false; }
            body = node.Value.Body;
            return true;
        }
    }

    public void Put(Guid id, byte[] body)
    {
        if (!Enabled) return;
        lock (_gate)
        {
            if (_map.TryGetValue(id, out var existing)) Remove(existing);
            _map[id] = _order.AddLast((id, body, _nowMs()));
            while (_map.Count > _max && _order.First is { } first) Remove(first);
            var cutoff = _nowMs() - (long)_window.TotalMilliseconds;
            while (_order.First is { } f && f.Value.At < cutoff) Remove(f);
        }
    }

    public int Count { get { lock (_gate) return _map.Count; } }

    private void Remove(LinkedListNode<(Guid Id, byte[] Body, long At)> node)
    {
        _map.Remove(node.Value.Id);
        _order.Remove(node);
    }
}
