// File: src/MQCache.Server/SubscriptionRegistry.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Lease-based pub/sub registry mapping channels/patterns to subscriber queue format names.
// License: MIT

using MQCache.Core.Storage;

namespace MQCache.Server;

/// <summary>
/// Subscribers are MSMQ queues. A subscription is a lease that the client must renew (re-SUBSCRIBE);
/// leases that expire, or whose queue repeatedly fails delivery, are dropped.
/// </summary>
public sealed class SubscriptionRegistry
{
    private const int MaxFailures = 3;

    private readonly object _gate = new();
    private readonly Dictionary<string, Dictionary<string, long>> _channels = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Dictionary<string, long>> _patterns = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _failures = new(StringComparer.OrdinalIgnoreCase);
    private readonly Func<long> _now;
    private readonly long _ttlMs;

    public SubscriptionRegistry(TimeSpan leaseTtl, Func<long>? clockMs = null)
    {
        _ttlMs = (long)leaseTtl.TotalMilliseconds;
        _now = clockMs ?? (() => Environment.TickCount64);
    }

    public int Subscribe(string format, IEnumerable<string> names, bool pattern)
    {
        lock (_gate)
        {
            var map = pattern ? _patterns : _channels;
            foreach (var n in names)
            {
                if (!map.TryGetValue(n, out var subs)) map[n] = subs = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
                subs[format] = _now() + _ttlMs;
            }
            _failures.Remove(format);
            return CountFor(format);
        }
    }

    /// <summary>Removes the given names, or every subscription of this kind when <paramref name="names"/> is empty.</summary>
    public int Unsubscribe(string format, IReadOnlyList<string> names, bool pattern)
    {
        lock (_gate)
        {
            var map = pattern ? _patterns : _channels;
            var targets = names.Count == 0 ? map.Keys.ToList() : names.ToList();
            foreach (var n in targets)
            {
                if (!map.TryGetValue(n, out var subs)) continue;
                subs.Remove(format);
                if (subs.Count == 0) map.Remove(n);
            }
            return CountFor(format);
        }
    }

    public List<(string Format, string? Pattern)> Targets(string channel)
    {
        lock (_gate)
        {
            var now = _now();
            var result = new List<(string, string?)>();
            if (_channels.TryGetValue(channel, out var direct))
                result.AddRange(direct.Where(kv => kv.Value > now).Select(kv => (kv.Key, (string?)null)));
            foreach (var (pat, subs) in _patterns)
            {
                if (!Glob.IsMatch(pat, channel)) continue;
                result.AddRange(subs.Where(kv => kv.Value > now).Select(kv => (kv.Key, (string?)pat)));
            }
            return result;
        }
    }

    public void ReportSuccess(string format)
    {
        lock (_gate) _failures.Remove(format);
    }

    public void ReportFailure(string format)
    {
        lock (_gate)
        {
            _failures[format] = _failures.GetValueOrDefault(format) + 1;
            if (_failures[format] < MaxFailures) return;
            _failures.Remove(format);
            RemoveFormat(_channels, format);
            RemoveFormat(_patterns, format);
        }
    }

    public int Prune()
    {
        lock (_gate)
        {
            var now = _now();
            return Prune(_channels, now) + Prune(_patterns, now);
        }
    }

    public int NumSub(string channel)
    {
        lock (_gate)
        {
            var now = _now();
            return _channels.TryGetValue(channel, out var s) ? s.Count(kv => kv.Value > now) : 0;
        }
    }

    public string[] Channels(string pattern)
    {
        lock (_gate) return _channels.Keys.Where(k => Glob.IsMatch(pattern, k)).ToArray();
    }

    public int TotalSubscriptions
    {
        get { lock (_gate) return _channels.Values.Sum(s => s.Count) + _patterns.Values.Sum(s => s.Count); }
    }

    private int CountFor(string format) =>
        _channels.Values.Count(s => s.ContainsKey(format)) + _patterns.Values.Count(s => s.ContainsKey(format));

    private static void RemoveFormat(Dictionary<string, Dictionary<string, long>> map, string format)
    {
        foreach (var k in map.Keys.ToList())
        {
            map[k].Remove(format);
            if (map[k].Count == 0) map.Remove(k);
        }
    }

    private static int Prune(Dictionary<string, Dictionary<string, long>> map, long now)
    {
        var removed = 0;
        foreach (var k in map.Keys.ToList())
        {
            foreach (var f in map[k].Where(kv => kv.Value <= now).Select(kv => kv.Key).ToList()) { map[k].Remove(f); removed++; }
            if (map[k].Count == 0) map.Remove(k);
        }
        return removed;
    }
}
