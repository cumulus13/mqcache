// File: src/MQCache.Core/Storage/KeyspaceStore.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Thread-safe in-memory keyspace: strings, hashes, lists, sets, TTL, exact LRU, memory accounting, eviction.
// License: MIT

using System.Globalization;
using System.Text;
using MQCache.Core.Config;

namespace MQCache.Core.Storage;

public enum ValueKind : byte { String = 1, Hash = 2, List = 3, Set = 4 }

public sealed class WrongTypeException : Exception
{
    public WrongTypeException() : base("WRONGTYPE Operation against a key holding the wrong kind of value") { }
}

public sealed class CacheValueException : Exception
{
    public CacheValueException(string message = "ERR value is not an integer or out of range") : base(message) { }
}

public sealed class CacheKeyException : Exception
{
    public CacheKeyException(string message) : base(message) { }
}

public readonly record struct SetOptions(long ExpireMs = 0, bool Nx = false, bool Xx = false, bool KeepTtl = false);

public sealed record StoreStats(long Keys, long KeysWithExpiry, long UsedBytes, long Hits, long Misses,
    long Expired, long Evicted, long Dirty);

public sealed record SnapEntry(string Key, ValueKind Kind, long ExpireAtMs, object Payload);

public sealed class StoreOptions
{
    public long MaxMemory { get; set; }
    public EvictionPolicy Policy { get; set; } = EvictionPolicy.NoEviction;
}

/// <summary>
/// Single-lock keyspace (like Redis' single thread): every public method is atomic.
/// Byte[] values are treated as immutable; they are replaced, never mutated in place.
/// </summary>
public sealed class KeyspaceStore
{
    private sealed class Entry
    {
        public ValueKind Kind;
        public object Value = null!;
        public long ExpireAt;
        public long Size;
        public LinkedListNode<string> Node = null!;
    }

    private sealed class ExpiryComparer : IComparer<(long At, string Key)>
    {
        public int Compare((long At, string Key) x, (long At, string Key) y)
        {
            var c = x.At.CompareTo(y.At);
            return c != 0 ? c : string.CompareOrdinal(x.Key, y.Key);
        }
    }

    private readonly object _gate = new();
    private readonly Dictionary<string, Entry> _map = new(StringComparer.Ordinal);
    private readonly LinkedList<string> _lru = new();
    private readonly SortedSet<(long At, string Key)> _expiry = new(new ExpiryComparer());
    private readonly Func<long> _now;
    private long _used, _hits, _misses, _expired, _evicted, _dirty;

    public KeyspaceStore(StoreOptions options, Func<long>? clockMs = null)
    {
        Options = options;
        _now = clockMs ?? (() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
    }

    public StoreOptions Options { get; }

    /// <summary>Runs <paramref name="action"/> while holding the store lock (MULTI/EXEC-style isolation).</summary>
    public T Atomically<T>(Func<T> action) { lock (_gate) return action(); }

    // ------------------------------------------------------------ internals
    private static long KeySize(string key) => 64 + key.Length * 2L;

    private Entry? Live(string key)
    {
        if (!_map.TryGetValue(key, out var e)) return null;
        if (e.ExpireAt > 0 && _now() >= e.ExpireAt)
        {
            Drop(key, e); _expired++; _dirty++;
            return null;
        }
        _lru.Remove(e.Node); _lru.AddFirst(e.Node);
        return e;
    }

    private Entry? LiveRead(string key)
    {
        var e = Live(key);
        if (e is null) _misses++; else _hits++;
        return e;
    }

    private Entry Add(string key, ValueKind kind, object value, long payloadSize)
    {
        var e = new Entry { Kind = kind, Value = value, Size = KeySize(key) + payloadSize };
        e.Node = _lru.AddFirst(key);
        _map[key] = e;
        _used += e.Size;
        return e;
    }

    private void Grow(Entry e, long delta) { e.Size += delta; _used += delta; }

    private void Drop(string key, Entry e)
    {
        _map.Remove(key);
        _lru.Remove(e.Node);
        if (e.ExpireAt > 0) _expiry.Remove((e.ExpireAt, key));
        _used -= e.Size;
    }

    private void SetExpiryAt(string key, Entry e, long at)
    {
        if (e.ExpireAt > 0) _expiry.Remove((e.ExpireAt, key));
        e.ExpireAt = at;
        if (at > 0) _expiry.Add((at, key));
    }

    private static T? Expect<T>(Entry? e, ValueKind kind) where T : class
    {
        if (e is null) return null;
        if (e.Kind != kind) throw new WrongTypeException();
        return (T)e.Value;
    }

    private static long ParseLong(byte[] b)
    {
        if (!long.TryParse(Encoding.ASCII.GetString(b), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var n))
            throw new CacheValueException();
        return n;
    }

    private static byte[] Ascii(long n) => Encoding.ASCII.GetBytes(n.ToString(CultureInfo.InvariantCulture));

    // -------------------------------------------------------------- strings
    public byte[]? Get(string key)
    {
        lock (_gate) return Expect<byte[]>(LiveRead(key), ValueKind.String);
    }

    public bool Set(string key, byte[] value, SetOptions o = default)
    {
        lock (_gate)
        {
            var old = Live(key);
            if (o.Nx && old is not null) return false;
            if (o.Xx && old is null) return false;
            var keepAt = o.KeepTtl && old is not null ? old.ExpireAt : 0;
            if (old is not null) Drop(key, old);
            var e = Add(key, ValueKind.String, value, value.Length);
            var at = o.ExpireMs > 0 ? _now() + o.ExpireMs : keepAt;
            if (at > 0) SetExpiryAt(key, e, at);
            _dirty++;
            return true;
        }
    }

    public byte[]? GetSet(string key, byte[] value)
    {
        lock (_gate)
        {
            var old = Live(key);
            var prev = Expect<byte[]>(old, ValueKind.String);
            if (old is not null) Drop(key, old);
            Add(key, ValueKind.String, value, value.Length);
            _dirty++;
            return prev;
        }
    }

    public long Append(string key, byte[] suffix)
    {
        lock (_gate)
        {
            var e = Live(key);
            var cur = Expect<byte[]>(e, ValueKind.String);
            if (cur is null)
            {
                Add(key, ValueKind.String, suffix, suffix.Length);
                _dirty++;
                return suffix.Length;
            }
            var next = new byte[cur.Length + suffix.Length];
            Buffer.BlockCopy(cur, 0, next, 0, cur.Length);
            Buffer.BlockCopy(suffix, 0, next, cur.Length, suffix.Length);
            Grow(e!, suffix.Length);
            e!.Value = next;
            _dirty++;
            return next.Length;
        }
    }

    public long StrLen(string key)
    {
        lock (_gate) return Expect<byte[]>(LiveRead(key), ValueKind.String)?.Length ?? 0;
    }

    public long IncrBy(string key, long delta)
    {
        lock (_gate)
        {
            var e = Live(key);
            var cur = Expect<byte[]>(e, ValueKind.String);
            long next;
            try { next = checked((cur is null ? 0 : ParseLong(cur)) + delta); }
            catch (OverflowException) { throw new CacheValueException("ERR increment or decrement would overflow"); }
            var nb = Ascii(next);
            if (e is null) Add(key, ValueKind.String, nb, nb.Length);
            else { Grow(e, nb.Length - cur!.Length); e.Value = nb; }
            _dirty++;
            return next;
        }
    }

    public byte[]?[] MGet(IReadOnlyList<string> keys)
    {
        lock (_gate)
        {
            var r = new byte[]?[keys.Count];
            for (var i = 0; i < keys.Count; i++)
            {
                var e = LiveRead(keys[i]);
                r[i] = e is { Kind: ValueKind.String } ? (byte[])e.Value : null;
            }
            return r;
        }
    }

    public void MSet(IReadOnlyList<(string Key, byte[] Value)> pairs)
    {
        lock (_gate) foreach (var (k, v) in pairs) Set(k, v);
    }

    // -------------------------------------------------------------- generic
    public long Del(IEnumerable<string> keys)
    {
        lock (_gate)
        {
            long n = 0;
            foreach (var k in keys)
            {
                var e = Live(k);
                if (e is null) continue;
                Drop(k, e); n++; _dirty++;
            }
            return n;
        }
    }

    public long Exists(IEnumerable<string> keys)
    {
        lock (_gate) return keys.Count(k => Live(k) is not null);
    }

    public string Type(string key)
    {
        lock (_gate)
            return Live(key)?.Kind switch
            {
                ValueKind.String => "string",
                ValueKind.Hash => "hash",
                ValueKind.List => "list",
                ValueKind.Set => "set",
                _ => "none",
            };
    }

    /// <summary>Sets a relative TTL. A non-positive TTL deletes the key (Redis semantics).</summary>
    public bool Expire(string key, long ttlMs)
    {
        lock (_gate)
        {
            var e = Live(key);
            if (e is null) return false;
            if (ttlMs <= 0) { Drop(key, e); _dirty++; return true; }
            SetExpiryAt(key, e, _now() + ttlMs);
            _dirty++;
            return true;
        }
    }

    /// <summary>Remaining TTL in ms; -2 when the key is missing, -1 when it has no TTL.</summary>
    public long PTtl(string key)
    {
        lock (_gate)
        {
            var e = Live(key);
            if (e is null) return -2;
            return e.ExpireAt == 0 ? -1 : Math.Max(0, e.ExpireAt - _now());
        }
    }

    public bool Persist(string key)
    {
        lock (_gate)
        {
            var e = Live(key);
            if (e is null || e.ExpireAt == 0) return false;
            SetExpiryAt(key, e, 0);
            _dirty++;
            return true;
        }
    }

    public void Rename(string src, string dst)
    {
        lock (_gate)
        {
            var e = Live(src) ?? throw new CacheKeyException("ERR no such key");
            if (src == dst) return;
            var at = e.ExpireAt; var value = e.Value; var kind = e.Kind; var payload = e.Size - KeySize(src);
            Drop(src, e);
            var old = Live(dst);
            if (old is not null) Drop(dst, old);
            var ne = Add(dst, kind, value, payload);
            if (at > 0) SetExpiryAt(dst, ne, at);
            _dirty++;
        }
    }

    public string[] Keys(string pattern, int max)
    {
        lock (_gate)
        {
            var now = _now();
            var result = new List<string>();
            var dead = new List<string>();
            foreach (var (k, e) in _map)
            {
                if (e.ExpireAt > 0 && now >= e.ExpireAt) { dead.Add(k); continue; }
                if (result.Count < max && Glob.IsMatch(pattern, k)) result.Add(k);
            }
            foreach (var k in dead) { var e = _map[k]; Drop(k, e); _expired++; _dirty++; }
            return result.ToArray();
        }
    }

    public long DbSize() { lock (_gate) return _map.Count; }

    public void FlushAll()
    {
        lock (_gate)
        {
            _dirty += _map.Count;
            _map.Clear(); _lru.Clear(); _expiry.Clear(); _used = 0;
        }
    }

    // --------------------------------------------------------------- hashes
    private static long FieldSize(string f, byte[] v) => 32 + f.Length * 2L + v.Length;

    public long HSet(string key, IReadOnlyList<(string Field, byte[] Value)> pairs)
    {
        lock (_gate)
        {
            var e = Live(key);
            var h = Expect<Dictionary<string, byte[]>>(e, ValueKind.Hash);
            if (h is null)
            {
                h = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                e = Add(key, ValueKind.Hash, h, 0);
            }
            long added = 0;
            foreach (var (f, v) in pairs)
            {
                if (h.TryGetValue(f, out var old)) Grow(e!, v.Length - old.Length);
                else { added++; Grow(e!, FieldSize(f, v)); }
                h[f] = v;
            }
            _dirty++;
            return added;
        }
    }

    public byte[]? HGet(string key, string field)
    {
        lock (_gate) return Expect<Dictionary<string, byte[]>>(LiveRead(key), ValueKind.Hash)?.GetValueOrDefault(field);
    }

    public byte[]?[] HMGet(string key, IReadOnlyList<string> fields)
    {
        lock (_gate)
        {
            var h = Expect<Dictionary<string, byte[]>>(LiveRead(key), ValueKind.Hash);
            return fields.Select(f => h?.GetValueOrDefault(f)).ToArray();
        }
    }

    public long HDel(string key, IEnumerable<string> fields)
    {
        lock (_gate)
        {
            var e = Live(key);
            var h = Expect<Dictionary<string, byte[]>>(e, ValueKind.Hash);
            if (h is null) return 0;
            long n = 0;
            foreach (var f in fields)
            {
                if (!h.Remove(f, out var old)) continue;
                Grow(e!, -FieldSize(f, old)); n++;
            }
            if (n > 0) { _dirty++; if (h.Count == 0) Drop(key, e!); }
            return n;
        }
    }

    public (string Field, byte[] Value)[] HGetAll(string key)
    {
        lock (_gate)
            return Expect<Dictionary<string, byte[]>>(LiveRead(key), ValueKind.Hash)?.Select(kv => (kv.Key, kv.Value)).ToArray()
                   ?? Array.Empty<(string, byte[])>();
    }

    public long HLen(string key)
    {
        lock (_gate) return Expect<Dictionary<string, byte[]>>(LiveRead(key), ValueKind.Hash)?.Count ?? 0;
    }

    public bool HExists(string key, string field)
    {
        lock (_gate) return Expect<Dictionary<string, byte[]>>(LiveRead(key), ValueKind.Hash)?.ContainsKey(field) ?? false;
    }

    public long HIncrBy(string key, string field, long delta)
    {
        lock (_gate)
        {
            var e = Live(key);
            var h = Expect<Dictionary<string, byte[]>>(e, ValueKind.Hash);
            if (h is null)
            {
                h = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                e = Add(key, ValueKind.Hash, h, 0);
            }
            var hasOld = h.TryGetValue(field, out var old);
            long next;
            try { next = checked((hasOld ? ParseLong(old!) : 0) + delta); }
            catch (OverflowException) { throw new CacheValueException("ERR increment or decrement would overflow"); }
            var nb = Ascii(next);
            Grow(e!, hasOld ? nb.Length - old!.Length : FieldSize(field, nb));
            h[field] = nb;
            _dirty++;
            return next;
        }
    }

    // ---------------------------------------------------------------- lists
    public long Push(string key, IReadOnlyList<byte[]> values, bool left)
    {
        lock (_gate)
        {
            var e = Live(key);
            var l = Expect<LinkedList<byte[]>>(e, ValueKind.List);
            if (l is null)
            {
                l = new LinkedList<byte[]>();
                e = Add(key, ValueKind.List, l, 0);
            }
            foreach (var v in values)
            {
                if (left) l.AddFirst(v); else l.AddLast(v);
                Grow(e!, 24 + v.Length);
            }
            _dirty++;
            return l.Count;
        }
    }

    public byte[]? Pop(string key, bool left)
    {
        lock (_gate)
        {
            var e = Live(key);
            var l = Expect<LinkedList<byte[]>>(e, ValueKind.List);
            if (l is null || l.Count == 0) return null;
            var node = left ? l.First! : l.Last!;
            l.Remove(node);
            Grow(e!, -(24 + node.Value.Length));
            _dirty++;
            if (l.Count == 0) Drop(key, e!);
            return node.Value;
        }
    }

    public long LLen(string key)
    {
        lock (_gate) return Expect<LinkedList<byte[]>>(LiveRead(key), ValueKind.List)?.Count ?? 0;
    }

    public byte[][] LRange(string key, long start, long stop)
    {
        lock (_gate)
        {
            var l = Expect<LinkedList<byte[]>>(LiveRead(key), ValueKind.List);
            if (l is null) return Array.Empty<byte[]>();
            long len = l.Count;
            if (start < 0) start += len;
            if (stop < 0) stop += len;
            if (start < 0) start = 0;
            if (stop >= len) stop = len - 1;
            if (start > stop || start >= len) return Array.Empty<byte[]>();
            return l.Skip((int)start).Take((int)(stop - start + 1)).ToArray();
        }
    }

    public byte[]? LIndex(string key, long index)
    {
        lock (_gate)
        {
            var l = Expect<LinkedList<byte[]>>(LiveRead(key), ValueKind.List);
            if (l is null) return null;
            if (index < 0) index += l.Count;
            return index < 0 || index >= l.Count ? null : l.ElementAt((int)index);
        }
    }

    // ----------------------------------------------------------------- sets
    private static long MemberSize(string m) => 24 + m.Length * 2L;

    public long SAdd(string key, IEnumerable<string> members)
    {
        lock (_gate)
        {
            var e = Live(key);
            var s = Expect<HashSet<string>>(e, ValueKind.Set);
            if (s is null)
            {
                s = new HashSet<string>(StringComparer.Ordinal);
                e = Add(key, ValueKind.Set, s, 0);
            }
            long n = 0;
            foreach (var m in members)
                if (s.Add(m)) { n++; Grow(e!, MemberSize(m)); }
            _dirty++;
            if (s.Count == 0) Drop(key, e!);
            return n;
        }
    }

    public long SRem(string key, IEnumerable<string> members)
    {
        lock (_gate)
        {
            var e = Live(key);
            var s = Expect<HashSet<string>>(e, ValueKind.Set);
            if (s is null) return 0;
            long n = 0;
            foreach (var m in members)
                if (s.Remove(m)) { n++; Grow(e!, -MemberSize(m)); }
            if (n > 0) { _dirty++; if (s.Count == 0) Drop(key, e!); }
            return n;
        }
    }

    public string[] SMembers(string key)
    {
        lock (_gate) return Expect<HashSet<string>>(LiveRead(key), ValueKind.Set)?.ToArray() ?? Array.Empty<string>();
    }

    public bool SIsMember(string key, string member)
    {
        lock (_gate) return Expect<HashSet<string>>(LiveRead(key), ValueKind.Set)?.Contains(member) ?? false;
    }

    public long SCard(string key)
    {
        lock (_gate) return Expect<HashSet<string>>(LiveRead(key), ValueKind.Set)?.Count ?? 0;
    }

    // ---------------------------------------------------------- maintenance
    /// <summary>Removes up to <paramref name="max"/> expired keys. Called periodically by <see cref="StoreMaintenance"/>.</summary>
    public int ActiveExpire(int max = 500)
    {
        lock (_gate)
        {
            var n = 0;
            var now = _now();
            while (n < max && _expiry.Count > 0)
            {
                var min = _expiry.Min;
                if (min.At > now) break;
                _expiry.Remove(min);
                if (_map.TryGetValue(min.Key, out var e) && e.ExpireAt == min.At)
                {
                    Drop(min.Key, e); _expired++; _dirty++; n++;
                }
            }
            return n;
        }
    }

    /// <summary>
    /// Frees memory according to the eviction policy until usage is within <c>maxmemory</c>.
    /// Returns false if the limit is still exceeded (noeviction, or no eligible key).
    /// </summary>
    public bool TryMakeRoom()
    {
        lock (_gate)
        {
            var max = Options.MaxMemory;
            if (max <= 0) return true;
            while (_used > max)
            {
                if (!EvictOne()) return false;
            }
            return true;
        }
    }

    private bool EvictOne()
    {
        string? victim = null;
        switch (Options.Policy)
        {
            case EvictionPolicy.AllKeysLru:
                victim = _lru.Last?.Value;
                break;
            case EvictionPolicy.VolatileLru:
                for (var n = _lru.Last; n is not null; n = n.Previous)
                    if (_map[n.Value].ExpireAt > 0) { victim = n.Value; break; }
                break;
            case EvictionPolicy.VolatileTtl:
                if (_expiry.Count > 0) victim = _expiry.Min.Key;
                break;
        }
        if (victim is null) return false;
        Drop(victim, _map[victim]);
        _evicted++; _dirty++;
        return true;
    }

    public StoreStats Stats()
    {
        lock (_gate) return new StoreStats(_map.Count, _expiry.Count, _used, _hits, _misses, _expired, _evicted, _dirty);
    }

    // ------------------------------------------------------------ snapshots
    public IReadOnlyList<SnapEntry> Capture(out long dirtyAtCapture)
    {
        lock (_gate)
        {
            dirtyAtCapture = _dirty;
            var now = _now();
            var list = new List<SnapEntry>(_map.Count);
            foreach (var (k, e) in _map)
            {
                if (e.ExpireAt > 0 && now >= e.ExpireAt) continue;
                object payload = e.Kind switch
                {
                    ValueKind.String => e.Value,
                    ValueKind.Hash => ((Dictionary<string, byte[]>)e.Value).Select(kv => (kv.Key, kv.Value)).ToArray(),
                    ValueKind.List => ((LinkedList<byte[]>)e.Value).ToArray(),
                    _ => ((HashSet<string>)e.Value).ToArray(),
                };
                list.Add(new SnapEntry(k, e.Kind, e.ExpireAt, payload));
            }
            return list;
        }
    }

    public void MarkSaved(long dirtyAtCapture)
    {
        lock (_gate) _dirty = Math.Max(0, _dirty - dirtyAtCapture);
    }

    public void Restore(IEnumerable<SnapEntry> entries)
    {
        lock (_gate)
        {
            var now = _now();
            foreach (var s in entries)
            {
                if (s.ExpireAtMs > 0 && now >= s.ExpireAtMs) continue;
                if (_map.TryGetValue(s.Key, out var old)) Drop(s.Key, old);
                Entry e;
                switch (s.Kind)
                {
                    case ValueKind.String:
                        var b = (byte[])s.Payload;
                        e = Add(s.Key, ValueKind.String, b, b.Length);
                        break;
                    case ValueKind.Hash:
                        var h = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                        long hs = 0;
                        foreach (var (f, v) in ((string Field, byte[] Value)[])s.Payload) { h[f] = v; hs += FieldSize(f, v); }
                        e = Add(s.Key, ValueKind.Hash, h, hs);
                        break;
                    case ValueKind.List:
                        var l = new LinkedList<byte[]>();
                        long ls = 0;
                        foreach (var v in (byte[][])s.Payload) { l.AddLast(v); ls += 24 + v.Length; }
                        e = Add(s.Key, ValueKind.List, l, ls);
                        break;
                    default:
                        var set = new HashSet<string>((string[])s.Payload, StringComparer.Ordinal);
                        e = Add(s.Key, ValueKind.Set, set, set.Sum(MemberSize));
                        break;
                }
                if (s.ExpireAtMs > 0) SetExpiryAt(s.Key, e, s.ExpireAtMs);
            }
        }
    }
}
