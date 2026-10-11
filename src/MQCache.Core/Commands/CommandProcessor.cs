// File: src/MQCache.Core/Commands/CommandProcessor.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Redis-like command table (strings, hashes, lists, sets, TTL, admin) executing against KeyspaceStore.
// License: MIT

using System.Globalization;
using System.Text;
using MQCache.Core.Config;
using MQCache.Core.Protocol;
using MQCache.Core.Storage;

namespace MQCache.Core.Commands;

public sealed class CommandProcessor
{
    private delegate RespValue Handler(Command c);
    private sealed record Spec(int Min, int Max, bool Write, Handler Run);

    private readonly KeyspaceStore _store;
    private readonly ServerConfig _cfg;
    private readonly ISnapshotService? _snap;
    private readonly ILog _log;
    private readonly Dictionary<string, Spec> _table = new(StringComparer.Ordinal);
    private readonly DateTimeOffset _started = DateTimeOffset.UtcNow;
    private long _processed;

    /// <summary>Extra INFO text appended by the host (e.g. MSMQ queue statistics).</summary>
    public Func<string>? ExtraInfo { get; set; }

    public long CommandsProcessed => Interlocked.Read(ref _processed);

    public CommandProcessor(KeyspaceStore store, ServerConfig cfg, ISnapshotService? snapshots, ILog? log = null)
    {
        _store = store; _cfg = cfg; _snap = snapshots; _log = log ?? NullLog.Instance;
        Register();
    }

    public bool Knows(string upperName) => _table.ContainsKey(upperName);

    public RespValue Execute(Command c)
    {
        if (!_table.TryGetValue(c.Name, out var spec)) return Err($"ERR unknown command '{c.Name}'");
        if (c.Count < spec.Min || (spec.Max >= 0 && c.Count > spec.Max))
            return Err($"ERR wrong number of arguments for '{c.Name.ToLowerInvariant()}' command");
        if (spec.Write)
        {
            foreach (var a in c.Args)
                if (a.Length > _cfg.MaxValueBytes) return Err("ERR argument too large (max-value-bytes)");
            if (!_store.TryMakeRoom()) return Err("OOM command not allowed when used memory > 'maxmemory'.");
        }
        Interlocked.Increment(ref _processed);
        try { return spec.Run(c); }
        catch (WrongTypeException ex) { return Err(ex.Message); }
        catch (CacheValueException ex) { return Err(ex.Message); }
        catch (CacheKeyException ex) { return Err(ex.Message); }
        catch (OverflowException) { return Err("ERR value is out of range"); }
        catch (Exception ex)
        {
            _log.Error($"command {c.Name} failed", ex);
            return Err("ERR internal error: " + ex.GetType().Name);
        }
    }

    private static RespValue Err(string m) => RespValue.Error(m);
    private static string[] Strs(Command c, int from) => c.Args.Skip(from).Select(a => Encoding.UTF8.GetString(a)).ToArray();

    private void R(string name, int min, int max, bool write, Handler h) => _table[name] = new Spec(min, max, write, h);

    private void Register()
    {
        // ------------------------------------------------------------ connection / server
        R("PING", 0, 1, false, c => c.Count == 0 ? RespValue.Pong : RespValue.Bulk(c.Args[0]));
        R("ECHO", 1, 1, false, c => RespValue.Bulk(c.Args[0]));
        R("TIME", 0, 0, false, _ =>
        {
            var now = DateTimeOffset.UtcNow;
            var us = (now.ToUnixTimeMilliseconds() % 1000) * 1000;
            return RespValue.BulkArray(new[] { now.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture), us.ToString(CultureInfo.InvariantCulture) });
        });
        R("DBSIZE", 0, 0, false, _ => RespValue.Int(_store.DbSize()));
        R("FLUSHALL", 0, 0, false, _ => { _store.FlushAll(); return RespValue.Ok; });
        R("FLUSHDB", 0, 0, false, _ => { _store.FlushAll(); return RespValue.Ok; });
        R("INFO", 0, 1, false, _ => RespValue.Bulk(BuildInfo()));
        R("SAVE", 0, 0, false, _ => _snap is null ? Err("ERR persistence is not available") : SaveNow());
        R("BGSAVE", 0, 0, false, _ =>
            _snap is null ? Err("ERR persistence is not available")
            : _snap.SaveInBackground() ? RespValue.Status("Background saving started") : Err("ERR background save already in progress"));
        R("LASTSAVE", 0, 0, false, _ => RespValue.Int(_snap?.LastSaveUnixSeconds ?? 0));
        R("CONFIG", 1, 3, false, Config);

        // ------------------------------------------------------------ keys
        R("DEL", 1, -1, false, c => RespValue.Int(_store.Del(Strs(c, 0))));
        R("EXISTS", 1, -1, false, c => RespValue.Int(_store.Exists(Strs(c, 0))));
        R("TYPE", 1, 1, false, c => RespValue.Status(_store.Type(c.Str(0))));
        R("KEYS", 1, 1, false, c => RespValue.BulkArray(_store.Keys(c.Str(0), _cfg.KeysMaxResults)));
        R("RENAME", 2, 2, false, c => { _store.Rename(c.Str(0), c.Str(1)); return RespValue.Ok; });
        R("EXPIRE", 2, 2, false, c => c.TryLong(1, out var s) ? Bool(_store.Expire(c.Str(0), checked(s * 1000))) : NotInt());
        R("PEXPIRE", 2, 2, false, c => c.TryLong(1, out var ms) ? Bool(_store.Expire(c.Str(0), ms)) : NotInt());
        R("TTL", 1, 1, false, c =>
        {
            var t = _store.PTtl(c.Str(0));
            return RespValue.Int(t < 0 ? t : (t + 500) / 1000);
        });
        R("PTTL", 1, 1, false, c => RespValue.Int(_store.PTtl(c.Str(0))));
        R("PERSIST", 1, 1, false, c => Bool(_store.Persist(c.Str(0))));

        // ------------------------------------------------------------ strings
        R("GET", 1, 1, false, c => RespValue.Bulk(_store.Get(c.Str(0))));
        R("SET", 2, -1, true, Set);
        R("SETNX", 2, 2, true, c => Bool(_store.Set(c.Str(0), c.Args[1], new SetOptions(Nx: true))));
        R("SETEX", 3, 3, true, c =>
            c.TryLong(1, out var s) && s > 0 ? SetOk(c.Str(0), c.Args[2], checked(s * 1000)) : Err("ERR invalid expire time in 'setex' command"));
        R("PSETEX", 3, 3, true, c =>
            c.TryLong(1, out var ms) && ms > 0 ? SetOk(c.Str(0), c.Args[2], ms) : Err("ERR invalid expire time in 'psetex' command"));
        R("GETSET", 2, 2, true, c => RespValue.Bulk(_store.GetSet(c.Str(0), c.Args[1])));
        R("APPEND", 2, 2, true, c => RespValue.Int(_store.Append(c.Str(0), c.Args[1])));
        R("STRLEN", 1, 1, false, c => RespValue.Int(_store.StrLen(c.Str(0))));
        R("INCR", 1, 1, true, c => RespValue.Int(_store.IncrBy(c.Str(0), 1)));
        R("DECR", 1, 1, true, c => RespValue.Int(_store.IncrBy(c.Str(0), -1)));
        R("INCRBY", 2, 2, true, c => c.TryLong(1, out var d) ? RespValue.Int(_store.IncrBy(c.Str(0), d)) : NotInt());
        R("DECRBY", 2, 2, true, c => c.TryLong(1, out var d) ? RespValue.Int(_store.IncrBy(c.Str(0), checked(-d))) : NotInt());
        R("MGET", 1, -1, false, c => RespValue.BulkArray(_store.MGet(Strs(c, 0))));
        R("MSET", 2, -1, true, c =>
        {
            if (c.Count % 2 != 0) return Err("ERR wrong number of arguments for 'mset' command");
            var pairs = new List<(string, byte[])>();
            for (var i = 0; i < c.Count; i += 2) pairs.Add((c.Str(i), c.Args[i + 1]));
            _store.MSet(pairs);
            return RespValue.Ok;
        });

        // ------------------------------------------------------------ hashes
        R("HSET", 3, -1, true, c => HashSet(c, asStatus: false));
        R("HMSET", 3, -1, true, c => HashSet(c, asStatus: true));
        R("HGET", 2, 2, false, c => RespValue.Bulk(_store.HGet(c.Str(0), c.Str(1))));
        R("HMGET", 2, -1, false, c => RespValue.BulkArray(_store.HMGet(c.Str(0), Strs(c, 1))));
        R("HDEL", 2, -1, false, c => RespValue.Int(_store.HDel(c.Str(0), Strs(c, 1))));
        R("HGETALL", 1, 1, false, c =>
            RespValue.Arr(_store.HGetAll(c.Str(0)).SelectMany(kv => new[] { RespValue.Bulk(kv.Field), RespValue.Bulk(kv.Value) })));
        R("HKEYS", 1, 1, false, c => RespValue.BulkArray(_store.HGetAll(c.Str(0)).Select(kv => kv.Field)));
        R("HVALS", 1, 1, false, c => RespValue.BulkArray(_store.HGetAll(c.Str(0)).Select(kv => (byte[]?)kv.Value)));
        R("HLEN", 1, 1, false, c => RespValue.Int(_store.HLen(c.Str(0))));
        R("HEXISTS", 2, 2, false, c => Bool(_store.HExists(c.Str(0), c.Str(1))));
        R("HINCRBY", 3, 3, true, c => c.TryLong(2, out var d) ? RespValue.Int(_store.HIncrBy(c.Str(0), c.Str(1), d)) : NotInt());

        // ------------------------------------------------------------ lists
        R("LPUSH", 2, -1, true, c => RespValue.Int(_store.Push(c.Str(0), c.Args.Skip(1).ToArray(), left: true)));
        R("RPUSH", 2, -1, true, c => RespValue.Int(_store.Push(c.Str(0), c.Args.Skip(1).ToArray(), left: false)));
        R("LPOP", 1, 1, false, c => RespValue.Bulk(_store.Pop(c.Str(0), left: true)));
        R("RPOP", 1, 1, false, c => RespValue.Bulk(_store.Pop(c.Str(0), left: false)));
        R("LLEN", 1, 1, false, c => RespValue.Int(_store.LLen(c.Str(0))));
        R("LRANGE", 3, 3, false, c =>
            c.TryLong(1, out var a) && c.TryLong(2, out var b)
                ? RespValue.BulkArray(_store.LRange(c.Str(0), a, b).Select(x => (byte[]?)x))
                : NotInt());
        R("LINDEX", 2, 2, false, c => c.TryLong(1, out var i) ? RespValue.Bulk(_store.LIndex(c.Str(0), i)) : NotInt());

        // ------------------------------------------------------------ sets
        R("SADD", 2, -1, true, c => RespValue.Int(_store.SAdd(c.Str(0), Strs(c, 1))));
        R("SREM", 2, -1, false, c => RespValue.Int(_store.SRem(c.Str(0), Strs(c, 1))));
        R("SMEMBERS", 1, 1, false, c => RespValue.BulkArray(_store.SMembers(c.Str(0))));
        R("SISMEMBER", 2, 2, false, c => Bool(_store.SIsMember(c.Str(0), c.Str(1))));
        R("SCARD", 1, 1, false, c => RespValue.Int(_store.SCard(c.Str(0))));
    }

    private static RespValue Bool(bool b) => b ? RespValue.One : RespValue.Zero;
    private static RespValue NotInt() => Err("ERR value is not an integer or out of range");

    private RespValue SetOk(string key, byte[] value, long ttlMs)
    {
        _store.Set(key, value, new SetOptions(ExpireMs: ttlMs));
        return RespValue.Ok;
    }

    private RespValue Set(Command c)
    {
        long px = 0; bool nx = false, xx = false, keep = false, haveTtl = false;
        for (var i = 2; i < c.Count; i++)
        {
            switch (c.Str(i).ToUpperInvariant())
            {
                case "EX":
                    if (haveTtl || ++i >= c.Count || !c.TryLong(i, out var s) || s <= 0) return Err("ERR invalid expire time in 'set' command");
                    px = checked(s * 1000); haveTtl = true; break;
                case "PX":
                    if (haveTtl || ++i >= c.Count || !c.TryLong(i, out var ms) || ms <= 0) return Err("ERR invalid expire time in 'set' command");
                    px = ms; haveTtl = true; break;
                case "NX": nx = true; break;
                case "XX": xx = true; break;
                case "KEEPTTL": keep = true; break;
                default: return Err("ERR syntax error");
            }
        }
        if ((nx && xx) || (keep && haveTtl)) return Err("ERR syntax error");
        return _store.Set(c.Str(0), c.Args[1], new SetOptions(px, nx, xx, keep)) ? RespValue.Ok : RespValue.Nil;
    }

    private RespValue HashSet(Command c, bool asStatus)
    {
        if (c.Count % 2 == 0) return Err($"ERR wrong number of arguments for '{c.Name.ToLowerInvariant()}' command");
        var pairs = new List<(string, byte[])>();
        for (var i = 1; i < c.Count; i += 2) pairs.Add((c.Str(i), c.Args[i + 1]));
        var added = _store.HSet(c.Str(0), pairs);
        return asStatus ? RespValue.Ok : RespValue.Int(added);
    }

    private RespValue SaveNow()
    {
        try { _snap!.Save(); return RespValue.Ok; }
        catch (Exception ex) { _log.Error("SAVE failed", ex); return Err("ERR save failed: " + ex.Message); }
    }

    private RespValue Config(Command c)
    {
        switch (c.Str(0).ToUpperInvariant())
        {
            case "GET" when c.Count == 2:
                var pattern = c.Str(1);
                var items = new List<RespValue>();
                foreach (var (k, v) in _cfg.ToDictionary())
                    if (Glob.IsMatch(pattern, k)) { items.Add(RespValue.Bulk(k)); items.Add(RespValue.Bulk(v)); }
                return RespValue.Arr(items);
            case "SET" when c.Count == 3:
                var key = c.Str(1).ToLowerInvariant();
                if (key is not ("maxmemory" or "maxmemory-policy"))
                    return Err($"ERR CONFIG SET is not supported for '{key}'");
                try
                {
                    _cfg.Set(key, new[] { c.Str(2) });
                    _store.Options.MaxMemory = _cfg.MaxMemory;
                    _store.Options.Policy = _cfg.MaxMemoryPolicy;
                    return RespValue.Ok;
                }
                catch (ConfigException ex) { return Err("ERR " + ex.Message); }
            default: return Err("ERR syntax error (CONFIG GET <pattern> | CONFIG SET <maxmemory|maxmemory-policy> <value>)");
        }
    }

    private string BuildInfo()
    {
        var s = _store.Stats();
        var up = DateTimeOffset.UtcNow - _started;
        var sb = new StringBuilder();
        sb.Append("# Server\r\n")
          .Append("mqcache_version:").Append(typeof(CommandProcessor).Assembly.GetName().Version).Append("\r\n")
          .Append("uptime_in_seconds:").Append((long)up.TotalSeconds).Append("\r\n")
          .Append("process_id:").Append(Environment.ProcessId).Append("\r\n")
          .Append("# Stats\r\n")
          .Append("total_commands_processed:").Append(CommandsProcessed).Append("\r\n")
          .Append("keyspace_hits:").Append(s.Hits).Append("\r\n")
          .Append("keyspace_misses:").Append(s.Misses).Append("\r\n")
          .Append("expired_keys:").Append(s.Expired).Append("\r\n")
          .Append("evicted_keys:").Append(s.Evicted).Append("\r\n")
          .Append("# Memory\r\n")
          .Append("used_memory:").Append(s.UsedBytes).Append("\r\n")
          .Append("maxmemory:").Append(_cfg.MaxMemory).Append("\r\n")
          .Append("maxmemory_policy:").Append(_cfg.ToDictionary()["maxmemory-policy"]).Append("\r\n")
          .Append("# Persistence\r\n")
          .Append("changes_since_last_save:").Append(s.Dirty).Append("\r\n")
          .Append("rdb_last_save_time:").Append(_snap?.LastSaveUnixSeconds ?? 0).Append("\r\n")
          .Append("# Keyspace\r\n")
          .Append("db0:keys=").Append(s.Keys).Append(",expires=").Append(s.KeysWithExpiry).Append("\r\n");
        var extra = ExtraInfo?.Invoke();
        if (!string.IsNullOrEmpty(extra)) sb.Append(extra);
        return sb.ToString();
    }
}
