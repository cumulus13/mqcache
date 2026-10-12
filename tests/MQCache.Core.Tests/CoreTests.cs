// File: tests/MQCache.Core.Tests/CoreTests.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Unit tests for the platform-independent core (store, glob, codec, config, snapshot, commands, dedupe).
// License: MIT

using System.Text;
using MQCache.Core;
using MQCache.Core.Commands;
using MQCache.Core.Config;
using MQCache.Core.Protocol;
using MQCache.Core.Storage;
using Xunit;

namespace MQCache.Core.Tests;

public class GlobTests
{
    [Theory]
    [InlineData("*", "anything", true)]
    [InlineData("user:*", "user:42", true)]
    [InlineData("user:*", "usr:42", false)]
    [InlineData("h?llo", "hello", true)]
    [InlineData("h?llo", "heello", false)]
    [InlineData("h[ae]llo", "hallo", true)]
    [InlineData("h[^e]llo", "hello", false)]
    [InlineData("h[a-b]llo", "hbllo", true)]
    [InlineData(@"a\*b", "a*b", true)]
    [InlineData(@"a\*b", "axb", false)]
    [InlineData("*a*b*", "xxaxxbxx", true)]
    public void Matches(string pattern, string text, bool expected) => Assert.Equal(expected, Glob.IsMatch(pattern, text));
}

public class StoreTests
{
    private static (KeyspaceStore Store, long[] Clock) Make(StoreOptions? o = null)
    {
        var clock = new long[] { 1_000_000 };
        return (new KeyspaceStore(o ?? new StoreOptions(), () => clock[0]), clock);
    }

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);
    private static string S(byte[]? b) => b is null ? "<nil>" : Encoding.UTF8.GetString(b);

    [Fact]
    public void SetGetDelete()
    {
        var (s, _) = Make();
        Assert.True(s.Set("k", B("v")));
        Assert.Equal("v", S(s.Get("k")));
        Assert.Equal(1, s.Del(new[] { "k", "missing" }));
        Assert.Null(s.Get("k"));
    }

    [Fact]
    public void NxXxSemantics()
    {
        var (s, _) = Make();
        Assert.False(s.Set("k", B("1"), new SetOptions(Xx: true)));
        Assert.True(s.Set("k", B("1"), new SetOptions(Nx: true)));
        Assert.False(s.Set("k", B("2"), new SetOptions(Nx: true)));
        Assert.True(s.Set("k", B("3"), new SetOptions(Xx: true)));
        Assert.Equal("3", S(s.Get("k")));
    }

    [Fact]
    public void TtlExpiresLazilyAndActively()
    {
        var (s, clock) = Make();
        s.Set("a", B("1"), new SetOptions(ExpireMs: 1000));
        s.Set("b", B("1"), new SetOptions(ExpireMs: 1000));
        Assert.Equal(1000, s.PTtl("a"));
        clock[0] += 999;
        Assert.NotNull(s.Get("a"));
        clock[0] += 1;
        Assert.Null(s.Get("a"));            // lazy
        Assert.Equal(-2, s.PTtl("a"));
        Assert.Equal(1, s.ActiveExpire());  // active (b)
        Assert.Equal(0, s.DbSize());
    }

    [Fact]
    public void KeepTtlAndPersist()
    {
        var (s, _) = Make();
        s.Set("k", B("1"), new SetOptions(ExpireMs: 5000));
        s.Set("k", B("2"), new SetOptions(KeepTtl: true));
        Assert.Equal(5000, s.PTtl("k"));
        Assert.True(s.Persist("k"));
        Assert.Equal(-1, s.PTtl("k"));
        s.Set("k", B("3"), new SetOptions(ExpireMs: 5000));
        s.Set("k", B("4"));
        Assert.Equal(-1, s.PTtl("k"));
    }

    [Fact]
    public void IncrDecrAndErrors()
    {
        var (s, _) = Make();
        Assert.Equal(1, s.IncrBy("n", 1));
        Assert.Equal(11, s.IncrBy("n", 10));
        Assert.Equal(-9, s.IncrBy("n", -20));
        s.Set("t", B("abc"));
        Assert.Throws<CacheValueException>(() => s.IncrBy("t", 1));
        s.Set("max", B(long.MaxValue.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        Assert.Throws<CacheValueException>(() => s.IncrBy("max", 1));
    }

    [Fact]
    public void WrongTypeIsRejected()
    {
        var (s, _) = Make();
        s.Set("k", B("v"));
        Assert.Throws<WrongTypeException>(() => s.HGet("k", "f"));
        Assert.Throws<WrongTypeException>(() => s.Push("k", new[] { B("x") }, true));
        s.SAdd("set", new[] { "a" });
        Assert.Throws<WrongTypeException>(() => s.Get("set"));
    }

    [Fact]
    public void HashOperations()
    {
        var (s, _) = Make();
        Assert.Equal(2, s.HSet("h", new[] { ("a", B("1")), ("b", B("2")) }));
        Assert.Equal(0, s.HSet("h", new[] { ("a", B("9")) }));
        Assert.Equal("9", S(s.HGet("h", "a")));
        Assert.Equal(2, s.HLen("h"));
        Assert.Equal(12, s.HIncrBy("h", "b", 10));
        Assert.Equal(1, s.HDel("h", new[] { "a", "zz" }));
        Assert.Equal(1, s.HDel("h", new[] { "b" }));
        Assert.Equal("none", s.Type("h")); // emptied hash disappears
    }

    [Fact]
    public void ListOperations()
    {
        var (s, _) = Make();
        Assert.Equal(3, s.Push("l", new[] { B("a"), B("b"), B("c") }, left: false));
        Assert.Equal(4, s.Push("l", new[] { B("z") }, left: true));
        Assert.Equal(new[] { "z", "a", "b", "c" }, s.LRange("l", 0, -1).Select(S).ToArray());
        Assert.Equal(new[] { "a", "b" }, s.LRange("l", 1, 2).Select(S).ToArray());
        Assert.Equal("c", S(s.LIndex("l", -1)));
        Assert.Equal("z", S(s.Pop("l", left: true)));
        Assert.Equal("c", S(s.Pop("l", left: false)));
        s.Pop("l", true); s.Pop("l", true);
        Assert.Null(s.Pop("l", true));
        Assert.Equal("none", s.Type("l"));
    }

    [Fact]
    public void SetOperations()
    {
        var (s, _) = Make();
        Assert.Equal(2, s.SAdd("s", new[] { "a", "b", "a" }));
        Assert.True(s.SIsMember("s", "a"));
        Assert.Equal(2, s.SCard("s"));
        Assert.Equal(1, s.SRem("s", new[] { "a" }));
        Assert.Equal(new[] { "b" }, s.SMembers("s"));
    }

    [Fact]
    public void RenameKeepsTtlAndOverwrites()
    {
        var (s, _) = Make();
        s.Set("a", B("1"), new SetOptions(ExpireMs: 7000));
        s.Set("b", B("old"));
        s.Rename("a", "b");
        Assert.Equal("1", S(s.Get("b")));
        Assert.Equal(7000, s.PTtl("b"));
        Assert.Null(s.Get("a"));
        Assert.Throws<CacheKeyException>(() => s.Rename("nope", "x"));
    }

    [Fact]
    public void KeysPattern()
    {
        var (s, _) = Make();
        s.Set("user:1", B("x")); s.Set("user:2", B("x")); s.Set("order:1", B("x"));
        Assert.Equal(new[] { "user:1", "user:2" }, s.Keys("user:*", 100).OrderBy(x => x).ToArray());
        Assert.Single(s.Keys("*", 1));
    }

    [Fact]
    public void LruEvictionRemovesLeastRecentlyUsed()
    {
        var (s, _) = Make(new StoreOptions { MaxMemory = 400, Policy = EvictionPolicy.AllKeysLru });
        s.Set("a", new byte[50]); s.Set("b", new byte[50]); s.Set("c", new byte[50]);
        s.Get("a");                       // a is now most recent; b is least recent
        s.Set("d", new byte[100]);
        Assert.True(s.TryMakeRoom());
        Assert.Null(s.Get("b"));
        Assert.NotNull(s.Get("a"));
        Assert.True(s.Stats().Evicted >= 1);
        Assert.True(s.Stats().UsedBytes <= 400);
    }

    [Fact]
    public void NoEvictionReportsFullAndVolatilePolicyOnlyTouchesTtlKeys()
    {
        var (s, _) = Make(new StoreOptions { MaxMemory = 100, Policy = EvictionPolicy.NoEviction });
        s.Set("a", new byte[200]);
        Assert.False(s.TryMakeRoom());

        var (v, _) = Make(new StoreOptions { MaxMemory = 400, Policy = EvictionPolicy.VolatileLru });
        v.Set("persistent", new byte[100]);
        v.Set("temp", new byte[100], new SetOptions(ExpireMs: 60_000));
        v.Set("big", new byte[100]);
        Assert.True(v.TryMakeRoom());
        Assert.NotNull(v.Get("persistent"));
        Assert.Null(v.Get("temp"));
    }

    [Fact]
    public void MemoryAccountingReturnsToZero()
    {
        var (s, _) = Make();
        s.Set("a", B("hello")); s.HSet("h", new[] { ("f", B("v")) }); s.Push("l", new[] { B("x") }, true); s.SAdd("s", new[] { "m" });
        s.Append("a", B("world")); s.IncrBy("n", 5);
        Assert.True(s.Stats().UsedBytes > 0);
        s.FlushAll();
        Assert.Equal(0, s.Stats().UsedBytes);
        s.Set("a", B("x"));
        s.Del(new[] { "a" });
        Assert.Equal(0, s.Stats().UsedBytes);
    }
}

public class CodecTests
{
    [Fact]
    public void RequestRoundTrip()
    {
        var id = Guid.NewGuid();
        var req = new RequestEnvelope(id, "secret", true, false, new[]
        {
            Command.Of("SET", "k", "v"),
            Command.Of("GET", "k"),
            Command.Of("SET", "bin", new byte[] { 0, 255, 1 }),
        });
        var back = WireCodec.DecodeRequest(WireCodec.EncodeRequest(req));
        Assert.Equal(id, back.RequestId);
        Assert.Equal("secret", back.Auth);
        Assert.True(back.Atomic);
        Assert.False(back.NoReply);
        Assert.Equal(3, back.Commands.Count);
        Assert.Equal("SET", back.Commands[0].Name);
        Assert.Equal(new byte[] { 0, 255, 1 }, back.Commands[2].Args[1]);
    }

    [Fact]
    public void ResponseRoundTripAllTypes()
    {
        var id = Guid.NewGuid();
        var values = new[]
        {
            RespValue.Ok, RespValue.Error("ERR x"), RespValue.Int(-42), RespValue.Bulk("héllo"), RespValue.Nil,
            RespValue.Arr(new[] { RespValue.Int(1), RespValue.Arr(new[] { RespValue.Bulk("n") }) }),
        };
        var back = WireCodec.DecodeResponse(WireCodec.EncodeResponse(new ResponseEnvelope(id, values)));
        Assert.Equal(id, back.RequestId);
        Assert.Equal("OK", back.Results[0].Text);
        Assert.True(back.Results[1].IsError);
        Assert.Equal(-42, back.Results[2].Integer);
        Assert.Equal("héllo", back.Results[3].AsString());
        Assert.True(back.Results[4].IsNil);
        Assert.Equal(2, back.Results[5].Items!.Count);
    }

    [Fact]
    public void MalformedInputIsRejectedNotCrashing()
    {
        var good = WireCodec.EncodeRequest(new RequestEnvelope(Guid.NewGuid(), null, false, false, new[] { Command.Of("PING") }));
        for (var cut = 0; cut < good.Length; cut++)
            Assert.Throws<WireFormatException>(() => WireCodec.DecodeRequest(good[..cut]));
        var rnd = new Random(1);
        for (var i = 0; i < 500; i++)
        {
            var junk = new byte[rnd.Next(0, 64)];
            rnd.NextBytes(junk);
            try { WireCodec.DecodeRequest(junk); } catch (WireFormatException) { }
            try { WireCodec.DecodeResponse(junk); } catch (WireFormatException) { }
        }
        var hugeLen = (byte[])good.Clone();
        BitConverter.GetBytes(int.MaxValue).CopyTo(hugeLen, 18); // auth length field
        Assert.Throws<WireFormatException>(() => WireCodec.DecodeRequest(hugeLen));
    }
}

public class ConfigTests
{
    [Fact]
    public void ParsesDirectivesQuotesAndComments()
    {
        var cfg = ConfigLoader.FromText("""
            # comment
            queue private$\mycache     # trailing comment
            workers 8
            maxmemory 64mb
            maxmemory-policy allkeys-lru
            requirepass "p a#ss"
            queue-transactional no
            save 10 5
            save 60 1
            multicast-channel news 234.1.1.1:8001
            """);
        Assert.Equal(@"private$\mycache", cfg.Queue);
        Assert.Equal(8, cfg.Workers);
        Assert.Equal(64L << 20, cfg.MaxMemory);
        Assert.Equal(EvictionPolicy.AllKeysLru, cfg.MaxMemoryPolicy);
        Assert.Equal("p a#ss", cfg.RequirePass);
        Assert.False(cfg.QueueTransactional);
        Assert.Equal(2, cfg.Save.Count);
        Assert.Single(cfg.MulticastChannels);
    }

    [Theory]
    [InlineData("bogus 1")]
    [InlineData("workers 0")]
    [InlineData("workers abc")]
    [InlineData("maxmemory-policy random")]
    [InlineData("queue-transactional maybe")]
    [InlineData("requirepass \"unterminated")]
    [InlineData("multicast-channel c notanaddress")]
    [InlineData("max-value-bytes 100mb")]
    public void RejectsBadInput(string text) => Assert.Throws<ConfigException>(() => ConfigLoader.FromText(text));

    [Fact]
    public void ErrorsCarryLineNumbers()
    {
        var ex = Assert.Throws<ConfigException>(() => ConfigLoader.FromText("workers 2\nnope 1\n", "x.conf"));
        Assert.Contains("x.conf:2", ex.Message);
    }

    [Fact]
    public void OverridesApply()
    {
        var cfg = new ServerConfig();
        ConfigLoader.ApplyOverrides(cfg, new[] { "--workers", "3", "--queue", @"private$\q2" });
        Assert.Equal(3, cfg.Workers);
        Assert.Equal(@"private$\q2", cfg.Queue);
    }

    [Fact]
    public void ConfigGetHidesPassword()
    {
        var cfg = new ServerConfig { RequirePass = "x" };
        Assert.Equal("(hidden)", cfg.ToDictionary()["requirepass"]);
    }
}

public class SnapshotTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "mqcache-test-" + Guid.NewGuid().ToString("N"));
    public SnapshotTests() => Directory.CreateDirectory(_dir);
    public void Dispose()
    {
        try { Directory.Delete(_dir, true); } catch (IOException) { }
        GC.SuppressFinalize(this);
    }

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    [Fact]
    public void RoundTripAllKindsAndTtl()
    {
        var clock = 5_000_000L;
        var a = new KeyspaceStore(new StoreOptions(), () => clock);
        a.Set("s", B("v"), new SetOptions(ExpireMs: 10_000));
        a.HSet("h", new[] { ("f", B("1")) });
        a.Push("l", new[] { B("x"), B("y") }, false);
        a.SAdd("set", new[] { "m1", "m2" });
        a.Set("gone", B("x"), new SetOptions(ExpireMs: 1));
        clock += 5;
        var path = Path.Combine(_dir, "d.mqdb");
        SnapshotFile.Save(path, a.Capture(out _), clock);

        var b = new KeyspaceStore(new StoreOptions(), () => clock);
        b.Restore(SnapshotFile.Load(path));
        Assert.Equal(4, b.DbSize());
        Assert.Equal("v", Encoding.UTF8.GetString(b.Get("s")!));
        Assert.Equal(9995, b.PTtl("s"));
        Assert.Equal("1", Encoding.UTF8.GetString(b.HGet("h", "f")!));
        Assert.Equal(2, b.LLen("l"));
        Assert.Equal(2, b.SCard("set"));
        a.ActiveExpire();
        Assert.Equal(a.Stats().UsedBytes, b.Stats().UsedBytes);
    }

    [Fact]
    public void CorruptionIsDetected()
    {
        var s = new KeyspaceStore(new StoreOptions());
        s.Set("k", B("value"));
        var path = Path.Combine(_dir, "d.mqdb");
        SnapshotFile.Save(path, s.Capture(out _), 0);
        var bytes = File.ReadAllBytes(path);
        bytes[bytes.Length / 2] ^= 0xFF;
        File.WriteAllBytes(path, bytes);
        Assert.Throws<SnapshotCorruptException>(() => SnapshotFile.Load(path));
        File.WriteAllBytes(path, new byte[] { 1, 2, 3 });
        Assert.Throws<SnapshotCorruptException>(() => SnapshotFile.Load(path));
    }

    [Fact]
    public void ServiceSavesAndReloads()
    {
        var cfg = new ServerConfig { Dir = _dir };
        var s1 = new KeyspaceStore(new StoreOptions());
        s1.Set("k", B("v"));
        using (var svc = new SnapshotService(s1, cfg, NullLog.Instance)) Assert.Equal(1, svc.Save());
        Assert.Equal(0, s1.Stats().Dirty);

        var s2 = new KeyspaceStore(new StoreOptions());
        using var svc2 = new SnapshotService(s2, cfg, NullLog.Instance);
        svc2.LoadIfPresent();
        Assert.Equal("v", Encoding.UTF8.GetString(s2.Get("k")!));
        Assert.Equal(0, s2.Stats().Dirty);
    }

    [Fact]
    public void CorruptFileIsQuarantinedAndServerStartsEmpty()
    {
        var cfg = new ServerConfig { Dir = _dir };
        File.WriteAllBytes(cfg.SnapshotPath, new byte[100]);
        var s = new KeyspaceStore(new StoreOptions());
        using var svc = new SnapshotService(s, cfg, NullLog.Instance);
        svc.LoadIfPresent();
        Assert.Equal(0, s.DbSize());
        Assert.False(File.Exists(cfg.SnapshotPath));
        Assert.Single(Directory.GetFiles(_dir, "*.corrupt-*"));
    }
}

public class CommandTests
{
    private static CommandProcessor Make(ServerConfig? cfg = null)
    {
        cfg ??= new ServerConfig();
        return new CommandProcessor(new KeyspaceStore(new StoreOptions { MaxMemory = cfg.MaxMemory, Policy = cfg.MaxMemoryPolicy }), cfg, null);
    }

    private static RespValue X(CommandProcessor p, params string[] parts) => p.Execute(Command.Of(parts));

    [Fact]
    public void BasicFlow()
    {
        var p = Make();
        Assert.Equal("PONG", X(p, "ping").Text);
        Assert.Equal("hi", X(p, "PING", "hi").AsString());
        Assert.Equal("OK", X(p, "set", "k", "v").Text);
        Assert.Equal("v", X(p, "GET", "k").AsString());
        Assert.True(X(p, "GET", "missing").IsNil);
        Assert.Equal(1, X(p, "EXISTS", "k").Integer);
        Assert.Equal("string", X(p, "TYPE", "k").Text);
        Assert.Equal(1, X(p, "DEL", "k").Integer);
        Assert.Equal(0, X(p, "DBSIZE").Integer);
    }

    [Fact]
    public void SetOptionsAndTtl()
    {
        var p = Make();
        Assert.Equal("OK", X(p, "SET", "k", "v", "EX", "100").Text);
        Assert.InRange(X(p, "TTL", "k").Integer, 99, 100);
        Assert.True(X(p, "SET", "k", "v2", "NX").IsNil);
        Assert.True(X(p, "SET", "k", "v", "NX", "XX").IsError);
        Assert.True(X(p, "SET", "k", "v", "EX", "0").IsError);
        Assert.Equal(1, X(p, "PERSIST", "k").Integer);
        Assert.Equal(-1, X(p, "TTL", "k").Integer);
        Assert.Equal(-2, X(p, "TTL", "nokey").Integer);
        Assert.Equal("OK", X(p, "SETEX", "e", "10", "v").Text);
        Assert.Equal(1, X(p, "SETNX", "n", "1").Integer);
        Assert.Equal(0, X(p, "SETNX", "n", "2").Integer);
    }

    [Fact]
    public void ErrorsLookLikeRedis()
    {
        var p = Make();
        Assert.StartsWith("ERR unknown command", X(p, "NOPE").Text);
        Assert.StartsWith("ERR wrong number of arguments", X(p, "GET").Text);
        X(p, "SET", "s", "abc");
        Assert.StartsWith("ERR value is not an integer", X(p, "INCR", "s").Text);
        Assert.StartsWith("WRONGTYPE", X(p, "LPUSH", "s", "x").Text);
        Assert.StartsWith("ERR value is not an integer", X(p, "EXPIRE", "s", "abc").Text);
        Assert.StartsWith("ERR no such key", X(p, "RENAME", "zzz", "y").Text);
    }

    [Fact]
    public void CollectionsViaCommands()
    {
        var p = Make();
        Assert.Equal(2, X(p, "HSET", "h", "a", "1", "b", "2").Integer);
        Assert.Equal(4, X(p, "HGETALL", "h").Items!.Count);
        Assert.Equal(3, X(p, "RPUSH", "l", "1", "2", "3").Integer);
        Assert.Equal(new[] { "1", "2" }, X(p, "LRANGE", "l", "0", "1").Items!.Select(i => i.AsString()!).ToArray());
        Assert.Equal(2, X(p, "SADD", "s", "x", "y").Integer);
        Assert.Equal(1, X(p, "SISMEMBER", "s", "x").Integer);
        Assert.Equal(5, X(p, "INCRBY", "c", "5").Integer);
        Assert.Equal(4, X(p, "DECR", "c").Integer);
        Assert.Equal("OK", X(p, "MSET", "a", "1", "b", "2").Text);
        Assert.Equal(2, X(p, "MGET", "a", "b", "zz").Items!.Count(i => !i.IsNil));
        Assert.StartsWith("ERR wrong number", X(p, "HSET", "h", "a").Text);
    }

    [Fact]
    public void OomAndValueLimits()
    {
        var cfg = new ServerConfig { MaxMemory = 200, MaxValueBytes = 50 };
        var p = Make(cfg);
        Assert.StartsWith("ERR argument too large", p.Execute(Command.Of("SET", "k", new byte[51])).Text);
        for (var i = 0; i < 10; i++) p.Execute(Command.Of("SET", "k" + i, new byte[40]));
        Assert.StartsWith("OOM", X(p, "SET", "x", "y").Text);
        Assert.False(X(p, "GET", "k0").IsError);      // reads still allowed
        Assert.Equal(1, X(p, "DEL", "k0").Integer);   // deletes allowed under OOM
    }

    [Fact]
    public void ConfigAndInfo()
    {
        var p = Make();
        Assert.Contains("maxmemory-policy", X(p, "CONFIG", "GET", "maxmemory*").Items!.Select(i => i.AsString()));
        Assert.Equal("OK", X(p, "CONFIG", "SET", "maxmemory", "10mb").Text);
        Assert.StartsWith("ERR CONFIG SET is not supported", X(p, "CONFIG", "SET", "dir", "x").Text);
        var info = X(p, "INFO").AsString()!;
        Assert.Contains("used_memory:", info);
        Assert.Contains("db0:keys=", info);
    }
}

public class ResponseCacheTests
{
    [Fact]
    public void ReturnsCachedWithinWindowExpiresAfterAndBounds()
    {
        long now = 0;
        var c = new ResponseCache(TimeSpan.FromSeconds(10), 2, () => now);
        var a = Guid.NewGuid(); var b = Guid.NewGuid(); var d = Guid.NewGuid();
        c.Put(a, new byte[] { 1 });
        Assert.True(c.TryGet(a, out var body));
        Assert.Equal(new byte[] { 1 }, body);
        now = 10_001;
        Assert.False(c.TryGet(a, out _));
        c.Put(b, new byte[] { 2 }); c.Put(d, new byte[] { 3 }); c.Put(Guid.NewGuid(), new byte[] { 4 });
        Assert.Equal(2, c.Count);
        Assert.False(c.TryGet(b, out _));
        Assert.False(new ResponseCache(TimeSpan.Zero, 10).Enabled);
    }
}
