// File: tests/MQCache.IntegrationTests/EndToEndTests.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: End-to-end tests against a real MSMQ (skipped automatically when the MSMQ service is not available).
// License: MIT

using Experimental.System.Messaging;
using MQCache.Client;
using MQCache.Core;
using MQCache.Core.Config;
using MQCache.Core.Protocol;
using MQCache.Messaging;
using MQCache.Server;
using Xunit;

namespace MQCache.IntegrationTests;

internal static class MsmqProbe
{
    public static readonly bool Available = Check();
    private static bool Check()
    {
        try { MessageQueue.Exists(@".\private$\mqcache.probe"); return true; }
        catch (Exception) { return false; }
    }
}

public sealed class MsmqFactAttribute : FactAttribute
{
    public MsmqFactAttribute() { if (!MsmqProbe.Available) Skip = "MSMQ is not installed/running on this machine"; }
}

/// <summary>Boots a real server on uniquely named queues and cleans everything up afterwards.</summary>
public sealed class Harness : IDisposable
{
    public string Queue { get; } = "mqcache.it." + Guid.NewGuid().ToString("N")[..10];
    public ServerConfig Config { get; }
    public CacheServer Server { get; private set; }
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "mqcache-it-" + Guid.NewGuid().ToString("N"));

    public Harness(Action<ServerConfig>? tweak = null)
    {
        Config = new ServerConfig { Queue = Queue, PoisonQueue = Queue + ".poison", Dir = _dir, Workers = 2, ReceiveTimeoutMs = 200 };
        Config.Save.Clear();
        tweak?.Invoke(Config);
        Config.Validate();
        Server = new CacheServer(Config, NullLog.Instance);
        Server.Start();
    }

    public CacheClient Client(Action<CacheClientOptions>? tweak = null)
    {
        var o = new CacheClientOptions { QueueName = Queue, Timeout = TimeSpan.FromSeconds(10), Password = Config.RequirePass };
        tweak?.Invoke(o);
        return new CacheClient(o);
    }

    public void Restart()
    {
        Server.Dispose();
        Server = new CacheServer(Config, NullLog.Instance);
        Server.Start();
    }

    public void Dispose()
    {
        Server.Dispose();
        foreach (var q in new[] { Queue, Queue + ".poison" })
        {
            try { MessageQueue.Delete(MsmqPaths.LocalPrivate(q)); } catch (Exception) { }
        }
        try { Directory.Delete(_dir, true); } catch (Exception) { }
    }
}

public class EndToEndTests
{
    [MsmqFact]
    public void PingAndBasicCommands()
    {
        using var h = new Harness();
        using var c = h.Client();
        Assert.True(c.Ping() < TimeSpan.FromSeconds(10));
        Assert.True(c.Set("k", "v"));
        Assert.Equal("v", c.GetString("k"));
        Assert.Equal(1, c.Delete("k"));
        Assert.Null(c.GetString("k"));
    }

    [MsmqFact]
    public void BinarySafeAndTtl()
    {
        using var h = new Harness();
        using var c = h.Client();
        var bytes = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray();
        c.Set("bin", bytes, ttl: TimeSpan.FromMilliseconds(400));
        Assert.Equal(bytes, c.GetBytes("bin"));
        Thread.Sleep(900);
        Assert.Null(c.GetBytes("bin"));
    }

    [MsmqFact]
    public void PipelineRunsAtomicallyInOneMessage()
    {
        using var h = new Harness();
        using var c = h.Client();
        var r = c.Pipeline(new[] { Command.Of("SET", "a", "1"), Command.Of("INCR", "a"), Command.Of("GET", "a") });
        Assert.Equal(3, r.Count);
        Assert.Equal(2, r[1].Integer);
        Assert.Equal("2", r[2].AsString());
    }

    [MsmqFact]
    public void ConcurrentIncrementsAreExact()
    {
        using var h = new Harness();
        using var c = h.Client();
        Parallel.For(0, 8, _ => { for (var i = 0; i < 25; i++) c.Increment("counter"); });
        Assert.Equal("200", c.GetString("counter"));
    }

    [MsmqFact]
    public void PasswordIsEnforced()
    {
        using var h = new Harness(cfg => cfg.RequirePass = "s3cret");
        using var good = h.Client();
        Assert.True(good.Set("k", "v"));
        using var bad = h.Client(o => o.Password = "nope");
        Assert.Throws<CacheServerException>(() => bad.Ping());
        using var none = h.Client(o => o.Password = null);
        Assert.Throws<CacheServerException>(() => none.Ping());
    }

    [MsmqFact]
    public void WorksWithNonTransactionalQueueViaAutoFallback()
    {
        using var h = new Harness(cfg => cfg.QueueTransactional = false);
        using var c = h.Client(o => o.Transactional = true); // wrong guess on purpose
        Assert.True(c.Set("k", "v"));
        Assert.Equal("v", c.GetString("k"));
    }

    [MsmqFact]
    public void ServerErrorsSurfaceAsExceptions()
    {
        using var h = new Harness();
        using var c = h.Client();
        c.Set("s", "text");
        Assert.Throws<CacheServerException>(() => c.Increment("s"));
        Assert.Throws<CacheServerException>(() => c.ListPush("s", "x"));
    }

    [MsmqFact]
    public void PubSubDeliversToChannelAndPatternSubscribers()
    {
        using var h = new Harness();
        using var c = h.Client();
        using var s1 = c.Subscribe("news");
        using var s2 = c.PSubscribe("new*");
        Thread.Sleep(500); // subscriptions are sent asynchronously
        Assert.Equal(2, c.Publish("news", "hello"));
        Assert.True(s1.TryRead(TimeSpan.FromSeconds(5), out var m1));
        Assert.Equal("hello", m1!.PayloadText);
        Assert.True(s2.TryRead(TimeSpan.FromSeconds(5), out var m2));
        Assert.Equal("new*", m2!.Pattern);
        Assert.Equal(0, c.Publish("other", "x"));
    }

    [MsmqFact]
    public void ReceiveAckConfirmsConsumption()
    {
        using var h = new Harness();
        using var c = h.Client();
        c.Send(Command.Of("SET", "fire", "forget"), waitForReceiveAck: true);
        Assert.Equal("forget", c.GetString("fire"));
    }

    [MsmqFact]
    public void MalformedMessageGoesToPoisonQueue()
    {
        using var h = new Harness();
        using var c = h.Client();
        using (var q = new MessageQueue(MsmqPaths.LocalPrivate(h.Queue)))
        {
            var m = new Message { Label = "garbage", BodyStream = new MemoryStream(new byte[] { 9, 9, 9 }) };
            q.Send(m, MessageQueueTransactionType.Single);
        }
        for (var i = 0; i < 50 && c.Execute("POISON", "COUNT").Integer == 0; i++) Thread.Sleep(100);
        Assert.Equal(1, c.Execute("POISON", "COUNT").Integer);
        Assert.Equal(1, c.Execute("POISON", "PURGE").Integer);
        Assert.True(c.Ping() > TimeSpan.Zero); // server still healthy
    }

    [MsmqFact]
    public void SnapshotSurvivesRestart()
    {
        using var h = new Harness(cfg => { cfg.Save.Add(new SaveRule(3600, 1_000_000)); });
        using (var c = h.Client())
        {
            c.Set("persist", "me");
            c.HashSet("h", "f", "v");
            c.Save();
        }
        h.Restart();
        using var c2 = h.Client();
        Assert.Equal("me", c2.GetString("persist"));
        Assert.Equal("v", c2.HashGet("h", "f"));
    }

    [MsmqFact]
    public void QueueAndInfoAdminCommands()
    {
        using var h = new Harness();
        using var c = h.Client();
        Assert.Contains("request_queue_depth:", c.Execute("QUEUE", "INFO").AsString()!);
        var info = c.Info();
        Assert.Contains("# Msmq", info);
        Assert.Contains("used_memory:", info);
    }
}
