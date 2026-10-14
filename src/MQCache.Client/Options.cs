// File: src/MQCache.Client/Options.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Client options, connection-string parsing and exception types.
// License: MIT

using System.Globalization;
using Experimental.System.Messaging;

namespace MQCache.Client;

public class CacheException : Exception
{
    public CacheException(string message, Exception? inner = null) : base(message, inner) { }
}

public sealed class CacheTimeoutException : CacheException
{
    public CacheTimeoutException(string message, Exception? inner = null) : base(message, inner) { }
}

/// <summary>The server answered with an error reply (e.g. WRONGTYPE, OOM, NOAUTH).</summary>
public sealed class CacheServerException : CacheException
{
    public CacheServerException(string message) : base(message) { }
}

public sealed class CacheClientOptions
{
    /// <summary>"." (local), "host", "os:host" or "tcp:10.0.0.5", optionally "/queue".</summary>
    public string Endpoint { get; set; } = ".";
    public string QueueName { get; set; } = "mqcache";
    /// <summary>Initial guess; the client flips automatically if the server queue is of the other kind.</summary>
    public bool Transactional { get; set; } = true;
    /// <summary>
    /// Read the queue's transactional flag when the server queue is local (exact, no guessing). For remote queues MSMQ
    /// offers no way to ask, so the client falls back to probing both send modes on the first request.
    /// </summary>
    public bool DetectQueueType { get; set; } = true;
    public string? Password { get; set; }
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(5);
    /// <summary>Re-sends with the same request id after a timeout; the server de-duplicates.</summary>
    public int Retries { get; set; } = 2;
    public MessagePriority Priority { get; set; } = MessagePriority.Normal;
    /// <summary>Persist request messages to disk in MSMQ (survives MSMQ/host restart).</summary>
    public bool Recoverable { get; set; } = true;
    public bool UseJournal { get; set; }
    public bool UseDeadLetter { get; set; } = true;
    public bool UseTracing { get; set; }
    /// <summary>Host name the server uses to reach this client's reply queue (default: this machine's name).</summary>
    public string? ReplyHost { get; set; }
    /// <summary>"os" (name resolution) or "tcp" (set ReplyHost to an IP address).</summary>
    public string ReplyScheme { get; set; } = "os";
    public bool SweepOrphanQueues { get; set; } = true;
    public TimeSpan SubscriptionRenewInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>"endpoint=tcp:10.0.0.5;queue=mqcache;password=x;timeout=3000;retries=1;transactional=false;priority=high"</summary>
    public static CacheClientOptions FromConnectionString(string cs)
    {
        var o = new CacheClientOptions();
        foreach (var part in cs.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var eq = part.IndexOf('=');
            if (eq <= 0) throw new ArgumentException($"invalid connection string segment '{part}'");
            var k = part[..eq].Trim().ToLowerInvariant();
            var v = part[(eq + 1)..].Trim();
            switch (k)
            {
                case "endpoint": o.Endpoint = v; break;
                case "queue": o.QueueName = v; break;
                case "password": o.Password = v; break;
                case "timeout": o.Timeout = TimeSpan.FromMilliseconds(long.Parse(v, CultureInfo.InvariantCulture)); break;
                case "retries": o.Retries = int.Parse(v, CultureInfo.InvariantCulture); break;
                case "transactional": o.Transactional = bool.Parse(v); break;
                case "detectqueuetype": o.DetectQueueType = bool.Parse(v); break;
                case "recoverable": o.Recoverable = bool.Parse(v); break;
                case "journal": o.UseJournal = bool.Parse(v); break;
                case "deadletter": o.UseDeadLetter = bool.Parse(v); break;
                case "replyhost": o.ReplyHost = v; break;
                case "replyscheme": o.ReplyScheme = v; break;
                case "priority": o.Priority = Enum.Parse<MessagePriority>(v, ignoreCase: true); break;
                default: throw new ArgumentException($"unknown connection string key '{k}'");
            }
        }
        return o;
    }
}

/// <summary>Per-call overrides.</summary>
public sealed class CallOptions
{
    public TimeSpan? Timeout { get; set; }
    public MessagePriority? Priority { get; set; }
    public bool? Recoverable { get; set; }
    public int? Retries { get; set; }
}

public enum SetWhen { Always, NotExists, Exists }
