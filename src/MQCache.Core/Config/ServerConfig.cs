// File: src/MQCache.Core/Config/ServerConfig.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Strongly typed server configuration (redis.conf style) with validation.
// License: MIT

using System.Globalization;

namespace MQCache.Core.Config;

public sealed class ConfigException : Exception
{
    public ConfigException(string message) : base(message) { }
}

public enum EvictionPolicy { NoEviction, AllKeysLru, VolatileLru, VolatileTtl }

public enum QueueEncryption { None, Optional, Body }

public readonly record struct SaveRule(int Seconds, int Changes);

public sealed record MulticastChannel(string Channel, string Address);

public sealed class ServerConfig
{
    public const long MaxAllowedValueBytes = 3_670_016; // MSMQ hard limit is 4 MiB per message incl. envelope

    private bool _saveCustomized;

    // ------------------------------------------------------------------ MSMQ
    public string Queue { get; set; } = @"private$\mqcache";
    public bool QueueTransactional { get; set; } = true;
    public bool QueueJournal { get; set; }
    public long QueueJournalMaxKb { get; set; }
    public long QueueMaxSizeKb { get; set; }
    public bool QueueAuthenticate { get; set; }
    public QueueEncryption QueueEncryption { get; set; } = QueueEncryption.None;
    public List<string> QueueAllow { get; } = new();
    public List<string> QueueAdmin { get; } = new();
    public int Workers { get; set; } = 4;
    public int ReceiveTimeoutMs { get; set; } = 1000;
    public int ReplyTtlSeconds { get; set; } = 60;
    public bool ReplyRecoverable { get; set; }
    public string PoisonQueue { get; set; } = @"private$\mqcache.poison";
    public int PoisonMaxAttempts { get; set; } = 3;
    public int DedupeWindowSeconds { get; set; } = 60;
    public int DedupeMaxEntries { get; set; } = 100_000;

    // ----------------------------------------------------------------- store
    public long MaxMemory { get; set; }
    public EvictionPolicy MaxMemoryPolicy { get; set; } = EvictionPolicy.NoEviction;
    public long MaxValueBytes { get; set; } = 1_048_576;
    public int MaxBatchCommands { get; set; } = 1000;
    public int KeysMaxResults { get; set; } = 10_000;
    public int ActiveExpireIntervalMs { get; set; } = 100;

    // -------------------------------------------------------------- security
    public string? RequirePass { get; set; }

    // ----------------------------------------------------------- persistence
    public string Dir { get; set; } = "./data";
    public string DbFilename { get; set; } = "dump.mqdb";
    public List<SaveRule> Save { get; } = new() { new(900, 1), new(300, 100), new(60, 10_000) };
    public bool SaveOnShutdown { get; set; } = true;
    public string SnapshotPath => Path.Combine(Dir, DbFilename);

    // ---------------------------------------------------------------- pubsub
    public int PubSubSubscriptionTtlSeconds { get; set; } = 120;
    public List<MulticastChannel> MulticastChannels { get; } = new();

    // --------------------------------------------------------------- logging
    public LogLevel LogLevel { get; set; } = LogLevel.Info;
    public string? LogFile { get; set; }

    /// <summary>Applies one directive. Throws <see cref="ConfigException"/> for unknown keys or bad values.</summary>
    public void Set(string key, IReadOnlyList<string> v)
    {
        string One() => v.Count == 1 ? v[0] : throw new ConfigException($"'{key}' expects exactly one value");
        switch (key.ToLowerInvariant())
        {
            case "queue": Queue = One(); break;
            case "queue-transactional": QueueTransactional = Bool(key, One()); break;
            case "queue-journal": QueueJournal = Bool(key, One()); break;
            case "queue-journal-max-kb": QueueJournalMaxKb = Long(key, One()); break;
            case "queue-max-size-kb": QueueMaxSizeKb = Long(key, One()); break;
            case "queue-authenticate": QueueAuthenticate = Bool(key, One()); break;
            case "queue-encryption":
                QueueEncryption = One().ToLowerInvariant() switch
                {
                    "none" => QueueEncryption.None,
                    "optional" => QueueEncryption.Optional,
                    "body" => QueueEncryption.Body,
                    _ => throw new ConfigException("queue-encryption must be none|optional|body"),
                };
                break;
            case "queue-allow": QueueAllow.Add(One()); break;
            case "queue-admin": QueueAdmin.Add(One()); break;
            case "workers": Workers = (int)Long(key, One()); break;
            case "receive-timeout-ms": ReceiveTimeoutMs = (int)Long(key, One()); break;
            case "reply-ttl-seconds": ReplyTtlSeconds = (int)Long(key, One()); break;
            case "reply-recoverable": ReplyRecoverable = Bool(key, One()); break;
            case "poison-queue": PoisonQueue = One(); break;
            case "poison-max-attempts": PoisonMaxAttempts = (int)Long(key, One()); break;
            case "dedupe-window-seconds": DedupeWindowSeconds = (int)Long(key, One()); break;
            case "dedupe-max-entries": DedupeMaxEntries = (int)Long(key, One()); break;
            case "maxmemory": MaxMemory = Bytes(key, One()); break;
            case "maxmemory-policy":
                MaxMemoryPolicy = One().ToLowerInvariant() switch
                {
                    "noeviction" => EvictionPolicy.NoEviction,
                    "allkeys-lru" => EvictionPolicy.AllKeysLru,
                    "volatile-lru" => EvictionPolicy.VolatileLru,
                    "volatile-ttl" => EvictionPolicy.VolatileTtl,
                    _ => throw new ConfigException("maxmemory-policy must be noeviction|allkeys-lru|volatile-lru|volatile-ttl"),
                };
                break;
            case "max-value-bytes": MaxValueBytes = Bytes(key, One()); break;
            case "max-batch-commands": MaxBatchCommands = (int)Long(key, One()); break;
            case "keys-max-results": KeysMaxResults = (int)Long(key, One()); break;
            case "active-expire-interval-ms": ActiveExpireIntervalMs = (int)Long(key, One()); break;
            case "requirepass": RequirePass = One().Length == 0 ? null : One(); break;
            case "dir": Dir = One(); break;
            case "dbfilename": DbFilename = One(); break;
            case "save-on-shutdown": SaveOnShutdown = Bool(key, One()); break;
            case "save":
                if (!_saveCustomized) { Save.Clear(); _saveCustomized = true; }
                if (v.Count == 1 && (v[0].Length == 0 || v[0].Equals("off", StringComparison.OrdinalIgnoreCase))) break;
                if (v.Count != 2) throw new ConfigException("save expects '<seconds> <changes>' or \"\"");
                Save.Add(new SaveRule((int)Long(key, v[0]), (int)Long(key, v[1])));
                break;
            case "pubsub-subscription-ttl-seconds": PubSubSubscriptionTtlSeconds = (int)Long(key, One()); break;
            case "multicast-channel":
                if (v.Count != 2) throw new ConfigException("multicast-channel expects '<channel> <ip:port>'");
                MulticastChannels.Add(new MulticastChannel(v[0], v[1]));
                break;
            case "loglevel":
                LogLevel = One().ToLowerInvariant() switch
                {
                    "debug" => LogLevel.Debug,
                    "info" or "notice" => LogLevel.Info,
                    "warn" or "warning" => LogLevel.Warn,
                    "error" => LogLevel.Error,
                    _ => throw new ConfigException("loglevel must be debug|info|warn|error"),
                };
                break;
            case "logfile": LogFile = One().Length == 0 ? null : One(); break;
            default: throw new ConfigException($"unknown directive '{key}'");
        }
    }

    public void Validate()
    {
        static void Range(string name, long value, long min, long max)
        {
            if (value < min || value > max) throw new ConfigException($"'{name}' must be between {min} and {max} (got {value})");
        }

        if (string.IsNullOrWhiteSpace(Queue)) throw new ConfigException("'queue' must not be empty");
        if (string.IsNullOrWhiteSpace(PoisonQueue)) throw new ConfigException("'poison-queue' must not be empty");
        if (string.Equals(Queue, PoisonQueue, StringComparison.OrdinalIgnoreCase))
            throw new ConfigException("'queue' and 'poison-queue' must differ");
        Range("workers", Workers, 1, 64);
        Range("receive-timeout-ms", ReceiveTimeoutMs, 100, 60_000);
        Range("reply-ttl-seconds", ReplyTtlSeconds, 1, 86_400);
        Range("poison-max-attempts", PoisonMaxAttempts, 1, 100);
        Range("dedupe-window-seconds", DedupeWindowSeconds, 0, 86_400);
        Range("dedupe-max-entries", DedupeMaxEntries, 0, 10_000_000);
        Range("max-value-bytes", MaxValueBytes, 1, MaxAllowedValueBytes);
        Range("max-batch-commands", MaxBatchCommands, 1, 100_000);
        Range("keys-max-results", KeysMaxResults, 1, 10_000_000);
        Range("active-expire-interval-ms", ActiveExpireIntervalMs, 10, 60_000);
        Range("pubsub-subscription-ttl-seconds", PubSubSubscriptionTtlSeconds, 5, 86_400);
        if (MaxMemory < 0) throw new ConfigException("'maxmemory' must be >= 0");
        foreach (var r in Save)
            if (r.Seconds < 1 || r.Changes < 1) throw new ConfigException("save rules need seconds >= 1 and changes >= 1");
        foreach (var m in MulticastChannels)
        {
            var parts = m.Address.Split(':');
            if (parts.Length != 2 || !System.Net.IPAddress.TryParse(parts[0], out _) ||
                !int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var port) || port is < 1 or > 65535)
                throw new ConfigException($"invalid multicast address '{m.Address}' (expected ip:port)");
        }
    }

    /// <summary>Flat view used by CONFIG GET. Secrets are masked.</summary>
    public IReadOnlyDictionary<string, string> ToDictionary()
    {
        static string B(bool b) => b ? "yes" : "no";
        var d = new SortedDictionary<string, string>(StringComparer.Ordinal)
        {
            ["queue"] = Queue,
            ["queue-transactional"] = B(QueueTransactional),
            ["queue-journal"] = B(QueueJournal),
            ["queue-journal-max-kb"] = QueueJournalMaxKb.ToString(CultureInfo.InvariantCulture),
            ["queue-max-size-kb"] = QueueMaxSizeKb.ToString(CultureInfo.InvariantCulture),
            ["queue-authenticate"] = B(QueueAuthenticate),
            ["queue-encryption"] = QueueEncryption.ToString().ToLowerInvariant(),
            ["workers"] = Workers.ToString(CultureInfo.InvariantCulture),
            ["receive-timeout-ms"] = ReceiveTimeoutMs.ToString(CultureInfo.InvariantCulture),
            ["reply-ttl-seconds"] = ReplyTtlSeconds.ToString(CultureInfo.InvariantCulture),
            ["reply-recoverable"] = B(ReplyRecoverable),
            ["poison-queue"] = PoisonQueue,
            ["poison-max-attempts"] = PoisonMaxAttempts.ToString(CultureInfo.InvariantCulture),
            ["dedupe-window-seconds"] = DedupeWindowSeconds.ToString(CultureInfo.InvariantCulture),
            ["dedupe-max-entries"] = DedupeMaxEntries.ToString(CultureInfo.InvariantCulture),
            ["maxmemory"] = MaxMemory.ToString(CultureInfo.InvariantCulture),
            ["maxmemory-policy"] = MaxMemoryPolicy switch
            {
                EvictionPolicy.AllKeysLru => "allkeys-lru",
                EvictionPolicy.VolatileLru => "volatile-lru",
                EvictionPolicy.VolatileTtl => "volatile-ttl",
                _ => "noeviction",
            },
            ["max-value-bytes"] = MaxValueBytes.ToString(CultureInfo.InvariantCulture),
            ["max-batch-commands"] = MaxBatchCommands.ToString(CultureInfo.InvariantCulture),
            ["keys-max-results"] = KeysMaxResults.ToString(CultureInfo.InvariantCulture),
            ["requirepass"] = RequirePass is null ? "" : "(hidden)",
            ["dir"] = Dir,
            ["dbfilename"] = DbFilename,
            ["save"] = string.Join(' ', Save.Select(s => $"{s.Seconds} {s.Changes}")),
            ["save-on-shutdown"] = B(SaveOnShutdown),
            ["pubsub-subscription-ttl-seconds"] = PubSubSubscriptionTtlSeconds.ToString(CultureInfo.InvariantCulture),
            ["loglevel"] = LogLevel.ToString().ToLowerInvariant(),
        };
        return d;
    }

    private static bool Bool(string key, string v) => v.ToLowerInvariant() switch
    {
        "yes" or "true" or "on" or "1" => true,
        "no" or "false" or "off" or "0" => false,
        _ => throw new ConfigException($"'{key}' expects yes|no"),
    };

    private static long Long(string key, string v) =>
        long.TryParse(v, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var n)
            ? n : throw new ConfigException($"'{key}' expects an integer (got '{v}')");

    /// <summary>Parses sizes such as 1048576, 64kb, 100mb, 2gb (base 1024).</summary>
    public static long Bytes(string key, string v)
    {
        var s = v.Trim().ToLowerInvariant();
        long mult = 1;
        foreach (var (suffix, m) in new[] { ("gb", 1L << 30), ("mb", 1L << 20), ("kb", 1L << 10), ("g", 1L << 30), ("m", 1L << 20), ("k", 1L << 10), ("b", 1L) })
        {
            if (s.EndsWith(suffix, StringComparison.Ordinal)) { mult = m; s = s[..^suffix.Length]; break; }
        }
        if (!long.TryParse(s, NumberStyles.None, CultureInfo.InvariantCulture, out var n))
            throw new ConfigException($"'{key}' expects a size such as 64kb, 100mb, 2gb (got '{v}')");
        try { return checked(n * mult); }
        catch (OverflowException) { throw new ConfigException($"'{key}' is too large"); }
    }
}
