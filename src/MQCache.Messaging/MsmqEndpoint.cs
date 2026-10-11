// File: src/MQCache.Messaging/MsmqEndpoint.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Parses endpoint specs ([os:|tcp:]host[/queue]) into MSMQ path/format names.
// License: MIT

namespace MQCache.Messaging;

/// <summary>
/// Addressing helper. Only private queues are used (no Active Directory needed).
/// Remote queues use DIRECT format names, e.g. <c>FormatName:DIRECT=TCP:10.0.0.5\private$\mqcache</c>.
/// </summary>
public sealed record MsmqEndpoint(string Host, string Scheme, string QueueName)
{
    public const string DefaultQueue = "mqcache";

    public bool IsLocal =>
        Host.Length == 0 || Host == "." || Host.Equals("localhost", StringComparison.OrdinalIgnoreCase) ||
        Host.Equals(Environment.MachineName, StringComparison.OrdinalIgnoreCase);

    /// <summary>Path for local administration (exists/create/delete).</summary>
    public string LocalPath => MsmqPaths.LocalPrivate(QueueName);

    /// <summary>What to pass to the MessageQueue constructor for sending/receiving.</summary>
    public string QueueSpec => IsLocal
        ? LocalPath
        : $@"FormatName:DIRECT={Scheme.ToUpperInvariant()}:{Host}\private$\{QueueName}";

    public static MsmqEndpoint Parse(string? spec, string defaultQueue = DefaultQueue)
    {
        spec = spec?.Trim() ?? string.Empty;
        var queue = defaultQueue;
        var slash = spec.IndexOf('/');
        if (slash >= 0)
        {
            queue = MsmqPaths.StripPrivatePrefix(spec[(slash + 1)..]);
            spec = spec[..slash];
        }
        var scheme = "OS";
        var colon = spec.IndexOf(':');
        if (colon > 0)
        {
            scheme = spec[..colon].ToUpperInvariant();
            spec = spec[(colon + 1)..];
            if (scheme is not ("OS" or "TCP"))
                throw new ArgumentException($"unsupported endpoint scheme '{scheme}' (use os: or tcp:)");
        }
        if (queue.Length == 0) throw new ArgumentException("empty queue name");
        return new MsmqEndpoint(spec, scheme, queue);
    }
}

public static class MsmqPaths
{
    public static string StripPrivatePrefix(string name)
    {
        name = name.Trim().TrimStart('.', '\\', '/');
        foreach (var p in new[] { @"private$\", "private$/" })
            if (name.StartsWith(p, StringComparison.OrdinalIgnoreCase)) return name[p.Length..];
        return name;
    }

    /// <summary>Turns "mqcache", "private$\mqcache" or ".\private$\mqcache" into ".\private$\mqcache".</summary>
    public static string LocalPrivate(string configured)
    {
        if (configured.StartsWith("FormatName:", StringComparison.OrdinalIgnoreCase)) return configured;
        return @".\private$\" + StripPrivatePrefix(configured);
    }

    public static string Journal(string localPath) => localPath + @"\Journal$";
}
