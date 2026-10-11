// File: src/MQCache.Client/ReplyQueues.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Names and cleans up per-process reply/subscription queues (orphans left by crashed clients).
// License: MIT

using System.Diagnostics;
using System.Globalization;
using Experimental.System.Messaging;

namespace MQCache.Client;

internal static class ReplyQueues
{
    public const string ReplyPrefix = "mqcache.reply.";
    public const string SubPrefix = "mqcache.sub.";

    private static long StartTicks() => Process.GetCurrentProcess().StartTime.ToUniversalTime().Ticks;

    /// <summary>Name encodes pid + process start time so that dead owners can be detected later.</summary>
    public static string NewName(string prefix) =>
        $"{prefix}{Environment.ProcessId}.{StartTicks().ToString(CultureInfo.InvariantCulture)}.{Guid.NewGuid().ToString("N")[..8]}";

    public static string LocalPath(string name) => @".\private$\" + name;

    public static string Format(string scheme, string? host, string name) =>
        $@"DIRECT={scheme.ToUpperInvariant()}:{host ?? Environment.MachineName}\private$\{name}";

    public static void Sweep()
    {
        MessageQueue[] queues;
        try { queues = MessageQueue.GetPrivateQueuesByMachine("."); }
        catch (MessageQueueException) { return; }

        foreach (var q in queues)
        {
            var name = StripPrivate(q.QueueName);
            var prefix = name.StartsWith(ReplyPrefix, StringComparison.Ordinal) ? ReplyPrefix
                       : name.StartsWith(SubPrefix, StringComparison.Ordinal) ? SubPrefix : null;
            if (prefix is null) continue;
            var parts = name[prefix.Length..].Split('.');
            if (parts.Length < 3 || !int.TryParse(parts[0], out var pid) || !long.TryParse(parts[1], out var ticks)) continue;
            if (IsAlive(pid, ticks)) continue;
            try { MessageQueue.Delete(LocalPath(name)); }
            catch (Exception ex) when (ex is MessageQueueException or InvalidOperationException) { /* someone else got it first */ }
        }
    }

    public static void DeleteQuietly(string name)
    {
        try { MessageQueue.Delete(LocalPath(name)); }
        catch (Exception ex) when (ex is MessageQueueException or InvalidOperationException) { }
    }

    private static string StripPrivate(string n) =>
        n.StartsWith(@"private$\", StringComparison.OrdinalIgnoreCase) ? n[@"private$\".Length..] : n;

    private static bool IsAlive(int pid, long startTicks)
    {
        try
        {
            using var p = Process.GetProcessById(pid);
            return p.StartTime.ToUniversalTime().Ticks == startTicks;
        }
        catch (ArgumentException) { return false; }          // no such process
        catch (InvalidOperationException) { return false; }  // exited meanwhile
        catch (System.ComponentModel.Win32Exception) { return true; } // cannot inspect: assume alive
    }
}
