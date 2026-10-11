// File: src/MQCache.Messaging/QueueAdmin.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Creates/configures local MSMQ queues (transactional, journal, quotas, auth, encryption, ACLs) and counts messages.
// License: MIT

using Experimental.System.Messaging;
using MQCache.Core;
using MQCache.Core.Config;

namespace MQCache.Messaging;

public sealed record QueueSettings(
    string Label,
    bool Journal = false,
    long JournalMaxKb = 0,
    long MaxSizeKb = 0,
    bool Authenticate = false,
    QueueEncryption Encryption = QueueEncryption.None,
    IReadOnlyList<string>? Allow = null,
    IReadOnlyList<string>? Admin = null);

public static class QueueAdmin
{
    public const string LabelPrefix = "mqcache.";

    /// <summary>Throws a descriptive exception when the MSMQ service/feature is unavailable.</summary>
    public static void EnsureMsmqAvailable()
    {
        try { MessageQueue.Exists(@".\private$\mqcache.probe"); }
        catch (MessageQueueException ex)
        {
            throw new InvalidOperationException(
                "MSMQ is not available (" + ex.MessageQueueErrorCode + "). Enable the 'Microsoft Message Queue (MSMQ) Server' " +
                "Windows feature and make sure the 'Message Queuing' service is running.", ex);
        }
    }

    /// <summary>Creates the queue when missing and (re)applies settings. Never changes the transactional flag of an existing queue.</summary>
    public static void EnsureLocal(string path, bool transactional, QueueSettings s, ILog log)
    {
        if (!MessageQueue.Exists(path))
        {
            using var created = MessageQueue.Create(path, transactional);
            log.Info($"created queue {path} (transactional={transactional})");
        }

        using var q = new MessageQueue(path);
        if (q.Transactional != transactional)
            throw new InvalidOperationException(
                $"Queue {path} exists with transactional={q.Transactional} but configuration requires {transactional}. " +
                "MSMQ cannot change this flag; delete the queue (after draining it) or fix the configuration.");

        Try(log, "label", () => q.Label = s.Label);
        Try(log, "journal", () => q.UseJournalQueue = s.Journal);
        if (s.JournalMaxKb > 0) Try(log, "journal size", () => q.MaximumJournalSize = s.JournalMaxKb);
        if (s.MaxSizeKb > 0) Try(log, "queue size", () => q.MaximumQueueSize = s.MaxSizeKb);
        Try(log, "authenticate", () => q.Authenticate = s.Authenticate);
        Try(log, "encryption", () => q.EncryptionRequired = s.Encryption switch
        {
            QueueEncryption.Body => EncryptionRequired.Body,
            QueueEncryption.Optional => EncryptionRequired.Optional,
            _ => EncryptionRequired.None,
        });

        foreach (var user in s.Allow ?? Array.Empty<string>())
            Try(log, "allow " + user, () => Grant(q, user,
                MessageQueueAccessRights.WriteMessage | MessageQueueAccessRights.GetQueueProperties));
        foreach (var user in s.Admin ?? Array.Empty<string>())
            Try(log, "admin " + user, () => Grant(q, user, MessageQueueAccessRights.FullControl));
    }

    private static void Grant(MessageQueue q, string account, MessageQueueAccessRights rights) =>
        q.SetPermissions(new MessageQueueAccessControlEntry(new Trustee(account), rights, AccessControlEntryType.Allow));

    private static void Try(ILog log, string what, Action a)
    {
        try { a(); }
        catch (Exception ex) when (ex is MessageQueueException or InvalidOperationException or UnauthorizedAccessException)
        {
            log.Warn($"could not apply queue setting '{what}': {ex.Message}");
        }
    }

    public static MessageQueue Open(string pathOrFormat, QueueAccessMode mode)
    {
        var q = new MessageQueue(pathOrFormat, false, true, mode);
        Configure(q);
        return q;
    }

    /// <summary>Enables the message properties the protocol needs (the default filter omits several).</summary>
    public static void Configure(MessageQueue q)
    {
        var f = q.MessageReadPropertyFilter;
        f.Body = true; f.Id = true; f.CorrelationId = true; f.Label = true; f.AppSpecific = true;
        f.ResponseQueue = true; f.Priority = true; f.ArrivedTime = true; f.SentTime = true; f.Acknowledgment = true;
        f.DefaultBodySize = 8192;
    }

    /// <summary>Counts messages (optionally only those whose label starts with <paramref name="labelPrefix"/>), up to a limit.</summary>
    public static int Count(string path, int limit = 100_000, string? labelPrefix = null)
    {
        using var q = new MessageQueue(path, false, true, QueueAccessMode.Peek);
        q.MessageReadPropertyFilter.ClearAll();
        if (labelPrefix is not null) q.MessageReadPropertyFilter.Label = true;
        var n = 0;
        using var e = q.GetMessageEnumerator2();
        while (n < limit && e.MoveNext(TimeSpan.Zero))
        {
            if (labelPrefix is null || (e.Current?.Label ?? "").StartsWith(labelPrefix, StringComparison.Ordinal)) n++;
        }
        return n;
    }

    /// <summary>Returns "label | arrived | bytes" lines for the first messages matching the prefix.</summary>
    public static IReadOnlyList<string> Peek(string path, int max, string? labelPrefix)
    {
        using var q = new MessageQueue(path, false, true, QueueAccessMode.Peek);
        Configure(q);
        var list = new List<string>();
        using var e = q.GetMessageEnumerator2();
        while (list.Count < max && e.MoveNext(TimeSpan.Zero))
        {
            var m = e.Current!;
            if (labelPrefix is not null && !(m.Label ?? "").StartsWith(labelPrefix, StringComparison.Ordinal)) continue;
            list.Add($"{m.Label} | {m.ArrivedTime:u} | {m.BodyStream?.Length ?? 0} bytes | class={m.MessageType}");
        }
        return list;
    }

    /// <summary>Removes only messages with our label prefix (never touches other applications' messages in shared system queues).</summary>
    public static int Purge(string path, bool transactional, string labelPrefix)
    {
        using var q = new MessageQueue(path, false, true, QueueAccessMode.ReceiveAndAdmin);
        q.MessageReadPropertyFilter.ClearAll();
        q.MessageReadPropertyFilter.Label = true;
        var removed = 0;
        using var e = q.GetMessageEnumerator2();
        while (e.MoveNext(TimeSpan.Zero))
        {
            if (!(e.Current?.Label ?? "").StartsWith(labelPrefix, StringComparison.Ordinal)) continue;
            if (transactional) e.RemoveCurrent(TimeSpan.Zero, MessageQueueTransactionType.Single);
            else e.RemoveCurrent(TimeSpan.Zero);
            removed++;
        }
        return removed;
    }
}
