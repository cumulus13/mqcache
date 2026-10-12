// File: src/MQCache.Messaging/MessageFactory.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Builds request/response/publication messages using the MSMQ features (priority, TTL, journal, DLQ, acks).
// License: MIT

using Experimental.System.Messaging;

namespace MQCache.Messaging;

public sealed class SendOptions
{
    public MessagePriority Priority { get; set; } = MessagePriority.Normal;
    /// <summary>TimeToBeReceived: MSMQ discards (or dead-letters) the message if it is not consumed in time.</summary>
    public TimeSpan Ttl { get; set; } = TimeSpan.FromSeconds(30);
    public bool Recoverable { get; set; } = true;
    public bool UseJournal { get; set; }
    public bool UseDeadLetter { get; set; } = true;
    public bool UseTracing { get; set; }
    public AcknowledgeTypes Acknowledge { get; set; } = AcknowledgeTypes.None;
}

public static class MessageFactory
{
    public const string RequestLabel = "mqcache.req";
    public const string ResponseLabel = "mqcache.resp";
    public const string PublishLabel = "mqcache.pub";
    public const int ProtocolId = 1;
    /// <summary>VT_ARRAY | VT_UI1: lets COM/C API clients (Python, Go, Rust, ...) read the body as a plain byte array.</summary>
    public const int BodyTypeByteArray = 0x2011;
    public const long MaxBodyBytes = 4L * 1024 * 1024;

    public static Message Request(byte[] body, SendOptions o, string? responseFormat, string? adminFormat)
    {
        var m = new Message
        {
            Label = RequestLabel,
            AppSpecific = ProtocolId,
            BodyStream = new MemoryStream(body, writable: false),
            BodyType = BodyTypeByteArray,
            Priority = o.Priority,
            Recoverable = o.Recoverable,
            TimeToBeReceived = o.Ttl,
            UseJournalQueue = o.UseJournal,
            UseDeadLetterQueue = o.UseDeadLetter,
            UseTracing = o.UseTracing,
        };
        if (responseFormat is not null) m.ResponseQueue = new MessageQueue("FormatName:" + responseFormat);
        if (o.Acknowledge != AcknowledgeTypes.None && adminFormat is not null)
        {
            m.AdministrationQueue = new MessageQueue("FormatName:" + adminFormat);
            m.AcknowledgeType = o.Acknowledge;
        }
        return m;
    }

    public static Message Response(byte[] body, string correlationId, MessagePriority priority, TimeSpan ttl, bool recoverable) => new()
    {
        Label = ResponseLabel,
        AppSpecific = ProtocolId,
        BodyStream = new MemoryStream(body, writable: false),
        BodyType = BodyTypeByteArray,
        CorrelationId = correlationId,
        Priority = priority,
        Recoverable = recoverable,
        TimeToBeReceived = ttl,
        UseDeadLetterQueue = false,
    };

    public static Message Publication(byte[] body, TimeSpan ttl) => new()
    {
        Label = PublishLabel,
        AppSpecific = ProtocolId,
        BodyStream = new MemoryStream(body, writable: false),
        BodyType = BodyTypeByteArray,
        Recoverable = false,
        TimeToBeReceived = ttl,
        UseDeadLetterQueue = false,
    };

    /// <summary>Copies a message (for the poison queue) keeping body, label class and priority; the reason goes in the label.</summary>
    public static Message Poison(Message original, string reason)
    {
        var label = "mqcache.poison:" + reason;
        return new Message
        {
            Label = label.Length > 120 ? label[..120] : label,
            AppSpecific = original.AppSpecific,
            BodyStream = new MemoryStream(ReadBody(original, MaxBodyBytes), writable: false),
            BodyType = BodyTypeByteArray,
            Priority = original.Priority,
            Recoverable = true,
        };
    }

    public static byte[] ReadBody(Message m, long maxBytes)
    {
        var s = m.BodyStream ?? throw new InvalidDataException("message has no body");
        if (s.CanSeek)
        {
            if (s.Length > maxBytes) throw new InvalidDataException("message body too large");
            s.Position = 0;
        }
        using var ms = new MemoryStream();
        s.CopyTo(ms);
        return ms.ToArray();
    }
}
