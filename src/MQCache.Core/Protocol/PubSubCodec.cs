// File: src/MQCache.Core/Protocol/PubSubCodec.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Wire format of published messages delivered to subscriber queues.
// License: MIT

using System.Text;

namespace MQCache.Core.Protocol;

/// <summary>A message delivered to a subscriber. <see cref="Pattern"/> is set for PSUBSCRIBE deliveries.</summary>
public sealed record PubMessage(string Channel, string? Pattern, byte[] Payload)
{
    public string PayloadText => Encoding.UTF8.GetString(Payload);
}

public static class PubSubCodec
{
    private const byte Version = 1;

    public static byte[] Encode(PubMessage m)
    {
        using var ms = new MemoryStream();
        using var w = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);
        w.Write(Version);
        w.Write(m.Pattern is null ? (byte)1 : (byte)2);
        WriteBytes(w, Encoding.UTF8.GetBytes(m.Pattern ?? string.Empty));
        WriteBytes(w, Encoding.UTF8.GetBytes(m.Channel));
        WriteBytes(w, m.Payload);
        w.Flush();
        return ms.ToArray();
    }

    public static PubMessage Decode(byte[] data)
    {
        try
        {
            using var ms = new MemoryStream(data, writable: false);
            using var r = new BinaryReader(ms, Encoding.UTF8);
            if (r.ReadByte() != Version) throw new WireFormatException("unsupported publish version");
            var kind = r.ReadByte();
            var pattern = Encoding.UTF8.GetString(ReadBytes(r, ms));
            var channel = Encoding.UTF8.GetString(ReadBytes(r, ms));
            var payload = ReadBytes(r, ms);
            return new PubMessage(channel, kind == 2 ? pattern : null, payload);
        }
        catch (EndOfStreamException) { throw new WireFormatException("truncated publish message"); }
    }

    private static void WriteBytes(BinaryWriter w, byte[] b) { w.Write(b.Length); w.Write(b); }

    private static byte[] ReadBytes(BinaryReader r, MemoryStream ms)
    {
        var n = r.ReadInt32();
        if (n < 0 || n > ms.Length - ms.Position) throw new WireFormatException("invalid length");
        return r.ReadBytes(n);
    }
}
