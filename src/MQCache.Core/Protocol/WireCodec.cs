// File: src/MQCache.Core/Protocol/WireCodec.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Binary request/response envelope codec with strict bounds checking (MSMQ message bodies).
// License: MIT

using System.Buffers.Binary;
using System.Text;

namespace MQCache.Core.Protocol;

public sealed class WireFormatException : Exception
{
    public WireFormatException(string message) : base(message) { }
}

/// <summary>One MSMQ request message carries 1..N commands (pipeline) and a client-generated idempotency id.</summary>
public sealed record RequestEnvelope(Guid RequestId, string? Auth, bool Atomic, bool NoReply, IReadOnlyList<Command> Commands);

public sealed record ResponseEnvelope(Guid RequestId, IReadOnlyList<RespValue> Results);

public static class WireCodec
{
    public const byte Version = 1;
    public const int MaxCommands = 100_000;
    public const int MaxArgsPerCommand = 65_536;
    public const int MaxDepth = 16;

    private const byte FlagAtomic = 1;
    private const byte FlagNoReply = 2;

    // ---------------------------------------------------------------- request
    public static byte[] EncodeRequest(RequestEnvelope request)
    {
        using var ms = new MemoryStream();
        using var w = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);
        w.Write(Version);
        w.Write((byte)((request.Atomic ? FlagAtomic : 0) | (request.NoReply ? FlagNoReply : 0)));
        w.Write(request.RequestId.ToByteArray());
        WriteBytes(w, Encoding.UTF8.GetBytes(request.Auth ?? string.Empty));
        w.Write(request.Commands.Count);
        foreach (var c in request.Commands)
        {
            w.Write(c.Args.Length + 1);
            WriteBytes(w, Encoding.UTF8.GetBytes(c.Name));
            foreach (var a in c.Args) WriteBytes(w, a);
        }
        w.Flush();
        return ms.ToArray();
    }

    public static RequestEnvelope DecodeRequest(byte[] data)
    {
        var r = new ByteReader(data);
        var version = r.ReadByte();
        if (version != Version) throw new WireFormatException($"unsupported protocol version {version}");
        var flags = r.ReadByte();
        var id = new Guid(r.ReadBytes(16));
        var auth = Encoding.UTF8.GetString(r.ReadLengthPrefixed());
        var count = r.ReadInt32();
        if (count < 1 || count > MaxCommands) throw new WireFormatException("invalid command count");
        var commands = new List<Command>(Math.Min(count, 1024));
        for (var i = 0; i < count; i++)
        {
            var argc = r.ReadInt32();
            if (argc < 1 || argc > MaxArgsPerCommand) throw new WireFormatException("invalid argument count");
            var name = Encoding.UTF8.GetString(r.ReadLengthPrefixed());
            var args = new byte[argc - 1][];
            for (var a = 0; a < args.Length; a++) args[a] = r.ReadLengthPrefixed();
            commands.Add(new Command(name, args));
        }
        if (r.Remaining != 0) throw new WireFormatException("trailing bytes in request");
        return new RequestEnvelope(id, auth.Length == 0 ? null : auth, (flags & FlagAtomic) != 0, (flags & FlagNoReply) != 0, commands);
    }

    // --------------------------------------------------------------- response
    public static byte[] EncodeResponse(ResponseEnvelope response)
    {
        using var ms = new MemoryStream();
        using var w = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);
        w.Write(Version);
        w.Write(response.RequestId.ToByteArray());
        w.Write(response.Results.Count);
        foreach (var v in response.Results) WriteValue(w, v);
        w.Flush();
        return ms.ToArray();
    }

    public static ResponseEnvelope DecodeResponse(byte[] data)
    {
        var r = new ByteReader(data);
        var version = r.ReadByte();
        if (version != Version) throw new WireFormatException($"unsupported protocol version {version}");
        var id = new Guid(r.ReadBytes(16));
        var count = r.ReadInt32();
        if (count < 0 || count > MaxCommands) throw new WireFormatException("invalid result count");
        var results = new List<RespValue>(Math.Min(count, 1024));
        for (var i = 0; i < count; i++) results.Add(ReadValue(r, 0));
        if (r.Remaining != 0) throw new WireFormatException("trailing bytes in response");
        return new ResponseEnvelope(id, results);
    }

    private static void WriteValue(BinaryWriter w, RespValue v)
    {
        w.Write((byte)v.Type);
        switch (v.Type)
        {
            case RespType.Status:
            case RespType.Error: WriteBytes(w, Encoding.UTF8.GetBytes(v.Text ?? string.Empty)); break;
            case RespType.Integer: w.Write(v.Integer); break;
            case RespType.Bulk: WriteBytes(w, v.Bytes!); break;
            case RespType.Null: break;
            case RespType.Array:
                w.Write(v.Items!.Count);
                foreach (var i in v.Items) WriteValue(w, i);
                break;
            default: throw new WireFormatException("unknown value type");
        }
    }

    private static RespValue ReadValue(ByteReader r, int depth)
    {
        if (depth > MaxDepth) throw new WireFormatException("nesting too deep");
        var type = (RespType)r.ReadByte();
        switch (type)
        {
            case RespType.Status: return RespValue.Status(Encoding.UTF8.GetString(r.ReadLengthPrefixed()));
            case RespType.Error: return RespValue.Error(Encoding.UTF8.GetString(r.ReadLengthPrefixed()));
            case RespType.Integer: return RespValue.Int(r.ReadInt64());
            case RespType.Bulk: return RespValue.Bulk(r.ReadLengthPrefixed());
            case RespType.Null: return RespValue.Nil;
            case RespType.Array:
                var n = r.ReadInt32();
                if (n < 0 || n > r.Remaining) throw new WireFormatException("invalid array length");
                var items = new RespValue[n];
                for (var i = 0; i < n; i++) items[i] = ReadValue(r, depth + 1);
                return RespValue.Arr(items);
            default: throw new WireFormatException($"unknown value type {(byte)type}");
        }
    }

    private static void WriteBytes(BinaryWriter w, byte[] b) { w.Write(b.Length); w.Write(b); }

    private sealed class ByteReader
    {
        private readonly byte[] _b;
        private int _p;
        public ByteReader(byte[] b) => _b = b;
        public int Remaining => _b.Length - _p;

        public byte ReadByte() { Need(1); return _b[_p++]; }
        public int ReadInt32() { Need(4); var v = BinaryPrimitives.ReadInt32LittleEndian(_b.AsSpan(_p)); _p += 4; return v; }
        public long ReadInt64() { Need(8); var v = BinaryPrimitives.ReadInt64LittleEndian(_b.AsSpan(_p)); _p += 8; return v; }

        public byte[] ReadBytes(int n)
        {
            Need(n);
            var r = new byte[n];
            Buffer.BlockCopy(_b, _p, r, 0, n);
            _p += n;
            return r;
        }

        public byte[] ReadLengthPrefixed() => ReadBytes(ReadInt32());

        private void Need(int n)
        {
            if (n < 0 || n > Remaining) throw new WireFormatException("truncated or malformed message");
        }
    }
}
