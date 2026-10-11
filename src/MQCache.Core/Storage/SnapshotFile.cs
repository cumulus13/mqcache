// File: src/MQCache.Core/Storage/SnapshotFile.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Atomic, checksummed on-disk snapshot format (MQDB1) for the keyspace.
// License: MIT

using System.Security.Cryptography;
using System.Text;

namespace MQCache.Core.Storage;

public sealed class SnapshotCorruptException : Exception
{
    public SnapshotCorruptException(string message) : base(message) { }
}

/// <summary>
/// Layout: "MQDB" | version(1) | savedAtMs(i64) | count(i32) | entries... | SHA-256 of all preceding bytes.
/// Saved to "path.tmp" then swapped in with File.Replace/Move so a crash never leaves a half-written file.
/// </summary>
public static class SnapshotFile
{
    private const byte Version = 1;
    private static readonly byte[] Magic = "MQDB"u8.ToArray();

    public static void Save(string path, IReadOnlyList<SnapEntry> entries, long nowMs)
    {
        var dir = Path.GetDirectoryName(Path.GetFullPath(path))!;
        Directory.CreateDirectory(dir);
        var tmp = path + ".tmp";
        using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 1 << 16))
        {
            using var w = new BinaryWriter(fs, Encoding.UTF8, leaveOpen: true);
            w.Write(Magic); w.Write(Version); w.Write(nowMs); w.Write(entries.Count);
            foreach (var e in entries)
            {
                WriteStr(w, e.Key); w.Write((byte)e.Kind); w.Write(e.ExpireAtMs);
                switch (e.Kind)
                {
                    case ValueKind.String: WriteBytes(w, (byte[])e.Payload); break;
                    case ValueKind.Hash:
                        var h = ((string Field, byte[] Value)[])e.Payload;
                        w.Write(h.Length);
                        foreach (var (f, v) in h) { WriteStr(w, f); WriteBytes(w, v); }
                        break;
                    case ValueKind.List:
                        var l = (byte[][])e.Payload;
                        w.Write(l.Length);
                        foreach (var v in l) WriteBytes(w, v);
                        break;
                    default:
                        var s = (string[])e.Payload;
                        w.Write(s.Length);
                        foreach (var m in s) WriteStr(w, m);
                        break;
                }
            }
            w.Flush();
            fs.Flush(true);
        }

        byte[] hash;
        using (var rs = new FileStream(tmp, FileMode.Open, FileAccess.Read, FileShare.Read))
            hash = SHA256.HashData(rs);
        using (var fs = new FileStream(tmp, FileMode.Append, FileAccess.Write, FileShare.None))
        {
            fs.Write(hash, 0, hash.Length);
            fs.Flush(true);
        }

        if (File.Exists(path)) File.Replace(tmp, path, path + ".bak");
        else File.Move(tmp, path);
    }

    public static IReadOnlyList<SnapEntry> Load(string path)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 16);
        if (fs.Length < Magic.Length + 1 + 8 + 4 + 32) throw new SnapshotCorruptException("file too short");
        var body = fs.Length - 32;

        using (var inc = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
        {
            var buf = new byte[1 << 16];
            long left = body;
            while (left > 0)
            {
                var n = fs.Read(buf, 0, (int)Math.Min(buf.Length, left));
                if (n <= 0) throw new SnapshotCorruptException("unexpected end of file");
                inc.AppendData(buf, 0, n);
                left -= n;
            }
            var expected = new byte[32];
            fs.ReadExactly(expected, 0, 32);
            if (!CryptographicOperations.FixedTimeEquals(inc.GetHashAndReset(), expected))
                throw new SnapshotCorruptException("checksum mismatch");
        }

        fs.Position = 0;
        using var r = new BinaryReader(fs, Encoding.UTF8, leaveOpen: true);
        if (!r.ReadBytes(4).AsSpan().SequenceEqual(Magic)) throw new SnapshotCorruptException("bad magic");
        var version = r.ReadByte();
        if (version != Version) throw new SnapshotCorruptException($"unsupported snapshot version {version}");
        r.ReadInt64();
        var count = r.ReadInt32();
        if (count < 0) throw new SnapshotCorruptException("negative entry count");
        var list = new List<SnapEntry>(Math.Min(count, 1 << 16));
        for (var i = 0; i < count; i++)
        {
            var key = ReadStr(r, fs, body);
            var kind = (ValueKind)r.ReadByte();
            var exp = r.ReadInt64();
            object payload;
            switch (kind)
            {
                case ValueKind.String: payload = ReadBytes(r, fs, body); break;
                case ValueKind.Hash:
                    var hn = Count(r, fs, body);
                    var h = new (string, byte[])[hn];
                    for (var j = 0; j < hn; j++) h[j] = (ReadStr(r, fs, body), ReadBytes(r, fs, body));
                    payload = h;
                    break;
                case ValueKind.List:
                    var ln = Count(r, fs, body);
                    var l = new byte[ln][];
                    for (var j = 0; j < ln; j++) l[j] = ReadBytes(r, fs, body);
                    payload = l;
                    break;
                case ValueKind.Set:
                    var sn = Count(r, fs, body);
                    var s = new string[sn];
                    for (var j = 0; j < sn; j++) s[j] = ReadStr(r, fs, body);
                    payload = s;
                    break;
                default: throw new SnapshotCorruptException($"unknown value kind {(byte)kind}");
            }
            list.Add(new SnapEntry(key, kind, exp, payload));
        }
        return list;
    }

    private static void WriteStr(BinaryWriter w, string s) => WriteBytes(w, Encoding.UTF8.GetBytes(s));
    private static void WriteBytes(BinaryWriter w, byte[] b) { w.Write(b.Length); w.Write(b); }

    private static int Count(BinaryReader r, FileStream fs, long body)
    {
        var n = r.ReadInt32();
        if (n < 0 || n > body - fs.Position) throw new SnapshotCorruptException("invalid collection length");
        return n;
    }

    private static byte[] ReadBytes(BinaryReader r, FileStream fs, long body)
    {
        var n = r.ReadInt32();
        if (n < 0 || n > body - fs.Position) throw new SnapshotCorruptException("invalid length");
        return r.ReadBytes(n);
    }

    private static string ReadStr(BinaryReader r, FileStream fs, long body) => Encoding.UTF8.GetString(ReadBytes(r, fs, body));
}
