// File: src/MQCache.Core/Protocol/RespValue.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Redis-style reply values (status, error, integer, bulk, null, array).
// License: MIT

using System.Text;

namespace MQCache.Core.Protocol;

public enum RespType : byte { Status = 1, Error = 2, Integer = 3, Bulk = 4, Null = 5, Array = 6 }

/// <summary>Immutable reply value modelled after the Redis RESP type system.</summary>
public sealed class RespValue
{
    public static readonly RespValue Ok = new(RespType.Status, text: "OK");
    public static readonly RespValue Pong = new(RespType.Status, text: "PONG");
    public static readonly RespValue Nil = new(RespType.Null);
    public static readonly RespValue Zero = new(RespType.Integer, integer: 0);
    public static readonly RespValue One = new(RespType.Integer, integer: 1);
    public static readonly RespValue EmptyArray = new(RespType.Array, items: Array.Empty<RespValue>());

    private RespValue(RespType type, string? text = null, long integer = 0, byte[]? bytes = null,
        IReadOnlyList<RespValue>? items = null)
    {
        Type = type; Text = text; Integer = integer; Bytes = bytes; Items = items;
    }

    public RespType Type { get; }
    public string? Text { get; }
    public long Integer { get; }
    public byte[]? Bytes { get; }
    public IReadOnlyList<RespValue>? Items { get; }

    public bool IsError => Type == RespType.Error;
    public bool IsNil => Type == RespType.Null;

    public static RespValue Status(string text) => new(RespType.Status, text: text);
    public static RespValue Error(string text) => new(RespType.Error, text: text);
    public static RespValue Int(long value) => value == 0 ? Zero : value == 1 ? One : new(RespType.Integer, integer: value);
    public static RespValue Bulk(byte[]? value) => value is null ? Nil : new(RespType.Bulk, bytes: value);
    public static RespValue Bulk(string? value) => value is null ? Nil : new(RespType.Bulk, bytes: Encoding.UTF8.GetBytes(value));
    public static RespValue Arr(IEnumerable<RespValue> items) => new(RespType.Array, items: items.ToArray());
    public static RespValue BulkArray(IEnumerable<string> items) => Arr(items.Select(s => Bulk(s)));
    public static RespValue BulkArray(IEnumerable<byte[]?> items) => Arr(items.Select(b => Bulk(b)));

    /// <summary>Text view of status/error/bulk/integer values; null for everything else.</summary>
    public string? AsString() => Type switch
    {
        RespType.Status or RespType.Error => Text,
        RespType.Bulk => Encoding.UTF8.GetString(Bytes!),
        RespType.Integer => Integer.ToString(System.Globalization.CultureInfo.InvariantCulture),
        _ => null,
    };

    public override string ToString() => Type switch
    {
        RespType.Status => Text!,
        RespType.Error => "(error) " + Text,
        RespType.Integer => "(integer) " + Integer,
        RespType.Bulk => Encoding.UTF8.GetString(Bytes!),
        RespType.Null => "(nil)",
        _ => $"(array[{Items!.Count}])",
    };
}
