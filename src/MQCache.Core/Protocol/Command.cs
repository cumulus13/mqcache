// File: src/MQCache.Core/Protocol/Command.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: A parsed command: upper-case name plus binary-safe arguments.
// License: MIT

using System.Globalization;
using System.Text;

namespace MQCache.Core.Protocol;

public sealed class Command
{
    public Command(string name, byte[][] args)
    {
        Name = name.ToUpperInvariant();
        Args = args;
    }

    public string Name { get; }
    public byte[][] Args { get; }
    public int Count => Args.Length;

    public static Command Of(params string[] parts)
    {
        if (parts.Length == 0) throw new ArgumentException("empty command", nameof(parts));
        return new Command(parts[0], parts.Skip(1).Select(p => Encoding.UTF8.GetBytes(p)).ToArray());
    }

    public static Command Of(string name, params byte[][] args) => new(name, args);

    /// <summary>Convenience for "NAME key binary-value" commands such as SET/APPEND/LPUSH.</summary>
    public static Command Of(string name, string key, byte[] value) => new(name, new[] { Encoding.UTF8.GetBytes(key), value });

    public string Str(int index) => Encoding.UTF8.GetString(Args[index]);

    public bool TryLong(int index, out long value) =>
        long.TryParse(Str(index), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out value);

    public override string ToString() => Name + (Args.Length == 0 ? "" : " " + string.Join(' ', Args.Select(a => Encoding.UTF8.GetString(a))));
}
