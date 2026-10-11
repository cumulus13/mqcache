// File: src/MQCache.Core/Config/ConfigLoader.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Parses redis.conf-style files (key value ...; # comments; double-quoted strings) and --key value overrides.
// License: MIT

using System.Text;

namespace MQCache.Core.Config;

public static class ConfigLoader
{
    public static ServerConfig Load(string path)
    {
        if (!File.Exists(path)) throw new ConfigException($"config file not found: {path}");
        return FromText(File.ReadAllText(path), path);
    }

    public static ServerConfig FromText(string text, string source = "<memory>")
    {
        var cfg = new ServerConfig();
        Apply(cfg, text, source);
        cfg.Validate();
        return cfg;
    }

    public static void Apply(ServerConfig cfg, string text, string source)
    {
        var lineNo = 0;
        foreach (var raw in text.Split('\n'))
        {
            lineNo++;
            List<string> tokens;
            try { tokens = Tokenize(raw); }
            catch (ConfigException ex) { throw new ConfigException($"{source}:{lineNo}: {ex.Message}"); }
            if (tokens.Count == 0) continue;
            try { cfg.Set(tokens[0], tokens.Skip(1).ToList()); }
            catch (ConfigException ex) { throw new ConfigException($"{source}:{lineNo}: {ex.Message}"); }
        }
    }

    /// <summary>Applies "--key value [value]" style overrides (values run until the next "--" token).</summary>
    public static void ApplyOverrides(ServerConfig cfg, IReadOnlyList<string> args)
    {
        for (var i = 0; i < args.Count;)
        {
            if (!args[i].StartsWith("--", StringComparison.Ordinal))
                throw new ConfigException($"unexpected argument '{args[i]}'");
            var key = args[i][2..];
            var values = new List<string>();
            i++;
            while (i < args.Count && !args[i].StartsWith("--", StringComparison.Ordinal)) values.Add(args[i++]);
            cfg.Set(key, values);
        }
    }

    public static List<string> Tokenize(string line)
    {
        var tokens = new List<string>();
        var i = 0;
        while (i < line.Length)
        {
            var c = line[i];
            if (char.IsWhiteSpace(c)) { i++; continue; }
            if (c == '#') break;
            var sb = new StringBuilder();
            if (c == '"')
            {
                i++;
                var closed = false;
                while (i < line.Length)
                {
                    var d = line[i++];
                    if (d == '\\' && i < line.Length) { sb.Append(line[i++]); continue; }
                    if (d == '"') { closed = true; break; }
                    sb.Append(d);
                }
                if (!closed) throw new ConfigException("unterminated quoted string");
            }
            else
            {
                while (i < line.Length && !char.IsWhiteSpace(line[i])) sb.Append(line[i++]);
            }
            tokens.Add(sb.ToString());
        }
        return tokens;
    }
}
