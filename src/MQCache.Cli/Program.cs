// File: src/MQCache.Cli/Program.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: mqcache-cli: interactive shell and one-shot command runner, subscribe mode and a small benchmark.
// License: MIT

using System.Diagnostics;
using System.Text;
using Experimental.System.Messaging;
using MQCache.Client;
using MQCache.Core.Protocol;

const string Usage = """
    mqcache-cli - client for mqcache-server (Redis-like cache over MSMQ)

    Usage: mqcache-cli [options] [command [args...]]

    Options:
      -e, --endpoint <spec>   . | host | os:host | tcp:10.0.0.5   (default ".")
      -q, --queue <name>      request queue name                  (default "mqcache")
      -a, --auth <password>   password (requirepass)
      -t, --timeout <ms>      per-attempt timeout                 (default 5000)
          --retries <n>       retries after timeout               (default 2)
          --priority <p>      lowest|verylow|low|normal|abovenormal|high|veryhigh|highest
          --no-tx             server queue is NOT transactional (otherwise auto-detected)
          --raw               raw output (no quotes/numbering)
      -h, --help

    Without a command an interactive shell starts. Extras: SUBSCRIBE/PSUBSCRIBE <ch...> (Ctrl+C to stop),
    BENCHMARK <requests> [threads], HELP, QUIT.
    """;

var o = new CacheClientOptions();
var raw = Console.IsOutputRedirected;
var cmdArgs = new List<string>();
for (var i = 0; i < args.Length; i++)
{
    string Next() => i + 1 < args.Length ? args[++i] : throw new ArgumentException($"missing value for {args[i]}");
    if (cmdArgs.Count > 0) { cmdArgs.Add(args[i]); continue; }
    switch (args[i])
    {
        case "-h" or "--help": Console.WriteLine(Usage); return 0;
        case "-e" or "--endpoint": o.Endpoint = Next(); break;
        case "-q" or "--queue": o.QueueName = Next(); break;
        case "-a" or "--auth": o.Password = Next(); break;
        case "-t" or "--timeout": o.Timeout = TimeSpan.FromMilliseconds(long.Parse(Next())); break;
        case "--retries": o.Retries = int.Parse(Next()); break;
        case "--priority": o.Priority = Enum.Parse<MessagePriority>(Next(), true); break;
        case "--no-tx": o.Transactional = false; break;
        case "--raw": raw = true; break;
        default: cmdArgs.Add(args[i]); break;
    }
}

try
{
    using var client = new CacheClient(o);
    if (cmdArgs.Count > 0) return Run(client, cmdArgs.ToArray(), raw) ? 0 : 1;

    Console.CancelKeyPress += (_, e) => { e.Cancel = true; };
    var prompt = $"{(o.Endpoint == "." ? "local" : o.Endpoint)}/{o.QueueName}> ";
    while (true)
    {
        Console.Write(prompt);
        var line = Console.ReadLine();
        if (line is null) break;
        var parts = Split(line);
        if (parts.Count == 0) continue;
        var name = parts[0].ToUpperInvariant();
        if (name is "QUIT" or "EXIT") break;
        if (name == "CLEAR") { Console.Clear(); continue; }
        if (name == "HELP") { Console.WriteLine(Usage); continue; }
        Run(client, parts.ToArray(), raw);
    }
    return 0;
}
catch (Exception ex) when (ex is CacheException or InvalidOperationException or ArgumentException or FormatException)
{
    Console.Error.WriteLine("error: " + ex.Message);
    return 1;
}

static bool Run(CacheClient client, string[] parts, bool raw)
{
    try
    {
        var name = parts[0].ToUpperInvariant();
        if (name is "SUBSCRIBE" or "PSUBSCRIBE") return Subscribe(client, name, parts.Skip(1).ToArray());
        if (name == "BENCHMARK") return Benchmark(client, parts);
        var reply = client.Execute(Command.Of(parts));
        Print(reply, raw, 0);
        return !reply.IsError;
    }
    catch (CacheException ex)
    {
        Console.WriteLine("(error) " + ex.Message);
        return false;
    }
}

static bool Subscribe(CacheClient client, string kind, string[] names)
{
    if (names.Length == 0) { Console.WriteLine("(error) usage: " + kind + " <name> [name...]"); return false; }
    using var sub = kind == "SUBSCRIBE" ? client.Subscribe(names) : client.PSubscribe(names);
    Console.WriteLine($"Subscribed to {string.Join(", ", names)}. Press Ctrl+C to stop.");
    using var cts = new CancellationTokenSource();
    ConsoleCancelEventHandler h = (_, e) => { e.Cancel = true; cts.Cancel(); };
    Console.CancelKeyPress += h;
    try
    {
        foreach (var m in sub.ReadAll(cts.Token))
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] {m.Channel}{(m.Pattern is null ? "" : " (" + m.Pattern + ")")}: {m.PayloadText}");
    }
    finally { Console.CancelKeyPress -= h; }
    return true;
}

static bool Benchmark(CacheClient client, string[] parts)
{
    var total = parts.Length > 1 ? int.Parse(parts[1]) : 1000;
    var threads = parts.Length > 2 ? int.Parse(parts[2]) : 4;
    var latencies = new double[total];
    var next = -1;
    var errors = 0;
    var sw = Stopwatch.StartNew();
    Parallel.For(0, threads, _ =>
    {
        int i;
        while ((i = Interlocked.Increment(ref next)) < total)
        {
            var t0 = Stopwatch.GetTimestamp();
            try
            {
                client.Set("bench:" + i, "x");
                client.GetString("bench:" + i);
            }
            catch (CacheException) { Interlocked.Increment(ref errors); }
            latencies[i] = Stopwatch.GetElapsedTime(t0).TotalMilliseconds;
        }
    });
    sw.Stop();
    client.Delete(Enumerable.Range(0, Math.Min(total, 1000)).Select(i => "bench:" + i).ToArray());
    Array.Sort(latencies);
    double P(double p) => latencies[Math.Min(total - 1, (int)(total * p))];
    Console.WriteLine($"{total} SET+GET pairs, {threads} threads, {sw.Elapsed.TotalSeconds:0.00}s => {total * 2 / sw.Elapsed.TotalSeconds:0} ops/s, errors={errors}");
    Console.WriteLine($"pair latency ms: p50={P(.5):0.00} p95={P(.95):0.00} p99={P(.99):0.00} max={latencies[^1]:0.00}");
    return errors == 0;
}

static void Print(RespValue v, bool raw, int indent)
{
    var pad = new string(' ', indent);
    switch (v.Type)
    {
        case RespType.Status: Console.WriteLine(pad + v.Text); break;
        case RespType.Error: Console.WriteLine(pad + (raw ? "" : "(error) ") + v.Text); break;
        case RespType.Integer: Console.WriteLine(pad + (raw ? v.Integer.ToString() : "(integer) " + v.Integer)); break;
        case RespType.Null: Console.WriteLine(pad + (raw ? "" : "(nil)")); break;
        case RespType.Bulk:
            var text = Encoding.UTF8.GetString(v.Bytes!);
            Console.WriteLine(pad + (raw ? text : Quote(text))); break;
        default:
            if (v.Items!.Count == 0) { Console.WriteLine(pad + (raw ? "" : "(empty array)")); break; }
            for (var i = 0; i < v.Items.Count; i++)
            {
                var item = v.Items[i];
                if (item.Type == RespType.Array) { Console.WriteLine(pad + (raw ? "" : $"{i + 1})")); Print(item, raw, indent + 3); }
                else
                {
                    if (!raw) Console.Write(pad + $"{i + 1}) ");
                    Print(item, raw, raw ? indent : 0);
                }
            }
            break;
    }
}

static string Quote(string s)
{
    var sb = new StringBuilder("\"");
    foreach (var c in s)
        sb.Append(c switch { '"' => "\\\"", '\\' => "\\\\", '\n' => "\\n", '\r' => "\\r", '\t' => "\\t", < ' ' => $"\\x{(int)c:x2}", _ => c.ToString() });
    return sb.Append('"').ToString();
}

static List<string> Split(string line)
{
    var res = new List<string>();
    var sb = new StringBuilder();
    char quote = '\0';
    var has = false;
    for (var i = 0; i < line.Length; i++)
    {
        var c = line[i];
        if (quote != '\0')
        {
            if (c == '\\' && quote == '"' && i + 1 < line.Length)
            {
                var n = line[++i];
                sb.Append(n switch { 'n' => '\n', 'r' => '\r', 't' => '\t', _ => n });
            }
            else if (c == quote) quote = '\0';
            else sb.Append(c);
        }
        else if (c is '"' or '\'') { quote = c; has = true; }
        else if (char.IsWhiteSpace(c))
        {
            if (has || sb.Length > 0) { res.Add(sb.ToString()); sb.Clear(); has = false; }
        }
        else sb.Append(c);
    }
    if (quote != '\0') throw new ArgumentException("unbalanced quotes");
    if (has || sb.Length > 0) res.Add(sb.ToString());
    return res;
}
