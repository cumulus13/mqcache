// File: src/MQCache.Server/Hosting/Program.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: mqcache-server entry point: console or Windows Service, redis.conf style config plus --key value overrides.
// License: MIT

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MQCache.Core;
using MQCache.Core.Config;
using MQCache.Server;
using MQCache.Server.Hosting;

const string Usage = """
    mqcache-server - Redis-like cache over Microsoft Message Queuing (Windows only)

    Usage:
      mqcache-server [path\to\mqcache.conf] [--key value ...]
      mqcache-server --help | --version | --test-config [conf]

    Examples:
      mqcache-server conf\mqcache.conf --workers 8 --maxmemory 512mb
      sc.exe create MQCache binPath= "C:\mqcache\mqcache-server.exe C:\mqcache\mqcache.conf" start= auto
    """;

if (args.Length > 0 && args[0] is "--help" or "-h" or "/?") { Console.WriteLine(Usage); return 0; }
if (args.Length > 0 && args[0] is "--version")
{
    Console.WriteLine(typeof(CacheServer).Assembly.GetName().Version);
    return 0;
}

var testOnly = args.Length > 0 && args[0] == "--test-config";
var rest = args.Skip(testOnly ? 1 : 0).ToList();
string? confPath = rest.Count > 0 && !rest[0].StartsWith("--", StringComparison.Ordinal) ? rest[0] : null;
if (confPath is null)
{
    var candidate = Path.Combine(AppContext.BaseDirectory, "mqcache.conf");
    if (File.Exists(candidate)) confPath = candidate;
}

FileLog? log = null;
try
{
    var cfg = confPath is null ? new ServerConfig() : ConfigLoader.Load(confPath);
    ConfigLoader.ApplyOverrides(cfg, rest.Skip(confPath is not null && rest.Count > 0 && rest[0] == confPath ? 1 : 0).ToList());
    cfg.Validate();

    if (testOnly)
    {
        Console.WriteLine($"configuration OK ({confPath ?? "defaults"})");
        return 0;
    }

    var isService = WindowsServiceHelpers.IsWindowsService();
    var logFile = cfg.LogFile ?? (isService ? Path.Combine(AppContext.BaseDirectory, "logs", "mqcache-server.log") : null);
    log = new FileLog(cfg.LogLevel, logFile, console: !isService);
    log.Info($"mqcache-server starting (config: {confPath ?? "defaults"})");

    var builder = Host.CreateApplicationBuilder();
    builder.Services.AddWindowsService(o => o.ServiceName = "MQCache");
    builder.Services.AddSingleton<ILog>(log);
    builder.Services.AddSingleton(cfg);
    builder.Services.AddHostedService<CacheServerHostedService>();
    builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(30));
    await builder.Build().RunAsync();
    return 0;
}
catch (ConfigException ex)
{
    Console.Error.WriteLine("configuration error: " + ex.Message);
    return 2;
}
catch (Exception ex)
{
    if (log is not null) log.Error("fatal", ex); else Console.Error.WriteLine(ex);
    return 1;
}
finally
{
    log?.Dispose();
}
