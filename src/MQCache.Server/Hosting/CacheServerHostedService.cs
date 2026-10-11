// File: src/MQCache.Server/Hosting/CacheServerHostedService.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Bridges CacheServer into the generic host (console Ctrl+C and Windows Service lifetime).
// License: MIT

using Microsoft.Extensions.Hosting;
using MQCache.Core;
using MQCache.Core.Config;

namespace MQCache.Server.Hosting;

public sealed class CacheServerHostedService : IHostedService
{
    private readonly CacheServer _server;

    public CacheServerHostedService(ServerConfig cfg, ILog log) => _server = new CacheServer(cfg, log);

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _server.Start();
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _server.Dispose();
        return Task.CompletedTask;
    }
}
