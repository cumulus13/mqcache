# MQCache

A **Redis-like cache server and client that use Microsoft Message Queuing (MSMQ) as the transport**. Windows only (MSMQ is a Windows technology). Written in C# / .NET 8.

[![CI](https://github.com/cumulus13/MQCache/actions/workflows/ci.yml/badge.svg)](https://github.com/cumulus13/MQCache/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)


<p align="center">
  <img src="https://raw.githubusercontent.com/cumulus13/mqcache/master/icon.png" alt="Logo" width="320">
</p>


* Redis-style commands: strings, hashes, lists, sets, TTL, `INCR`, `KEYS`, pipelines, pub/sub, `INFO`, `CONFIG`
* `mqcache-server` (console or Windows Service) with a `redis.conf`-style **config file**
* `mqcache-cli` interactive shell + `MQCache.Client` NuGet library
* Uses MSMQ features on purpose: transactional queues, persistent messages, priorities, TTL, dead-letter, journal, acknowledgments, selective receive, poison handling, ACLs, optional multicast (see [feature matrix](docs/02-msmq-feature-matrix.md))
* **Python SDK** (`sdk/python`, `pip install mqcache`): same semantics through MSMQ COM; wire codec verified byte for byte against the .NET reference
* Idempotent retries (request-id dedupe), snapshots with checksum, LRU eviction, memory limit, password, structured logs
* Complete documentation exportable to **PDF / DOCX / HTML / EPUB / Markdown**; GitHub workflows for CI, build and release

[![Architecture](https://github.com/cumulus13/mqcache/blob/384a181042b48fddbf2c6e1ca6526ab48c8283ee/architecture1.png)](https://github.com/cumulus13/mqcache/blob/384a181042b48fddbf2c6e1ca6526ab48c8283ee/architecture1.png)

## Quick start

```powershell
Enable-WindowsOptionalFeature -Online -FeatureName MSMQ-Server -All     # once, then make sure the MSMQ service runs
dotnet run --project src/MQCache.Server -- conf/mqcache.conf           # terminal 1
dotnet run --project src/MQCache.Cli -- SET greeting hello EX 60      # terminal 2
dotnet run --project src/MQCache.Cli -- GET greeting
```

```csharp
using var cache = CacheClient.Connect("tcp:10.0.0.5");      // or "." for the local machine
cache.Set("user:1", "alice", ttl: TimeSpan.FromMinutes(5));
Console.WriteLine(cache.GetString("user:1"));
cache.Pipeline(new[] { Command.Of("SET", "a", "1"), Command.Of("INCR", "a") });   // one MSMQ message
using var sub = cache.Subscribe("news"); cache.Publish("news", "hello");
```

## Documentation

| | |
|---|---|
| [01 Introduction](docs/01-introduction.md) | design, semantics, trade-offs |
| [02 MSMQ feature matrix](docs/02-msmq-feature-matrix.md) | which MSMQ features are used and how |
| [03 Installation](docs/03-installation.md) | MSMQ, service, firewall, remote clients |
| [04 Configuration](docs/04-configuration.md) | every `mqcache.conf` directive |
| [05 Commands](docs/05-commands.md) | command reference |
| [06 Client library](docs/06-client-library.md), [07 CLI](docs/07-cli.md) | usage |
| [08 Protocol](docs/08-protocol.md) | wire format for other clients |
| [09 Operations](docs/09-operations.md) | monitoring, backup, security, troubleshooting |
| [10 Development](docs/10-development.md), [11 FAQ](docs/11-faq.md) | build, test, release |
| [12 Python SDK](docs/12-python-sdk.md) | usage, differences, writing other SDKs |

A pre-built [PDF](docs/MQCache-documentation.pdf) is included. Export: `pwsh docs/build-docs.ps1` (needs [pandoc](https://pandoc.org)) writes `dist/docs/mqcache-docs.{md,html,docx,pdf,epub}`.

## Honest expectations

Latency and throughput are bounded by MSMQ, not by the cache; use Redis when you need raw speed. It is a cache: data between snapshots is lost on a crash. MSMQ messages are limited to 4 MiB. See [Operations > Known limitations](docs/09-operations.md).

## License

MIT, see [LICENSE](LICENSE). The MSMQ API is provided by the community package `Experimental.System.Messaging` (Ms-RL), referenced as a dependency.

## 👤 Author

[Hadi Cahyadi](mailto:cumulus13@gmail.com)


[![Buy Me a Coffee](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://www.buymeacoffee.com/cumulus13)

[![Donate via Ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/cumulus13)

[Support me on Patreon](https://www.patreon.com/cumulus13)
