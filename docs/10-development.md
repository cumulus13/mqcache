# Development, testing and releases

## Repository layout

```
src/MQCache.Core       platform-independent: store, commands, codec, config, snapshots   (net8.0)
src/MQCache.Messaging  MSMQ plumbing: endpoints, queue admin, message factory          (net8.0-windows)
src/MQCache.Server     mqcache-server: workers, router, hosting/service                 (net8.0-windows)
src/MQCache.Client     MQCache.Client NuGet package                                  (net8.0-windows)
src/MQCache.Cli        mqcache-cli                                                      (net8.0-windows)
tests/MQCache.Core.Tests          unit tests (run anywhere)
tests/MQCache.IntegrationTests    real MSMQ (auto-skipped when MSMQ is missing)
conf/mqcache.conf  scripts/  docs/  .github/workflows/
```

All MSMQ-specific code is isolated in `Messaging`/`Server`/`Client`; the cache semantics live in `Core` and are unit-tested without MSMQ.

## Build and test

```powershell
dotnet build -c Release
dotnet test tests/MQCache.Core.Tests            # fast, no MSMQ needed
dotnet test tests/MQCache.IntegrationTests      # needs the MSMQ feature + service
```

## Adding a command

1. Add the operation to `KeyspaceStore` (keep it atomic under the store lock; update `_dirty` and size accounting).
2. Register it in `CommandProcessor.Register()` with its arity and whether it can **add memory** (`write = true`).
3. Add unit tests in `CoreTests.cs`; document it in chapter 5.

MSMQ-aware commands (anything needing queues or the sender's reply queue) go in `ServerCommandRouter`.

## Continuous integration

`.github/workflows/ci.yml` (push/PR): restore, format check (informational), build, Core unit tests with coverage, integration tests on a Windows runner with MSMQ enabled (best effort, non-blocking), NuGet pack, single-file publish smoke test, documentation build.

## Releasing

1. Update `CHANGELOG.md` with a `## [x.y.z] - date` section.
2. Tag and push: `git tag v1.0.0 && git push origin v1.0.0`.
3. `release.yml` verifies the tests, builds `win-x64` and `win-arm64` single-file self-contained zips (server, CLI, config, install scripts), packs `MQCache.Client`, builds the documentation in md/html/docx/pdf/epub, writes `SHA256SUMS.txt`, creates the GitHub Release (notes from the changelog) and pushes to NuGet when the `NUGET_API_KEY` secret exists.

## Building the documentation

```powershell
choco install pandoc
pwsh docs/build-docs.ps1                          # md, html, docx, pdf, epub into dist/docs
pwsh docs/build-docs.ps1 -Formats md,docx -Out out
```

Chapters are `docs/NN-*.md`; they are concatenated in order. PDF uses xelatex, else wkhtmltopdf, else headless Edge/Chrome. Single chapters can be exported with plain pandoc: `pandoc docs/05-commands.md -o commands.docx`.

## Compatibility note

The MSMQ API comes from `Experimental.System.Messaging` (community port, Ms-RL). If Microsoft ships an official .NET MSMQ client, only `MQCache.Messaging`, `Server` and `Client` need their `using` lines changed.
