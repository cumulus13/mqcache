# Changelog

All notable changes are documented here. Format: [Keep a Changelog](https://keepachangelog.com/), versioning: [SemVer](https://semver.org/).

## [1.0.1] - 2026-10-11

### Fixed
- Build: queue ACLs now use `SetPermissions(MessageQueueAccessControlEntry)`, the overload that exists in `Experimental.System.Messaging` 1.2.0.
- Docs build script: chapter discovery no longer relies on bracket wildcards in `-Filter` (unsupported by the Windows file system provider).
- Packaging: `MQCache.Core` and `MQCache.Messaging` are packed alongside `MQCache.Client`, so the client package restores from NuGet; server and CLI are not packable.
- Pinned the SDK to 8.0.x via `global.json`.

## [1.0.0] - 2026-10-11

### Added
- Server (`mqcache-server`) as console app and Windows Service with `redis.conf`-style configuration and CLI overrides.
- In-memory keyspace: strings, hashes, lists, sets, TTL, exact LRU, `maxmemory` with `noeviction`/`allkeys-lru`/`volatile-lru`/`volatile-ttl`.
- MSMQ transport: transactional/non-transactional request queue, persistent messages, priorities, TTL, dead-letter, journal, acknowledgments, poison queue, ACLs, optional multicast publishing.
- Idempotent retries via request-id cache; atomic pipelines.
- Pub/Sub with lease-based subscriptions.
- Snapshot persistence (atomic, SHA-256 checked) with `save` rules, `SAVE`/`BGSAVE`.
- `mqcache-cli` (interactive, one-shot, subscribe, benchmark) and `MQCache.Client` NuGet package.
- Documentation (11 chapters) with pandoc export script (md, html, docx, pdf, epub).
- GitHub workflows: CI, documentation, release (win-x64/win-arm64 zips, NuGet, checksums).
