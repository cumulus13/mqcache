# Installation

## Requirements

* Windows 10/11 or Windows Server 2016+ (x64 or ARM64).
* The **Microsoft Message Queue (MSMQ)** feature and the *Message Queuing* service running.
* For building: .NET SDK 8.0. Released binaries are self-contained.

## 1. Install MSMQ

```powershell
# Windows client (10/11)
Enable-WindowsOptionalFeature -Online -FeatureName MSMQ-Server -All
# Windows Server
Install-WindowsFeature MSMQ-Server
Get-Service MSMQ   # must be Running
```

Check that the queue manager works: `Get-MsmqQueue` (module `MSMQ`) or Computer Management > Services and Applications > Message Queuing.

## 2. Install the server as a service

Download the zip for your platform from the GitHub release, extract it, and from an elevated PowerShell:

```powershell
.\install-service.ps1 -EnableMsmq -OpenFirewall
```

The script copies binaries to `%ProgramFiles%\MQCache`, writes `%ProgramData%\MQCache\mqcache.conf` (never overwritten on upgrade), registers the service `MQCache` (delayed auto-start, depends on MSMQ, restart on failure), and optionally opens the MSMQ firewall ports for remote clients. Default account: `NT AUTHORITY\NetworkService`.

Manual alternative:

```powershell
sc.exe create MQCache binPath= "\"C:\mqcache\mqcache-server.exe\" \"C:\mqcache\mqcache.conf\"" start= delayed-auto depend= MSMQ
```

Run in a console for testing: `mqcache-server.exe conf\mqcache.conf` (Ctrl+C stops it cleanly and saves a snapshot).

Validate a config without starting: `mqcache-server.exe --test-config conf\mqcache.conf`.

## 3. Try it

```powershell
mqcache-cli.exe PING
mqcache-cli.exe SET greeting hello EX 60
mqcache-cli.exe GET greeting
```

## Remote clients

* Server and client machines both need MSMQ. Use `-e tcp:<server-ip>` (recommended) or `-e os:<server-name>`.
* Firewall (inbound on the **server**, and on the **client** for replies): TCP 1801, TCP 135/2103/2105 (RPC, remote read), UDP 3527. Multicast additionally needs the chosen group/port.
* The server answers to the client's **reply queue**, so the client must be reachable: set `ReplyHost` (name or IP) when the machine name does not resolve from the server; for `tcp:` use `ReplyScheme = "tcp"` with an IP.
* Workgroup (non-domain) setups: MSMQ then runs without AD; keep `queue-authenticate no` and rely on `requirepass` plus network controls.

## Upgrading

Stop the service, replace the executables, start the service. The snapshot format is versioned; a snapshot written by an older version is read by newer ones. Changing `queue-transactional` requires deleting the queue after it is drained.

## Uninstall

`.\uninstall-service.ps1 [-RemoveData]`. Queues `private$\mqcache*` are kept; delete them in Computer Management if wanted.

## Build from source

```powershell
git clone https://github.com/cumulus13/MQCache.git
cd MQCache
dotnet build -c Release
dotnet test tests/MQCache.Core.Tests
dotnet run --project src/MQCache.Server -- conf/mqcache.conf
```
