<#
.SYNOPSIS  Installs MQCache as a Windows Service.
.EXAMPLE   .\install-service.ps1 -EnableMsmq -OpenFirewall
.NOTES     Run from the extracted release folder in an elevated PowerShell.
#>
#Requires -RunAsAdministrator
[CmdletBinding()]
param(
    [string]$InstallDir = "$env:ProgramFiles\MQCache",
    [string]$DataDir = "$env:ProgramData\MQCache",
    [string]$ServiceName = 'MQCache',
    [string]$ServiceAccount = 'NT AUTHORITY\NetworkService',
    [switch]$EnableMsmq,
    [switch]$OpenFirewall
)
$ErrorActionPreference = 'Stop'
$src = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not (Test-Path (Join-Path $src 'mqcache-server.exe'))) { $src = Split-Path -Parent $src }
if (-not (Test-Path (Join-Path $src 'mqcache-server.exe'))) { throw 'mqcache-server.exe not found next to the script.' }

# 1. MSMQ
$svc = Get-Service -Name MSMQ -ErrorAction SilentlyContinue
if (-not $svc) {
    if (-not $EnableMsmq) { throw "MSMQ is not installed. Re-run with -EnableMsmq (installs Windows feature 'MSMQ-Server')." }
    Write-Host 'Enabling Windows feature MSMQ-Server...'
    Enable-WindowsOptionalFeature -Online -FeatureName MSMQ-Server -All -NoRestart | Out-Null
    $svc = Get-Service -Name MSMQ
}
if ($svc.Status -ne 'Running') { Start-Service MSMQ }

# 2. Files + config (existing config is never overwritten)
New-Item -ItemType Directory -Force -Path $InstallDir, $DataDir, "$DataDir\logs" | Out-Null
Copy-Item (Join-Path $src 'mqcache-server.exe'), (Join-Path $src 'mqcache-cli.exe') -Destination $InstallDir -Force
$conf = Join-Path $DataDir 'mqcache.conf'
if (-not (Test-Path $conf)) {
    $template = Join-Path $src 'mqcache.conf'
    $text = (Get-Content $template -Raw) -replace '(?m)^dir\s+.*$', ('dir                      ' + ($DataDir -replace '\\', '/') + '/data') `
                                         -replace '(?m)^# logfile.*$', ('logfile                  ' + "$DataDir\logs\mqcache.log")
    Set-Content -Path $conf -Value $text -Encoding UTF8
}
icacls $DataDir /grant "${ServiceAccount}:(OI)(CI)M" | Out-Null

# 3. Service
if (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue) {
    Stop-Service $ServiceName -ErrorAction SilentlyContinue
    sc.exe delete $ServiceName | Out-Null
    Start-Sleep -Seconds 2
}
$bin = "`"$InstallDir\mqcache-server.exe`" `"$conf`""
sc.exe create $ServiceName binPath= $bin start= delayed-auto obj= $ServiceAccount DisplayName= 'MQCache (Redis-like cache over MSMQ)' depend= MSMQ | Out-Null
sc.exe description $ServiceName 'In-memory cache server using Microsoft Message Queuing as transport.' | Out-Null
sc.exe failure $ServiceName reset= 86400 actions= restart/5000/restart/5000/restart/30000 | Out-Null

# 4. Firewall (only needed for remote clients)
if ($OpenFirewall) {
    New-NetFirewallRule -DisplayName 'MSMQ TCP 1801' -Direction Inbound -Protocol TCP -LocalPort 1801 -Action Allow -ErrorAction SilentlyContinue | Out-Null
    New-NetFirewallRule -DisplayName 'MSMQ RPC (135,2103,2105)' -Direction Inbound -Protocol TCP -LocalPort 135, 2103, 2105 -Action Allow -ErrorAction SilentlyContinue | Out-Null
    New-NetFirewallRule -DisplayName 'MSMQ UDP 3527' -Direction Inbound -Protocol UDP -LocalPort 3527 -Action Allow -ErrorAction SilentlyContinue | Out-Null
}

Start-Service $ServiceName
Write-Host "Installed and started '$ServiceName'. Config: $conf" -ForegroundColor Green
Write-Host "Try: & '$InstallDir\mqcache-cli.exe' PING"
