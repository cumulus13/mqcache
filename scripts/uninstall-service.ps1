#Requires -RunAsAdministrator
[CmdletBinding()]
param([string]$ServiceName = 'MQCache', [string]$InstallDir = "$env:ProgramFiles\MQCache", [switch]$RemoveData)
$ErrorActionPreference = 'Stop'
if (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue) {
    Stop-Service $ServiceName -ErrorAction SilentlyContinue
    sc.exe delete $ServiceName | Out-Null
}
Remove-Item -Recurse -Force $InstallDir -ErrorAction SilentlyContinue
if ($RemoveData) { Remove-Item -Recurse -Force "$env:ProgramData\MQCache" -ErrorAction SilentlyContinue }
Write-Host "Removed '$ServiceName'. MSMQ queues (private`$\mqcache*) are left in place; delete them in Computer Management if wanted."
