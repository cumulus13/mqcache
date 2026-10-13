<#
.SYNOPSIS  Builds the documentation into one or more formats using pandoc.
.EXAMPLE   pwsh docs/build-docs.ps1 -Formats md,html,docx,pdf,epub
.NOTES     Requires pandoc. PDF uses the first available engine: xelatex, wkhtmltopdf, or headless Edge/Chrome (via the HTML build).
#>
[CmdletBinding()]
param(
    [string[]]$Formats = @('md', 'html', 'docx', 'pdf', 'epub'),
    [string]$Out = (Join-Path $PSScriptRoot '..\dist\docs'),
    [string]$Version = 'dev'
)
$ErrorActionPreference = 'Stop'
if (-not (Get-Command pandoc -ErrorAction SilentlyContinue)) { throw 'pandoc not found. Install from https://pandoc.org/installing.html (choco install pandoc).' }

# Absolute path: headless Edge/Chrome resolve a relative --print-to-pdf path against their own working directory.
$Out = (New-Item -ItemType Directory -Force -Path $Out).FullName
$chapters = Get-ChildItem -Path $PSScriptRoot -File | Where-Object { $_.Name -match '^\d{2}-.*\.md$' } | Sort-Object Name
if (-not $chapters) { throw 'no chapters found (docs/NN-*.md)' }

$meta = @"
---
title: "MQCache - Redis-like cache over Microsoft Message Queuing"
subtitle: "Version $Version"
author: "Hadi Cahyadi"
date: "$(Get-Date -Format 'yyyy-MM-dd')"
lang: en
---

"@
$combined = Join-Path $Out 'mqcache-docs.md'
$body = ($chapters | ForEach-Object { (Get-Content $_.FullName -Raw).TrimEnd() + "`n`n" }) -join ''
Set-Content -Path $combined -Value ($meta + $body) -Encoding UTF8

# pandoc >= 3.8 renamed --highlight-style to --syntax-highlighting
$helpText = (& pandoc --help) -join "`n"
$highlight = if ($helpText -match '--syntax-highlighting') { '--syntax-highlighting=tango' } else { '--highlight-style=tango' }
$common = @('--from', 'markdown+pipe_tables+fenced_code_blocks', '--toc', '--toc-depth=3', '--number-sections', $highlight)
$produced = @()

foreach ($f in $Formats) {
    switch ($f.ToLower()) {
        'md'   { $produced += $combined }
        'html' {
            $o = Join-Path $Out 'mqcache-docs.html'
            pandoc $combined @common --standalone --embed-resources --css (Join-Path $PSScriptRoot 'style.css') --metadata pagetitle='MQCache docs' -o $o
            $produced += $o
        }
        'docx' { $o = Join-Path $Out 'mqcache-docs.docx'; pandoc $combined @common -o $o; $produced += $o }
        'epub' { $o = Join-Path $Out 'mqcache-docs.epub'; pandoc $combined @common -o $o; $produced += $o }
        'pdf'  {
            $o = Join-Path $Out 'mqcache-docs.pdf'
            $css = Join-Path $PSScriptRoot 'style.css'
            $attempts = @(
                @{ Name = 'xelatex'; Run = { pandoc $combined @common --pdf-engine=xelatex -V geometry:margin=2cm -V colorlinks=true -o $o } },
                @{ Name = 'wkhtmltopdf'; Run = { pandoc $combined @common --pdf-engine=wkhtmltopdf --css $css -o $o } },
                @{ Name = 'browser'; Run = {
                    $html = Join-Path $Out 'mcache-docs.html'
                    if (-not (Test-Path $html)) { pandoc $combined @common --standalone --embed-resources --css $css -o $html }
                    $candidates = @("${env:ProgramFiles(x86)}\Microsoft\Edge\Application\msedge.exe", "$env:ProgramFiles\Microsoft\Edge\Application\msedge.exe",
                                    "$env:ProgramFiles\Google\Chrome\Application\chrome.exe", "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe")
                    $browser = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
                    if (-not $browser) { throw 'no Edge/Chrome found' }
                    $profile = Join-Path ([IO.Path]::GetTempPath()) ('mqdocs-' + [guid]::NewGuid().ToString('N'))
                    $url = ([uri](Resolve-Path $html).Path).AbsoluteUri
                    # msedge/chrome are GUI-subsystem executables: PowerShell does not wait for them unless Start-Process -Wait is used.
                    $argLine = "--headless=new --disable-gpu --no-first-run --no-pdf-header-footer --user-data-dir=`"$profile`" --print-to-pdf=`"$o`" `"$url`""
                    Start-Process -FilePath $browser -ArgumentList $argLine -Wait -NoNewWindow
                    for ($i = 0; $i -lt 60 -and -not (Test-Path $o); $i++) { Start-Sleep -Milliseconds 500 }
                    Remove-Item $profile -Recurse -Force -ErrorAction SilentlyContinue
                } }
            )
            foreach ($a in $attempts) {
                $engine = $a.Name
                if ($engine -ne 'browser' -and -not (Get-Command $engine -ErrorAction SilentlyContinue)) { continue }
                Remove-Item $o -ErrorAction SilentlyContinue
                try { & $a.Run } catch { Write-Warning "PDF via $engine failed: $_" }
                if ((Test-Path $o) -and (Get-Item $o).Length -gt 1024) { Write-Host "PDF built with $engine"; $produced += $o; break }
                Write-Warning "PDF via $engine produced no file; trying next engine"
            }
            if (-not (Test-Path $o)) {
                $msg = 'PDF not produced (no working engine: xelatex, wkhtmltopdf, Edge, Chrome).'
                if ($env:CI -eq 'true') { throw $msg } else { Write-Warning $msg }
            }
        }
        default { Write-Warning "unknown format '$f'" }
    }
}
Write-Host 'Built:' -ForegroundColor Green
$produced | ForEach-Object { Write-Host "  $_" }
