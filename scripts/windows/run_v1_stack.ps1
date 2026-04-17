$ErrorActionPreference = "Stop"

$root = Resolve-Path "$PSScriptRoot\..\.."
Set-Location $root

$backendCmd = "Set-Location '$root'; .\scripts\windows\start_backend.ps1"

Start-Process powershell -ArgumentList "-NoExit", "-Command", $backendCmd

Write-Host "Launched backend in a separate window."
Write-Host "Open http://localhost:8080 after services initialize."
Write-Host "Note: recorder/detector scripts currently open camera streams directly and are not part of centralized ingest yet."
