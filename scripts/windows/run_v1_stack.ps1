$ErrorActionPreference = "Stop"

$root = Resolve-Path "$PSScriptRoot\..\.."
Set-Location $root

$backendCmd = "Set-Location '$root'; .\scripts\windows\start_backend.ps1 -WithDetector"

Start-Process powershell -ArgumentList "-NoExit", "-Command", $backendCmd

Write-Host "Launched backend (+ detector) in separate window(s)."
Write-Host "Open http://localhost:8080 after services initialize."
Write-Host "Note: detector now uses backend ingest by default; recorder path is still transitional."
