$ErrorActionPreference = "Stop"

$root = Resolve-Path "$PSScriptRoot\..\.."
Set-Location $root

$venvPython = Resolve-Path ".\.venv\Scripts\python.exe"
$configPath = ".\configs\app\settings.local.json"
if (-not (Test-Path $configPath)) {
    throw "Missing runtime config: $configPath. Copy .\configs\app\settings.example.json to settings.local.json and set real local values."
}

Write-Host "Starting camera recording pipeline..."
& $venvPython ".\scripts\python\ingest_rtsp.py" --config $configPath
