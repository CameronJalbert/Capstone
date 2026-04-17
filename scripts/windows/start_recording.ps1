$ErrorActionPreference = "Stop"

$root = Resolve-Path "$PSScriptRoot\..\.."
Set-Location $root

$venvPython = Resolve-Path ".\.venv\Scripts\python.exe"
$configPath = ".\configs\app\settings.local.json"
if (-not (Test-Path $configPath)) {
    $configPath = ".\configs\app\settings.example.json"
}

Write-Host "Starting camera recording pipeline..."
& $venvPython ".\scripts\python\ingest_rtsp.py" --config $configPath
