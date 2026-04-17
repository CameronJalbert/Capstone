$ErrorActionPreference = "Stop"

$root = Resolve-Path "$PSScriptRoot\..\.."
Set-Location $root

$venvPython = Resolve-Path ".\.venv\Scripts\python.exe"
$configPath = ".\configs\app\settings.local.json"
if (-not (Test-Path $configPath)) {
    $configPath = ".\configs\app\settings.example.json"
}

$config = Get-Content -Raw $configPath | ConvertFrom-Json
$apiHost = $config.api.host
$port = [int]$config.api.port

Write-Host "Starting backend on ${apiHost}:$port ..."
& $venvPython -m uvicorn app.backend.main:app --host $apiHost --port $port --reload
