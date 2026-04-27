param(
    [switch]$WithDetector,
    [switch]$DevReload
)

$ErrorActionPreference = "Stop"

$root = Resolve-Path "$PSScriptRoot\..\.."
Set-Location $root

$venvPython = Resolve-Path ".\.venv\Scripts\python.exe"
$configPath = ".\configs\app\settings.local.json"
if (-not (Test-Path $configPath)) {
    throw "Missing runtime config: $configPath. Copy .\configs\app\settings.example.json to settings.local.json and set real local values."
}

$config = Get-Content -Raw $configPath | ConvertFrom-Json
$apiHost = $config.api.host
$port = [int]$config.api.port
$logConfigPath = ".\configs\app\logging.backend.json"

if (-not (Test-Path $logConfigPath)) {
    throw "Backend logging config missing: $logConfigPath"
}

New-Item -ItemType Directory -Force -Path ".\data\logs" | Out-Null

# Avoid duplicate backend launches that can cause port conflicts and unstable behavior.
$listener = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
if ($listener) {
    Write-Host "Backend already appears to be running on port $port (PID $($listener.OwningProcess))."
    Write-Host "Stop existing backend first if you need to restart with different options."
    if ($WithDetector) {
        Write-Host "Launching detector only because -WithDetector was requested..."
        $detectorCmd = "Set-Location '$root'; .\scripts\windows\start_detection.ps1"
        Start-Process powershell -ArgumentList "-NoExit", "-Command", $detectorCmd | Out-Null
    }
    return
}

Write-Host "Starting backend on ${apiHost}:$port ..."
Write-Host "Backend logs: .\data\logs\backend.log"
Write-Host "Access-request spam is disabled (--no-access-log) to reduce detector polling noise."
Write-Host "Application/error logs are still written to console and backend.log."
Write-Host "Graceful shutdown timeout is set to 4s to avoid hangs from long-lived stream clients."

$reloadFlag = @()
if ($DevReload) {
    $reloadFlag = @("--reload")
    Write-Host "Developer hot-reload enabled."
} else {
    Write-Host "Stable mode enabled (no hot-reload) for better runtime performance."
}

if ($WithDetector) {
    Write-Host "Launching detector in a separate PowerShell window..."
    $detectorCmd = "Set-Location '$root'; .\scripts\windows\start_detection.ps1"
    Start-Process powershell -ArgumentList "-NoExit", "-Command", $detectorCmd | Out-Null
}

& $venvPython -m uvicorn app.backend.main:app --host $apiHost --port $port @reloadFlag --no-access-log --timeout-graceful-shutdown 4 --log-config $logConfigPath
