$ErrorActionPreference = "Stop"

$root = Resolve-Path "$PSScriptRoot\..\.."
Set-Location $root

$venvPython = Resolve-Path ".\.venv\Scripts\python.exe"
$configPath = ".\configs\app\settings.local.json"
if (-not (Test-Path $configPath)) {
    throw "Missing runtime config: $configPath. Copy .\configs\app\settings.example.json to settings.local.json and set real local values."
}

$existingDetector = Get-CimInstance Win32_Process |
    Where-Object {
        $_.Name -match '^python(\.exe)?$' -and
        $_.CommandLine -like '*scripts\python\detect_events.py*' -and
        $_.ExecutablePath -like '*\.venv\Scripts\python.exe'
    } |
    Select-Object -First 1

if ($existingDetector) {
    Write-Host "Detector already running (PID $($existingDetector.ProcessId))."
    Write-Host "Check Logs tab or data/logs/detector.log for runtime status."
    return
}

Write-Host "Starting YOLO detection pipeline..."
Write-Host "Detector log file: .\data\logs\detector.log"
& $venvPython ".\scripts\python\detect_events.py" --config $configPath
