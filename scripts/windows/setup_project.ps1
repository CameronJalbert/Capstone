$ErrorActionPreference = "Stop"

$root = Resolve-Path "$PSScriptRoot\..\.."
Set-Location $root

function Resolve-PythonExe {
    $candidates = @(
        "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe"
    )

    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) {
            return $candidate
        }
    }

    $pyLauncher = "$env:LOCALAPPDATA\Programs\Python\Launcher\py.exe"
    if (Test-Path $pyLauncher) {
        return "$pyLauncher -3.12"
    }

    $pythonCmd = Get-Command python -ErrorAction SilentlyContinue
    if ($pythonCmd) {
        return $pythonCmd.Path
    }

    throw "Python interpreter not found. Install Python 3.12 and reopen PowerShell."
}

$pythonExe = Resolve-PythonExe

if (-not (Test-Path ".\.venv")) {
    Write-Host "Creating Python virtual environment..."
    if ($pythonExe -like "*py.exe -3.12") {
        $launcher = "$env:LOCALAPPDATA\Programs\Python\Launcher\py.exe"
        & $launcher -3.12 -m venv .venv
    } else {
        & $pythonExe -m venv .venv
    }
}

$venvPython = Resolve-Path ".\.venv\Scripts\python.exe"

Write-Host "Upgrading pip..."
& $venvPython -m pip install --upgrade pip

Write-Host "Installing dependencies..."
& $venvPython -m pip install -r ".\app\backend\requirements.txt"

$ffmpegCmd = Get-Command ffmpeg -ErrorAction SilentlyContinue
$ffmpegLink = "$env:LOCALAPPDATA\Microsoft\WinGet\Links\ffmpeg.exe"
if (-not $ffmpegCmd -and -not (Test-Path $ffmpegLink)) {
    Write-Warning "FFmpeg not found in PATH. Install it with: winget install --id Gyan.FFmpeg --exact --source winget --scope user --accept-package-agreements --accept-source-agreements"
}

Write-Host "Ensuring data directories exist..."
New-Item -ItemType Directory -Force -Path `
    ".\data\recordings", `
    ".\data\logs", `
    ".\data\events", `
    ".\data\snapshots", `
    ".\data\models" | Out-Null

if (-not (Test-Path ".\configs\app\settings.local.json")) {
    Write-Host "Creating local config from template..."
    Copy-Item ".\configs\app\settings.example.json" ".\configs\app\settings.local.json"
}

Write-Host "Setup complete."
Write-Host "Next: edit .\configs\app\settings.local.json with your real camera stream details."
