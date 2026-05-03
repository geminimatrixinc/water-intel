[CmdletBinding()]
param(
    [string]$DashboardUrl = "http://localhost:3000/dashboard",
    [string]$WebHealthUrl = "http://localhost:3000/api/health",
    [string]$ApiHealthUrl = "http://127.0.0.1:8000/health",
    [int]$WebPort = 3000,
    [int]$ApiPort = 8000,
    [int]$TimeoutSeconds = 120,
    [switch]$NoBrowser
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSCommandPath
$webDir = Join-Path $repoRoot "web\app"

function Quote-Single([string]$Value) {
    return $Value -replace "'", "''"
}

function Test-CommandExists([string]$Name) {
    return $null -ne (Get-Command $Name -ErrorAction SilentlyContinue)
}

function Get-PythonLaunchCommand {
    $venvCandidates = @(
        (Join-Path $repoRoot ".venv\Scripts\python.exe"),
        (Join-Path $repoRoot "venv\Scripts\python.exe")
    )

    foreach ($candidate in $venvCandidates) {
        if (Test-Path $candidate) {
            return "& '" + (Quote-Single $candidate) + "'"
        }
    }

    if (Test-CommandExists "python") {
        return "python"
    }

    if (Test-CommandExists "py") {
        return "py -3"
    }

    throw "Python was not found. Install Python or create a local virtual environment before running this script."
}

function Test-UrlHealthy([string]$Url) {
    try {
        $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5
        return $response.StatusCode -ge 200 -and $response.StatusCode -lt 300
    }
    catch {
        return $false
    }
}

function Test-PortListening([int]$Port) {
    try {
        return $null -ne (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction Stop | Select-Object -First 1)
    }
    catch {
        return $false
    }
}

function Start-ServiceWindow([string]$Label, [string]$WorkingDirectory, [string]$Command) {
    $startupCommand = "Set-Location -LiteralPath '" + (Quote-Single $WorkingDirectory) + "'; " + $Command
    Write-Host "Starting $Label..." -ForegroundColor Cyan
    Start-Process -FilePath "powershell.exe" -WorkingDirectory $WorkingDirectory -ArgumentList @(
        "-NoExit",
        "-Command",
        $startupCommand
    ) | Out-Null
}

function Wait-ForHealthyUrl([string]$Name, [string]$Url, [int]$TimeoutSeconds) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    while ((Get-Date) -lt $deadline) {
        if (Test-UrlHealthy $Url) {
            Write-Host "$Name is ready at $Url" -ForegroundColor Green
            return
        }

        Start-Sleep -Seconds 2
    }

    throw "$Name did not become ready within $TimeoutSeconds seconds. Check the service window for errors."
}

if (-not (Test-Path $webDir)) {
    throw "Web app directory was not found: $webDir"
}

if (-not (Test-CommandExists "npm")) {
    throw "npm was not found. Install Node.js before running this script."
}

$pythonLaunch = Get-PythonLaunchCommand
$apiCommand = "$pythonLaunch -m uvicorn api.main:app --reload"
$webCommand = "npm run dev"

if (Test-UrlHealthy $ApiHealthUrl) {
    Write-Host "API is already running." -ForegroundColor Yellow
}
elseif (Test-PortListening $ApiPort) {
    throw "Port $ApiPort is already in use, but $ApiHealthUrl is not responding as expected."
}
else {
    Start-ServiceWindow -Label "FastAPI backend" -WorkingDirectory $repoRoot -Command $apiCommand
}

Wait-ForHealthyUrl -Name "API" -Url $ApiHealthUrl -TimeoutSeconds $TimeoutSeconds

if (Test-UrlHealthy $WebHealthUrl) {
    Write-Host "Web app is already running." -ForegroundColor Yellow
}
elseif (Test-PortListening $WebPort) {
    throw "Port $WebPort is already in use, but $WebHealthUrl is not responding as expected."
}
else {
    Start-ServiceWindow -Label "Next.js frontend" -WorkingDirectory $webDir -Command $webCommand
}

Wait-ForHealthyUrl -Name "Web app" -Url $WebHealthUrl -TimeoutSeconds $TimeoutSeconds

if (-not $NoBrowser) {
    Write-Host "Opening dashboard..." -ForegroundColor Cyan
    Start-Process $DashboardUrl | Out-Null
}

Write-Host "Water-Intel dashboard is ready: $DashboardUrl" -ForegroundColor Green
