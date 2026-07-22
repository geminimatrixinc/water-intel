[CmdletBinding()]
param(
    [string]$DashboardUrl = "http://localhost:3000/dashboard",
    [string]$WebHealthUrl = "http://localhost:3000/api/health",
    [string]$ApiHealthUrl = "http://127.0.0.1:8000/health",
    [int]$WebPort = 3000,
    [int]$ApiPort = 8000,
    [int]$TimeoutSeconds = 120,
    [switch]$NoBrowser,
    [switch]$StopOnly
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$webDir = Join-Path $repoRoot "web\app"
$versionFilePath = Join-Path $repoRoot "VERSION"

function Test-IsAdministrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

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

function Get-ListenerProcessIds([int[]]$Ports) {
    $ids = @()

    foreach ($port in $Ports) {
        try {
            $portIds = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction Stop |
                Select-Object -ExpandProperty OwningProcess -Unique
            if ($portIds) {
                $ids += $portIds
            }
        }
        catch {
            # No listener for this port.
        }
    }

    return $ids |
        Where-Object { $_ -and ($_ -gt 0) } |
        Select-Object -Unique
}

function Show-AdminPortHelp([int[]]$Ports) {
    $portCsv = ($Ports | Sort-Object -Unique) -join ","
    Write-Host "The remaining listeners may be owned by protected host processes." -ForegroundColor Yellow
    Write-Host "Run these commands from an elevated PowerShell:" -ForegroundColor Yellow
    Write-Host "  wsl --shutdown"
    Write-Host "  netstat -ano | findstr :$($Ports[0])"
    if ($Ports.Count -gt 1) {
        Write-Host "  netstat -ano | findstr :$($Ports[1])"
    }
    Write-Host "  # Then stop listed PIDs if needed: taskkill /PID <pid> /F /T"
    Write-Host "Requested ports: $portCsv"
}

function Clear-TargetPorts([int[]]$Ports) {
    $ports = $Ports | Sort-Object -Unique
    if (-not $ports -or $ports.Count -eq 0) {
        return $true
    }

    $processIds = Get-ListenerProcessIds -Ports $ports
    if ($processIds.Count -gt 0) {
        Write-Host "Stopping listeners on ports $($ports -join ', ')..." -ForegroundColor Cyan
    }

    foreach ($processId in $processIds) {
        Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
        try {
            & taskkill /PID $processId /F /T *> $null
        }
        catch {
            # Ignore races where the target PID exits before taskkill runs.
        }
    }

    $remaining = Get-ListenerProcessIds -Ports $ports
    if ($remaining.Count -gt 0 -and (Test-CommandExists "wsl.exe" -or Test-CommandExists "wsl")) {
        Write-Host "Some listeners persisted; shutting down WSL forwarding..." -ForegroundColor Yellow
        & wsl.exe --shutdown *> $null
        $remaining = Get-ListenerProcessIds -Ports $ports
    }

    if ($remaining.Count -eq 0) {
        return $true
    }

    Write-Host "Could not release all requested ports." -ForegroundColor Red
    foreach ($port in $ports) {
        try {
            $rows = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction Stop |
                Select-Object LocalAddress, LocalPort, OwningProcess, State
            if ($rows) {
                $rows | Format-Table -AutoSize | Out-Host
            }
        }
        catch {
            # No listener for this port.
        }
    }

    if (-not (Test-IsAdministrator)) {
        Show-AdminPortHelp -Ports $ports
    }

    return $false
}

function Get-ExpectedApiVersion {
    if (-not (Test-Path $versionFilePath)) {
        return $null
    }

    try {
        $value = (Get-Content -Path $versionFilePath -Raw).Trim()
        if ([string]::IsNullOrWhiteSpace($value)) {
            return $null
        }
        return $value
    }
    catch {
        return $null
    }
}

function Get-ApiVersion([string]$Url) {
    try {
        $response = Invoke-RestMethod -Uri $Url -TimeoutSec 5
        if ($response -and $response.version) {
            return [string]$response.version
        }
    }
    catch {
        # Unhealthy API or non-JSON response.
    }

    return $null
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
$apiCommand = "$pythonLaunch -m uvicorn services.api.main:app --reload"
$webCommand = "npm run dev"
$expectedApiVersion = Get-ExpectedApiVersion

if ($StopOnly) {
    if (-not (Clear-TargetPorts -Ports @($ApiPort, $WebPort))) {
        throw "Failed to clear one or more requested ports."
    }

    Write-Host "Ports cleared successfully: $ApiPort, $WebPort" -ForegroundColor Green
    return
}

$startApi = $false

if (Test-UrlHealthy $ApiHealthUrl) {
    $runningVersion = Get-ApiVersion -Url $ApiHealthUrl
    if ($expectedApiVersion -and $runningVersion -and $runningVersion -ne $expectedApiVersion) {
        Write-Host (
            "API is running but version is {0}; expected {1}. Restarting API..." -f
            $runningVersion,
            $expectedApiVersion
        ) -ForegroundColor Yellow

        if (-not (Clear-TargetPorts -Ports @($ApiPort))) {
            throw "Port $ApiPort is in use by a stale API and could not be released."
        }

        $startApi = $true
    }
    else {
        Write-Host "API is already running." -ForegroundColor Yellow
    }
}
elseif (Test-PortListening $ApiPort) {
    Write-Host "Port $ApiPort is in use but health is not available. Attempting cleanup..." -ForegroundColor Yellow
    if (-not (Clear-TargetPorts -Ports @($ApiPort))) {
        throw "Port $ApiPort is already in use, but $ApiHealthUrl is not responding as expected."
    }
    $startApi = $true
}
else {
    $startApi = $true
}

if ($startApi) {
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
