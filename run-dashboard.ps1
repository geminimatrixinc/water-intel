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

$canonicalScript = Join-Path $PSScriptRoot "ops\scripts\run-dashboard.ps1"

if (-not (Test-Path $canonicalScript)) {
    throw "Canonical dashboard runner was not found: $canonicalScript"
}

& $canonicalScript @PSBoundParameters
exit $LASTEXITCODE
