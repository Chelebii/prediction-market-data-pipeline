param(
    [int]$PollSec = 15,
    [switch]$NoStart,
    [int]$MaxLoops = 0
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir '..\..')
Set-Location $repoRoot

$pythonExe = (Get-Command python -ErrorAction Stop).Source
$summaryScript = Join-Path $repoRoot 'scripts\btc5m_collection_summary.py'
$controlScript = Join-Path $repoRoot 'control\scripts\btc5m_collection_control.ps1'

function Write-MonitorLine {
    param(
        [string]$Message,
        [string]$Level = 'INFO'
    )

    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $color = switch ($Level) {
        'ERROR' { 'Red' }
        'WARN' { 'Yellow' }
        'OK' { 'Green' }
        default { 'Gray' }
    }
    Write-Host "[$timestamp] BTC5M-MONITOR | $Message" -ForegroundColor $color
}

function Get-Summary {
    $jsonText = & $pythonExe $summaryScript --json 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $jsonText) {
        throw "collection_summary_failed"
    }
    return ($jsonText | ConvertFrom-Json)
}

function Get-WindowTitle {
    param($Summary)

    $warnings = @($Summary.warnings)
    $snapshotAge = if ($null -ne $Summary.freshness.snapshot_age_sec) { [int]$Summary.freshness.snapshot_age_sec } else { -1 }
    $referenceAge = if ($null -ne $Summary.freshness.reference_age_sec) { [int]$Summary.freshness.reference_age_sec } else { -1 }
    $auditAge = if ($null -ne $Summary.freshness.audit_age_sec) { [int]$Summary.freshness.audit_age_sec } else { -1 }

    if ($warnings.Count -gt 0) {
        return "BTC5M Monitor | WARN | snap=${snapshotAge}s ref=${referenceAge}s audit=${auditAge}s | warnings=$($warnings.Count)"
    }
    return "BTC5M Monitor | OK | snap=${snapshotAge}s ref=${referenceAge}s audit=${auditAge}s"
}

function Get-StateSignature {
    param($Summary)

    $payload = [ordered]@{
        warnings = @($Summary.warnings)
        scanner_running = [bool]$Summary.collectors.scanner.running
        reference_running = [bool]$Summary.collectors.reference.running
        resolution_running = [bool]$Summary.collectors.resolution.running
        audit_status = [string]($Summary.audit.audit_status)
    }
    return ($payload | ConvertTo-Json -Compress -Depth 6)
}

function Get-StateMessage {
    param($Summary)

    $warnings = @($Summary.warnings)
    if ($warnings.Count -eq 0) {
        return "healthy | snapshot_age=$($Summary.freshness.snapshot_age_sec)s | reference_age=$($Summary.freshness.reference_age_sec)s | audit_age=$($Summary.freshness.audit_age_sec)s"
    }
    return "warnings=" + ($warnings -join ', ')
}

if (-not $NoStart) {
    try {
        & $controlScript -Action start | Out-Null
        Write-MonitorLine "collector startup check tamamlandi" "OK"
    } catch {
        Write-MonitorLine "collector startup check failed: $($_.Exception.Message)" "ERROR"
    }
}

Write-MonitorLine "monitor basladi; title durum gosterecek, log sadece state degisince yazilacak" "INFO"

$lastSignature = $null
$lastWasHealthy = $false
$loopCount = 0

while ($true) {
    $loopCount += 1
    try {
        $summary = Get-Summary
        $Host.UI.RawUI.WindowTitle = Get-WindowTitle -Summary $summary
        $signature = Get-StateSignature -Summary $summary
        $isHealthy = (@($summary.warnings).Count -eq 0)

        if ($signature -ne $lastSignature) {
            if ($isHealthy) {
                Write-MonitorLine (Get-StateMessage -Summary $summary) "OK"
            } else {
                Write-MonitorLine (Get-StateMessage -Summary $summary) "WARN"
            }
            $lastSignature = $signature
        } elseif ($isHealthy -and -not $lastWasHealthy) {
            Write-MonitorLine (Get-StateMessage -Summary $summary) "OK"
        }
        $lastWasHealthy = $isHealthy
    } catch {
        $Host.UI.RawUI.WindowTitle = "BTC5M Monitor | ERROR | summary unavailable"
        $message = "summary unavailable: $($_.Exception.Message)"
        if ($message -ne $lastSignature) {
            Write-MonitorLine $message "ERROR"
            $lastSignature = $message
        }
        $lastWasHealthy = $false
    }

    if ($MaxLoops -gt 0 -and $loopCount -ge $MaxLoops) {
        break
    }
    Start-Sleep -Seconds ([Math]::Max(2, $PollSec))
}
