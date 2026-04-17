param(
    [int]$PollSec = 15,
    [switch]$NoStart,
    [int]$MaxLoops = 0
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir '..\..')
Set-Location $repoRoot

$pythonExe = $null
$knownPythonPath = Join-Path $env:LOCALAPPDATA 'Python\bin\python.exe'
if (Test-Path $knownPythonPath) {
    $pythonExe = $knownPythonPath
} else {
    $candidates = @(Get-Command python -All -ErrorAction SilentlyContinue |
        Where-Object { $_.Source -notlike '*\WindowsApps\*' })
    if ($candidates.Count -gt 0) {
        $pythonExe = $candidates[0].Source
    }
}
if (-not $pythonExe) {
    throw "Python could not be found. Ensure Python is installed and its path is in the system PATH (not via Windows Store alias)."
}
$summaryScript = Join-Path $repoRoot 'scripts\btc5m_collection_summary.py'
$controlScript = Join-Path $repoRoot 'control\scripts\btc5m_collection_control.ps1'
$monitorLockPath = Join-Path $repoRoot 'runtime\locks\btc5m_console_monitor.lock'
$monitorScriptPath = [System.IO.Path]::GetFullPath($MyInvocation.MyCommand.Path).ToLowerInvariant()

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

function Get-MonitorLockInfo {
    if (-not (Test-Path $monitorLockPath)) {
        return $null
    }

    try {
        $raw = Get-Content $monitorLockPath -Raw -ErrorAction Stop
        if (-not $raw) { return $null }

        $trimmed = $raw.Trim()
        if ($trimmed -match '^\d+$') {
            return @{ pid = [int]$trimmed }
        }

        $payload = $trimmed | ConvertFrom-Json -ErrorAction Stop
        if ($payload -and $payload.pid) {
            return @{
                pid = [int]$payload.pid
                started_at = $payload.started_at
                script_path = $payload.script_path
            }
        }
    } catch {}

    return $null
}

function Test-MonitorProcessAlive {
    param([int]$ProcId)

    if (-not $ProcId) { return $false }

    try {
        $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $ProcId" -ErrorAction Stop
        if (-not $proc) { return $false }
        if ($proc.Name -notin @('powershell.exe', 'pwsh.exe')) { return $false }

        $commandLine = [string]$proc.CommandLine
        if (-not $commandLine) { return $false }

        return $commandLine.ToLowerInvariant().Contains($monitorScriptPath)
    } catch {
        return $false
    }
}

function Acquire-MonitorLock {
    $lockDir = Split-Path -Parent $monitorLockPath
    if (-not (Test-Path $lockDir)) {
        New-Item -ItemType Directory -Path $lockDir -Force | Out-Null
    }

    $lockInfo = Get-MonitorLockInfo
    if ($lockInfo -and (Test-MonitorProcessAlive -ProcId $lockInfo.pid)) {
        Write-MonitorLine "Monitor is already running. Closing the second window." "WARN"
        exit 0
    }

    if (Test-Path $monitorLockPath) {
        Remove-Item $monitorLockPath -Force -ErrorAction SilentlyContinue
    }

    $payload = [ordered]@{
        pid = $PID
        started_at = (Get-Date).ToString('s')
        script_path = $monitorScriptPath
    }
    ($payload | ConvertTo-Json -Compress) | Set-Content -Path $monitorLockPath -Encoding UTF8
}

function Get-Summary {
    $jsonText = & $pythonExe $summaryScript --json 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $jsonText) {
        throw "collection_summary_failed"
    }
    return ($jsonText | ConvertFrom-Json)
}

function Format-PlainDuration {
    param([Nullable[int]]$TotalSec)

    if ($null -eq $TotalSec -or $TotalSec -lt 0) { return '-' }
    $seconds = [int]$TotalSec
    if ($seconds -lt 60) { return "$seconds sec" }

    $minutes = [math]::Floor($seconds / 60)
    $remain = $seconds % 60
    if ($minutes -lt 60) { return "$minutes min $remain sec" }

    $hours = [math]::Floor($minutes / 60)
    $minutes = $minutes % 60
    if ($hours -lt 24) { return "$hours hr $minutes min" }

    $days = [math]::Floor($hours / 24)
    $hours = $hours % 24
    return "$days day $hours hr"
}

function Format-ShortDuration {
    param([Nullable[int]]$TotalSec)

    if ($null -eq $TotalSec -or $TotalSec -lt 0) { return '-' }
    $seconds = [int]$TotalSec
    if ($seconds -lt 60) { return "${seconds}s" }

    $minutes = [math]::Floor($seconds / 60)
    $remain = $seconds % 60
    if ($minutes -lt 60) { return "${minutes}m${remain}s" }

    $hours = [math]::Floor($minutes / 60)
    $minutes = $minutes % 60
    return "${hours}h${minutes}m"
}

function Format-PlainTimestamp {
    param([Nullable[int]]$Ts)

    if ($null -eq $Ts -or $Ts -le 0) { return '-' }
    try {
        return (Get-Date -Date ([DateTimeOffset]::FromUnixTimeSeconds([int64]$Ts).LocalDateTime) -Format 'yyyy-MM-dd HH:mm:ss')
    } catch {
        return '-'
    }
}

function Get-WarningAgeSec {
    param([string]$Warning)

    if (-not $Warning) { return $null }
    if ($Warning -match ':(\d+)s$') {
        return [int]$matches[1]
    }
    return $null
}

function Get-WarningTier {
    param([string]$Warning)

    if (-not $Warning) { return 'INFO' }

    if (
        $Warning -eq 'scanner_collector_not_running' -or
        $Warning -eq 'reference_collector_not_running' -or
        $Warning -eq 'resolution_collector_not_running' -or
        $Warning -like 'snapshot_stale:*' -or
        $Warning -like 'reference_stale:*' -or
        $Warning -like 'backup_stale:*' -or
        $Warning -like 'scanner_collector_errors:*' -or
        $Warning -like 'reference_collector_errors:*' -or
        $Warning -like 'resolution_collector_errors:*' -or
        $Warning -like 'health_issue:*'
    ) {
        return 'ACTION'
    }

    if (
        $Warning -like 'audit_stale:*' -or
        $Warning -like 'health_status_stale:*' -or
        $Warning -like 'health_warning:*' -or
        $Warning -eq 'latest_audit_failed'
    ) {
        return 'WATCH'
    }

    return 'WATCH'
}

function Get-HighestWarningTier {
    param([object[]]$Warnings)

    $tiers = @($Warnings | ForEach-Object { Get-WarningTier $_ })
    if ($tiers -contains 'ACTION') { return 'ACTION' }
    if ($tiers -contains 'WATCH') { return 'WATCH' }
    return 'INFO'
}

function Get-WindowTitle {
    param($Summary)

    $warnings = @($Summary.warnings)
    $snapshotAge = if ($null -ne $Summary.freshness.snapshot_age_sec) { [int]$Summary.freshness.snapshot_age_sec } else { -1 }
    $referenceAge = if ($null -ne $Summary.freshness.reference_age_sec) { [int]$Summary.freshness.reference_age_sec } else { -1 }
    $auditAge = if ($null -ne $Summary.freshness.audit_age_sec) { [int]$Summary.freshness.audit_age_sec } else { -1 }
    if ($warnings.Count -gt 0) {
        $tier = Get-HighestWarningTier -Warnings $warnings
        if ($tier -eq 'ACTION') {
            return "Prediction Market Data Pipeline | BTC5M Monitor | Action required | snapshot=$(Format-ShortDuration $snapshotAge) reference=$(Format-ShortDuration $referenceAge) audit=$(Format-ShortDuration $auditAge)"
        }
        return "Prediction Market Data Pipeline | BTC5M Monitor | Attention | snapshot=$(Format-ShortDuration $snapshotAge) reference=$(Format-ShortDuration $referenceAge) audit=$(Format-ShortDuration $auditAge)"
    }
    return "Prediction Market Data Pipeline | BTC5M Monitor | Collecting data | snapshot=$(Format-ShortDuration $snapshotAge) reference=$(Format-ShortDuration $referenceAge) audit=$(Format-ShortDuration $auditAge)"
}

function Convert-WarningToPlainText {
    param([string]$Warning)

    if (-not $Warning) { return "Unknown warning" }
    if ($Warning -like 'scanner_collector_not_running') { return 'Scanner is not running.' }
    if ($Warning -like 'reference_collector_not_running') { return 'Reference collector is not running.' }
    if ($Warning -like 'resolution_collector_not_running') { return 'Resolution collector is not running.' }
    if ($Warning -like 'snapshot_stale:*') { return "Scanner has stopped writing fresh data ($(Format-PlainDuration (Get-WarningAgeSec $Warning)))." }
    if ($Warning -like 'reference_stale:*') { return "Reference data is stale ($(Format-PlainDuration (Get-WarningAgeSec $Warning)))." }
    if ($Warning -like 'audit_stale:*') { return "Audit has not been refreshed ($(Format-PlainDuration (Get-WarningAgeSec $Warning)))." }
    if ($Warning -like 'backup_stale:*') { return "Backup is overdue ($(Format-PlainDuration (Get-WarningAgeSec $Warning)))." }
    if ($Warning -like 'health_status_stale:*') { return "Health status has not been refreshed ($(Format-PlainDuration (Get-WarningAgeSec $Warning)))." }
    if ($Warning -like 'health_issue:*') { return ("Health issue: " + $Warning.Substring(13)) }
    if ($Warning -like 'health_warning:*') { return ("Health warning: " + $Warning.Substring(15)) }
    if ($Warning -like 'scanner_collector_errors:*') { return ("Scanner reported errors in the latest run (" + ($Warning -split ':', 2)[1] + ").") }
    if ($Warning -like 'reference_collector_errors:*') { return ("Reference collector reported errors in the latest run (" + ($Warning -split ':', 2)[1] + ").") }
    if ($Warning -like 'resolution_collector_errors:*') { return ("Resolution collector reported errors in the latest run (" + ($Warning -split ':', 2)[1] + ").") }
    if ($Warning -eq 'latest_audit_failed') { return 'The latest aggregate audit failed. This does not necessarily mean live collection is currently broken.' }
    return $Warning
}

function Normalize-WarningKey {
    param([string]$Warning)

    if (-not $Warning) { return "" }
    if ($Warning -match '^(snapshot_stale|reference_stale|audit_stale|backup_stale|health_status_stale):') {
        return $matches[1]
    }
    if ($Warning -match '^(scanner_collector_errors|reference_collector_errors|resolution_collector_errors):') {
        return $matches[1]
    }
    if ($Warning -match '^(health_issue|health_warning):') {
        return $matches[1] + ':' + ($Warning -split ':', 2)[1]
    }
    return $Warning
}

function Get-StateSignature {
    param($Summary)

    $payload = [ordered]@{
        warnings = @($Summary.warnings | ForEach-Object { Normalize-WarningKey $_ })
        scanner_running = [bool]$Summary.collectors.scanner.running
        reference_running = [bool]$Summary.collectors.reference.running
        resolution_running = [bool]$Summary.collectors.resolution.running
        scanner_image = [string]($Summary.collectors.scanner.process_image_name)
        reference_image = [string]($Summary.collectors.reference.process_image_name)
        resolution_image = [string]($Summary.collectors.resolution.process_image_name)
    }
    return ($payload | ConvertTo-Json -Compress -Depth 6)
}

function Get-StateMessage {
    param($Summary)

    $warnings = @($Summary.warnings)
    $scannerImage = if ($Summary.collectors.scanner.process_image_name) { [string]$Summary.collectors.scanner.process_image_name } else { '-' }
    $referenceImage = if ($Summary.collectors.reference.process_image_name) { [string]$Summary.collectors.reference.process_image_name } else { '-' }
    $resolutionImage = if ($Summary.collectors.resolution.process_image_name) { [string]$Summary.collectors.resolution.process_image_name } else { '-' }
    if ($warnings.Count -eq 0) {
        return "Data collection is healthy. Scanner=${scannerImage}, Reference=${referenceImage}, Resolution=${resolutionImage}"
    }

    $nonStaleWarnings = @($warnings | Where-Object { $_ -notmatch '^(audit_stale|health_status_stale):' })
    if ($nonStaleWarnings.Count -eq 0) {
        $auditAge = Format-PlainDuration (Get-WarningAgeSec ($warnings | Where-Object { $_ -like 'audit_stale:*' } | Select-Object -First 1))
        $healthAge = Format-PlainDuration (Get-WarningAgeSec ($warnings | Where-Object { $_ -like 'health_status_stale:*' } | Select-Object -First 1))
        return "Attention: data is still flowing, but control reports are stale. Audit=$auditAge, Health=$healthAge."
    }

    $effectiveWarnings = @()
    foreach ($warning in $warnings) {
        if ($warning -like 'snapshot_stale:*' -and $warnings -contains 'scanner_collector_not_running') { continue }
        if ($warning -like 'reference_stale:*' -and $warnings -contains 'reference_collector_not_running') { continue }
        if ($warning -like 'audit_stale:*' -and $warnings -contains 'latest_audit_failed') { continue }
        if ($warning -like 'health_status_stale:*' -and ($warnings | Where-Object { $_ -like 'health_issue:*' -or $_ -like 'health_warning:*' }).Count -gt 0) { continue }
        $effectiveWarnings += $warning
    }

    $plainWarnings = @($effectiveWarnings | ForEach-Object { Convert-WarningToPlainText $_ })
    $tier = Get-HighestWarningTier -Warnings $effectiveWarnings
    if ($tier -eq 'ACTION') {
        return "Action required: " + ($plainWarnings -join ' | ')
    }
    return "Attention: " + ($plainWarnings -join ' | ')
}

function Test-ScannerRawActivityFresh {
    param($Summary)

    if (-not $Summary -or -not $Summary.scanner_runtime) {
        return $false
    }

    $runtime = $Summary.scanner_runtime
    if ($null -eq $runtime.age_sec) {
        return $false
    }

    $state = [string]($runtime.state)
    if (-not $state -or $state -eq 'PARSE_FAILED') {
        return $false
    }

    return ([int]$runtime.age_sec -le [int]$ScannerRawActivityFreshSec)
}

function Get-RecoveryPlan {
    param($Summary)

    $targetActions = @{}
    $reasons = New-Object System.Collections.Generic.List[string]
    $warnings = @($Summary.warnings)

    if (-not [bool]$Summary.collectors.scanner.running -or $warnings -contains 'scanner_collector_not_running') {
        $targetActions['scanner'] = 'start'
        $reasons.Add('scanner_not_running')
    } elseif (@($warnings | Where-Object { $_ -like 'snapshot_stale:*' }).Count -gt 0) {
        if (-not (Test-ScannerRawActivityFresh -Summary $Summary)) {
            $targetActions['scanner'] = 'restart'
            $reasons.Add('snapshot_stale')
        }
    }

    if (-not [bool]$Summary.collectors.reference.running -or $warnings -contains 'reference_collector_not_running') {
        $targetActions['reference'] = 'start'
        $reasons.Add('reference_not_running')
    } elseif (@($warnings | Where-Object { $_ -like 'reference_stale:*' }).Count -gt 0) {
        $targetActions['reference'] = 'restart'
        $reasons.Add('reference_stale')
    }

    if (-not [bool]$Summary.collectors.resolution.running -or $warnings -contains 'resolution_collector_not_running') {
        $targetActions['resolution'] = 'start'
        $reasons.Add('resolution_not_running')
    }

    $startTargets = @($targetActions.GetEnumerator() | Where-Object { $_.Value -eq 'start' } | ForEach-Object { $_.Key } | Sort-Object)
    $restartTargets = @($targetActions.GetEnumerator() | Where-Object { $_.Value -eq 'restart' } | ForEach-Object { $_.Key } | Sort-Object)
    $reasonsText = @($reasons | Select-Object -Unique | Sort-Object)

    return [ordered]@{
        has_plan = ($startTargets.Count -gt 0 -or $restartTargets.Count -gt 0)
        start_targets = $startTargets
        restart_targets = $restartTargets
        reasons = $reasonsText
        signature = ((@($startTargets | ForEach-Object { "start:$($_)" }) + @($restartTargets | ForEach-Object { "restart:$($_)" }) + @($reasonsText | ForEach-Object { "reason:$($_)" })) -join '|')
    }
}

function Invoke-RecoveryPlan {
    param($Plan)

    if (-not $Plan -or -not $Plan.has_plan) {
        return
    }

    if (@($Plan.start_targets).Count -gt 0) {
        & $controlScript -Action start -Targets (@($Plan.start_targets) -join ',') | Out-Null
    }

    if (@($Plan.restart_targets).Count -gt 0) {
        & $controlScript -Action restart -Targets (@($Plan.restart_targets) -join ',') | Out-Null
    }
}

function Get-RecoveryDescription {
    param($Plan)

    if (-not $Plan -or -not $Plan.has_plan) {
        return ''
    }

    $parts = @()
    if (@($Plan.start_targets).Count -gt 0) {
        $parts += ("start=" + (@($Plan.start_targets) -join ','))
    }
    if (@($Plan.restart_targets).Count -gt 0) {
        $parts += ("restart=" + (@($Plan.restart_targets) -join ','))
    }
    if (@($Plan.reasons).Count -gt 0) {
        $parts += ("reason=" + (@($Plan.reasons) -join ','))
    }
    return ($parts -join ' | ')
}

$RecoveryCooldownSec = [Math]::Max(15, $PollSec)
$RecoveryFailClosedSec = [Math]::Max(30, $PollSec * 3)
$RecoveryMaxAttempts = 3
$ScannerRawActivityFreshSec = [Math]::Max(60, $PollSec * 4)

if (-not $NoStart) {
    try {
        & $controlScript -Action start | Out-Null
        Write-MonitorLine "Collector startup check completed." "OK"
    } catch {
        Write-MonitorLine "collector startup check failed: $($_.Exception.Message)" "ERROR"
    }
}

Acquire-MonitorLock

$startupSummary = $null
try {
    $startupSummary = Get-Summary
} catch {}

if ($startupSummary) {
    $startupSnapshotTs = Format-PlainTimestamp $startupSummary.freshness.snapshot_last_ts
    $startupReferenceTs = Format-PlainTimestamp $startupSummary.freshness.reference_last_ts
    $startupAuditTs = Format-PlainTimestamp $startupSummary.freshness.audit_last_ts
    Write-MonitorLine "Monitor ready. Latest DB timestamps: snapshot=$startupSnapshotTs | reference=$startupReferenceTs | audit=$startupAuditTs. If everything is healthy, this window will stay quiet." "INFO"
} else {
    Write-MonitorLine "Monitor ready. If everything is healthy, this window will stay quiet." "INFO"
}

$lastSignature = $null
$lastWasHealthy = $false
$pendingUnhealthySignature = $null
$pendingUnhealthyMessage = $null
$pendingUnhealthySince = $null
$recoverySignature = $null
$recoverySince = $null
$recoveryLastAttemptAt = $null
$recoveryAttempts = 0
$loopCount = 0

while ($true) {
    $loopCount += 1
    try {
        $summary = Get-Summary
        $Host.UI.RawUI.WindowTitle = Get-WindowTitle -Summary $summary
        $signature = Get-StateSignature -Summary $summary
        $isHealthy = (@($summary.warnings).Count -eq 0)
        $stateMessage = Get-StateMessage -Summary $summary
        $recoveryPlan = Get-RecoveryPlan -Summary $summary

        if ($isHealthy) {
            $pendingUnhealthySignature = $null
            $pendingUnhealthyMessage = $null
            $pendingUnhealthySince = $null
            $recoverySignature = $null
            $recoverySince = $null
            $recoveryLastAttemptAt = $null
            $recoveryAttempts = 0
            if ($signature -ne $lastSignature) {
                if (-not $lastWasHealthy) {
                    Write-MonitorLine "Recovered. Data collection is healthy again." "OK"
                }
                $lastSignature = $signature
            }
        } else {
            if ($recoveryPlan.has_plan) {
                if ($recoverySignature -ne $recoveryPlan.signature) {
                    $recoverySignature = $recoveryPlan.signature
                    $recoverySince = Get-Date
                    $recoveryLastAttemptAt = $null
                    $recoveryAttempts = 0
                }

                $shouldAttemptRecovery = (
                    $recoveryAttempts -lt $RecoveryMaxAttempts -and (
                        -not $recoveryLastAttemptAt -or
                        (((Get-Date) - $recoveryLastAttemptAt).TotalSeconds -ge $RecoveryCooldownSec)
                    )
                )
                if ($shouldAttemptRecovery) {
                    $recoveryAttempts += 1
                    $recoveryLastAttemptAt = Get-Date
                    $planText = Get-RecoveryDescription -Plan $recoveryPlan
                    Write-MonitorLine "Auto-recovery attempt ${recoveryAttempts}/${RecoveryMaxAttempts} | $planText" "WARN"
                    Invoke-RecoveryPlan -Plan $recoveryPlan
                    Start-Sleep -Seconds 2
                }

                if ($recoverySince -and (((Get-Date) - $recoverySince).TotalSeconds -ge $RecoveryFailClosedSec) -and $recoveryAttempts -ge $RecoveryMaxAttempts) {
                    $planText = Get-RecoveryDescription -Plan $recoveryPlan
                    Write-MonitorLine "Monitor fail-closed: collectors could not be brought to a healthy state. $planText" "ERROR"
                    exit 1
                }
            } else {
                $recoverySignature = $null
                $recoverySince = $null
                $recoveryLastAttemptAt = $null
                $recoveryAttempts = 0
            }
        }

        if (-not $isHealthy -and $signature -ne $lastSignature) {
            if ($pendingUnhealthySignature -ne $signature) {
                $pendingUnhealthySignature = $signature
                $pendingUnhealthyMessage = $stateMessage
                $pendingUnhealthySince = Get-Date
            } elseif ($pendingUnhealthySince -and (((Get-Date) - $pendingUnhealthySince).TotalSeconds -ge [Math]::Max(10, $PollSec))) {
                Write-MonitorLine $pendingUnhealthyMessage "WARN"
                $lastSignature = $signature
                $pendingUnhealthySignature = $null
                $pendingUnhealthyMessage = $null
                $pendingUnhealthySince = $null
            }
        } elseif ($isHealthy -and -not $lastWasHealthy) {
            Write-MonitorLine "Recovered. Data collection is healthy again." "OK"
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
