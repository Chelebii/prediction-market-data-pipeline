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
        Write-MonitorLine "Monitor zaten acik. Ikinci pencere kapatiliyor." "WARN"
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
    if ($seconds -lt 60) { return "$seconds sn" }

    $minutes = [math]::Floor($seconds / 60)
    $remain = $seconds % 60
    if ($minutes -lt 60) { return "$minutes dk $remain sn" }

    $hours = [math]::Floor($minutes / 60)
    $minutes = $minutes % 60
    if ($hours -lt 24) { return "$hours sa $minutes dk" }

    $days = [math]::Floor($hours / 24)
    $hours = $hours % 24
    return "$days gun $hours sa"
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

function Get-WarningAgeSec {
    param([string]$Warning)

    if (-not $Warning) { return $null }
    if ($Warning -match ':(\d+)s$') {
        return [int]$matches[1]
    }
    return $null
}

function Get-WindowTitle {
    param($Summary)

    $warnings = @($Summary.warnings)
    $snapshotAge = if ($null -ne $Summary.freshness.snapshot_age_sec) { [int]$Summary.freshness.snapshot_age_sec } else { -1 }
    $referenceAge = if ($null -ne $Summary.freshness.reference_age_sec) { [int]$Summary.freshness.reference_age_sec } else { -1 }
    $auditAge = if ($null -ne $Summary.freshness.audit_age_sec) { [int]$Summary.freshness.audit_age_sec } else { -1 }
    if ($warnings.Count -gt 0) {
        return "BTC5M Monitor | Sorun var | snapshot=$(Format-ShortDuration $snapshotAge) reference=$(Format-ShortDuration $referenceAge) audit=$(Format-ShortDuration $auditAge)"
    }
    return "BTC5M Monitor | Veri toplaniyor | snapshot=$(Format-ShortDuration $snapshotAge) reference=$(Format-ShortDuration $referenceAge) audit=$(Format-ShortDuration $auditAge)"
}

function Convert-WarningToPlainText {
    param([string]$Warning)

    if (-not $Warning) { return "Bilinmeyen uyari" }
    if ($Warning -like 'scanner_collector_not_running') { return 'Scanner calismiyor.' }
    if ($Warning -like 'reference_collector_not_running') { return 'Reference collector calismiyor.' }
    if ($Warning -like 'resolution_collector_not_running') { return 'Resolution collector calismiyor.' }
    if ($Warning -like 'snapshot_stale:*') { return "Scanner yeni veri yazmiyor ($(Format-PlainDuration (Get-WarningAgeSec $Warning)))." }
    if ($Warning -like 'reference_stale:*') { return "Reference veri akmiyor ($(Format-PlainDuration (Get-WarningAgeSec $Warning)))." }
    if ($Warning -like 'audit_stale:*') { return "Audit guncellenmedi ($(Format-PlainDuration (Get-WarningAgeSec $Warning)))." }
    if ($Warning -like 'backup_stale:*') { return "Backup gecikmis ($(Format-PlainDuration (Get-WarningAgeSec $Warning)))." }
    if ($Warning -like 'health_status_stale:*') { return "Health durumu guncellenmedi ($(Format-PlainDuration (Get-WarningAgeSec $Warning)))." }
    if ($Warning -like 'health_issue:*') { return ("Saglik problemi: " + $Warning.Substring(13)) }
    if ($Warning -like 'health_warning:*') { return ("Health warning: " + $Warning.Substring(15)) }
    if ($Warning -like 'scanner_collector_errors:*') { return ("Scanner son run'da hata verdi (" + ($Warning -split ':', 2)[1] + ").") }
    if ($Warning -like 'reference_collector_errors:*') { return ("Reference collector son run'da hata verdi (" + ($Warning -split ':', 2)[1] + ").") }
    if ($Warning -like 'resolution_collector_errors:*') { return ("Resolution collector son run'da hata verdi (" + ($Warning -split ':', 2)[1] + ").") }
    if ($Warning -eq 'latest_audit_failed') { return 'Son genel audit basarisiz. Bu anlik toplama bozuldu demek olmayabilir.' }
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
        return "Veri toplaniyor. Scanner=${scannerImage}, Reference=${referenceImage}, Resolution=${resolutionImage}"
    }

    $nonStaleWarnings = @($warnings | Where-Object { $_ -notmatch '^(audit_stale|health_status_stale):' })
    if ($nonStaleWarnings.Count -eq 0) {
        $auditAge = Format-PlainDuration (Get-WarningAgeSec ($warnings | Where-Object { $_ -like 'audit_stale:*' } | Select-Object -First 1))
        $healthAge = Format-PlainDuration (Get-WarningAgeSec ($warnings | Where-Object { $_ -like 'health_status_stale:*' } | Select-Object -First 1))
        return "Veri akiyor ama kontrol raporlari gecikmis. Audit=$auditAge, Health=$healthAge."
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
    return "Sorun var: " + ($plainWarnings -join ' | ')
}

if (-not $NoStart) {
    try {
        & $controlScript -Action start | Out-Null
        Write-MonitorLine "collector startup check tamamlandi" "OK"
    } catch {
        Write-MonitorLine "collector startup check failed: $($_.Exception.Message)" "ERROR"
    }
}

Acquire-MonitorLock

Write-MonitorLine "Monitor hazir. Her sey normalse sessiz kalacak; sorun olursa buraya yazacak." "INFO"

$lastSignature = $null
$lastWasHealthy = $false
$pendingUnhealthySignature = $null
$pendingUnhealthyMessage = $null
$pendingUnhealthySince = $null
$loopCount = 0

while ($true) {
    $loopCount += 1
    try {
        $summary = Get-Summary
        $Host.UI.RawUI.WindowTitle = Get-WindowTitle -Summary $summary
        $signature = Get-StateSignature -Summary $summary
        $isHealthy = (@($summary.warnings).Count -eq 0)
        $stateMessage = Get-StateMessage -Summary $summary

        if ($isHealthy) {
            $pendingUnhealthySignature = $null
            $pendingUnhealthyMessage = $null
            $pendingUnhealthySince = $null
            if ($signature -ne $lastSignature) {
                if (-not $lastWasHealthy) {
                    Write-MonitorLine "Sorun yok. Veri toplama normale dondu." "OK"
                }
                $lastSignature = $signature
            }
        } elseif ($signature -ne $lastSignature) {
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
            Write-MonitorLine "Sorun yok. Veri toplama normale dondu." "OK"
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
