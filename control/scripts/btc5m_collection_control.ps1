param(
    [ValidateSet('start','stop','restart','status')]
    [string]$Action = 'status',
    [string]$Targets = 'scanner,reference,resolution'
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir '..\..')
Set-Location $repoRoot

$pythonExe = (Get-Command python).Source

$collectorMap = @{
    'scanner' = @{
        Script = 'polymarket_scanner\btc_5min_clob_scanner.py'
        WorkingDir = 'polymarket_scanner'
        Lock = 'polymarket_scanner\btc_5min_clob_scanner.lock'
        Pattern = 'btc_5min_clob_scanner\.py'
    }
    'reference' = @{
        Script = 'scripts\btc5m_reference_collector.py'
        WorkingDir = '.'
        Lock = 'runtime\locks\btc5m_reference_collector.lock'
        Pattern = 'btc5m_reference_collector\.py'
    }
    'resolution' = @{
        Script = 'scripts\btc5m_resolution_collector.py'
        WorkingDir = '.'
        Lock = 'runtime\locks\btc5m_resolution_collector.lock'
        Pattern = 'btc5m_resolution_collector\.py'
    }
}

function Get-LockPid {
    param([string]$LockPath)

    if (-not (Test-Path $LockPath)) { return $null }
    try {
        $raw = Get-Content $LockPath -Raw -ErrorAction Stop
        if (-not $raw) { return $null }
        $trimmed = $raw.Trim()
        if ($trimmed -match '^\d+$') {
            return [int]$trimmed
        }
        $payload = $trimmed | ConvertFrom-Json -ErrorAction SilentlyContinue
        if ($payload -and $payload.pid) {
            return [int]$payload.pid
        }
    } catch {}
    return $null
}

function Test-PidAlive {
    param([int]$ProcId)

    if (-not $ProcId) { return $false }
    try {
        $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $ProcId" -ErrorAction Stop
        return ($proc -and ($proc.Name -eq 'python.exe' -or $proc.Name -eq 'pythonw.exe'))
    } catch {
        return $false
    }
}

function Get-WorkerProcesses {
    param([string]$Pattern)

    return @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
        ($_.Name -eq 'python.exe' -or $_.Name -eq 'pythonw.exe') -and $_.CommandLine -and $_.CommandLine -match $Pattern
    })
}

function Start-Collector {
    param([string]$Name)

    if (-not $collectorMap.ContainsKey($Name)) {
        Write-Host "Unknown target: $Name" -ForegroundColor Red
        return
    }

    $entry = $collectorMap[$Name]
    $lockPath = Join-Path $repoRoot $entry.Lock
    $scriptPath = Join-Path $repoRoot $entry.Script
    $workingDir = Resolve-Path (Join-Path $repoRoot $entry.WorkingDir)

    $lockPid = Get-LockPid -LockPath $lockPath
    if ($lockPid -and (Test-PidAlive -ProcId $lockPid)) {
        Write-Host "$Name already running (PID $lockPid)." -ForegroundColor Cyan
        return
    }

    $workers = Get-WorkerProcesses -Pattern $entry.Pattern
    if ($workers.Count -gt 0) {
        Write-Host "$Name already running (PID $($workers[0].ProcessId))." -ForegroundColor Cyan
        return
    }

    if (Test-Path $lockPath) {
        Remove-Item $lockPath -Force -ErrorAction SilentlyContinue
    }

    Write-Host "Starting $Name..." -ForegroundColor Yellow
    Start-Process -FilePath $pythonExe -ArgumentList $scriptPath -WorkingDirectory $workingDir -WindowStyle Hidden
    Start-Sleep -Seconds 1
    $newPid = Get-LockPid -LockPath $lockPath
    if ($newPid -and (Test-PidAlive -ProcId $newPid)) {
        Write-Host "$Name started (PID $newPid)." -ForegroundColor Green
    } else {
        Write-Host "$Name start command issued; lock not observed yet." -ForegroundColor DarkYellow
    }
}

function Stop-Collector {
    param([string]$Name)

    if (-not $collectorMap.ContainsKey($Name)) {
        Write-Host "Unknown target: $Name" -ForegroundColor Red
        return
    }

    $entry = $collectorMap[$Name]
    $lockPath = Join-Path $repoRoot $entry.Lock
    $lockPid = Get-LockPid -LockPath $lockPath

    Write-Host "Stopping $Name..." -ForegroundColor Yellow

    if ($lockPid -and (Test-PidAlive -ProcId $lockPid)) {
        Stop-Process -Id $lockPid -Force -ErrorAction SilentlyContinue
        Write-Host "  lock PID $lockPid stopped." -ForegroundColor DarkGray
    }

    $workers = Get-WorkerProcesses -Pattern $entry.Pattern
    foreach ($proc in $workers) {
        Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
        Write-Host "  worker PID $($proc.ProcessId) stopped." -ForegroundColor DarkGray
    }

    Start-Sleep -Milliseconds 600
    if (Test-Path $lockPath) {
        Remove-Item $lockPath -Force -ErrorAction SilentlyContinue
    }
    Write-Host "$Name stopped." -ForegroundColor Green
}

function Show-CollectorStatus {
    param([string]$Name)

    if (-not $collectorMap.ContainsKey($Name)) {
        Write-Host "Unknown target: $Name" -ForegroundColor Red
        return
    }

    $entry = $collectorMap[$Name]
    $lockPath = Join-Path $repoRoot $entry.Lock
    $lockPid = Get-LockPid -LockPath $lockPath
    $workers = Get-WorkerProcesses -Pattern $entry.Pattern
    $isRunning = $false
    $pidText = '-'

    if ($lockPid -and (Test-PidAlive -ProcId $lockPid)) {
        $isRunning = $true
        $pidText = [string]$lockPid
    } elseif ($workers.Count -gt 0) {
        $isRunning = $true
        $pidText = [string]$workers[0].ProcessId
    }

    $status = if ($isRunning) { 'RUNNING' } else { 'STOPPED' }
    $color = if ($isRunning) { 'Green' } else { 'DarkYellow' }
    Write-Host ("{0,-12} {1,-8} pid={2} lock={3}" -f $Name, $status, $pidText, $lockPath) -ForegroundColor $color
}

$targetList = @()
foreach ($rawName in ($Targets -split ',')) {
    $name = $rawName.Trim().ToLower()
    if ($name) {
        $targetList += $name
    }
}

if ($targetList.Count -eq 0) {
    $targetList = @('scanner', 'reference', 'resolution')
}

switch ($Action) {
    'start' {
        foreach ($name in $targetList) { Start-Collector -Name $name }
    }
    'stop' {
        foreach ($name in $targetList) { Stop-Collector -Name $name }
    }
    'restart' {
        foreach ($name in $targetList) { Stop-Collector -Name $name }
        Start-Sleep -Seconds 2
        foreach ($name in $targetList) { Start-Collector -Name $name }
    }
    'status' {
        foreach ($name in $targetList) { Show-CollectorStatus -Name $name }
    }
}
