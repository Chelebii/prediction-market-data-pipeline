param(
    [ValidateSet('start','stop','restart')]
    [string]$Action = 'restart',
    [string]$Bot = '',
    [switch]$Force
)

$ErrorActionPreference = 'Stop'
$base = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $base '..\..')
Set-Location $repoRoot

$botMap = @{
    '5min' = @{
        Dir = 'polymarket_paper_bot_5min'
        Script = 'manager.py'
        Lock = 'manager.lock'
        WorkerPattern = 'polymarket_paper_bot_5min\\runs|polymarket_paper_bot_5min\\\\runs|polymarket_paper_bot_5min.*manager\.py'
    }
    'btc5scan' = @{
        Dir = 'polymarket_scanner'
        Script = 'btc_5min_clob_scanner.py'
        Lock = 'btc_5min_clob_scanner.lock'
        WorkerPattern = 'btc_5min_clob_scanner\.py'
    }
}

$stackOrder = @('btc5scan', '5min')

function Get-LiveBots {
    $liveBots = @()
    $envFile = Join-Path $repoRoot 'polymarket_paper_bot_5min\.env'
    if (Test-Path $envFile) {
        $mode = Select-String -Path $envFile -Pattern '^TRADING_MODE\s*=\s*(.+)' -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($mode -and $mode.Matches[0].Groups[1].Value.Trim().ToLower() -eq 'live') {
            $liveBots += '5min'
        }
    }
    return $liveBots
}

function Assert-LiveSafety {
    param([string]$actionName)
    $liveBots = Get-LiveBots
    if ($liveBots.Count -gt 0 -and -not $Force) {
        Write-Host ("`n*** WARNING: {0} bot(s) are LIVE. Use -Force for stack-wide {1}. ***`n" -f $liveBots.Count, $actionName) -ForegroundColor Red
        exit 1
    }
}

function Get-LockPid {
    param([string]$lockPath)
    if (-not (Test-Path $lockPath)) { return $null }
    try {
        $content = Get-Content $lockPath -Raw -ErrorAction SilentlyContinue
        if (-not $content) { return $null }
        $trimmed = $content.Trim()
        if ($trimmed -match '^\d+$') { return [int]$trimmed }
        $json = $trimmed | ConvertFrom-Json -ErrorAction SilentlyContinue
        if ($json -and $json.pid) { return [int]$json.pid }
    } catch {}
    return $null
}

function Test-PidAlive {
    param([int]$procId)
    if (-not $procId) { return $false }
    $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $procId" -ErrorAction SilentlyContinue
    return ($proc -and ($proc.Name -eq 'python.exe' -or $proc.Name -eq 'pythonw.exe'))
}

function Stop-SingleBot {
    param([string]$botKey)

    if (-not $botMap.ContainsKey($botKey)) {
        Write-Host ("Unknown bot: {0}" -f $botKey) -ForegroundColor Red
        return
    }

    $entry = $botMap[$botKey]
    $botDir = Join-Path $repoRoot $entry.Dir
    $lockPath = Join-Path $botDir $entry.Lock
    $workerPattern = $entry.WorkerPattern

    Write-Host ("Stopping {0}..." -f $botKey) -ForegroundColor Yellow

    $lockPid = Get-LockPid -lockPath $lockPath
    if ($lockPid -and (Test-PidAlive -procId $lockPid)) {
        Stop-Process -Id $lockPid -Force -ErrorAction SilentlyContinue
        Write-Host ("  PID {0} stopped from lock file." -f $lockPid) -ForegroundColor DarkGray
    }

    $workerProcs = @(Get-CimInstance Win32_Process | Where-Object {
        ($_.Name -eq 'python.exe' -or $_.Name -eq 'pythonw.exe') -and $_.CommandLine -and $_.CommandLine -match $workerPattern
    })
    foreach ($proc in $workerProcs) {
        Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
        Write-Host ("  Worker PID {0} stopped." -f $proc.ProcessId) -ForegroundColor DarkGray
    }

    Start-Sleep -Milliseconds 600

    if (Test-Path $lockPath) {
        Remove-Item $lockPath -Force -ErrorAction SilentlyContinue
    }

    if ($botKey -eq '5min') {
        $runsDir = Join-Path $botDir 'runs'
        if (Test-Path $runsDir) {
            Get-ChildItem $runsDir -Directory -ErrorAction SilentlyContinue | ForEach-Object {
                $botLock = Join-Path $_.FullName 'bot.lock'
                if (Test-Path $botLock) {
                    Remove-Item $botLock -Force -ErrorAction SilentlyContinue
                }
            }
        }
    }

    Write-Host ("{0} stopped." -f $botKey) -ForegroundColor Green
}

function Start-SingleBot {
    param([string]$botKey)

    if (-not $botMap.ContainsKey($botKey)) {
        Write-Host ("Unknown bot: {0}" -f $botKey) -ForegroundColor Red
        return
    }

    $entry = $botMap[$botKey]
    $botDir = Join-Path $repoRoot $entry.Dir
    $scriptPath = Join-Path $botDir $entry.Script
    $lockPath = Join-Path $botDir $entry.Lock

    if (-not (Test-Path $botDir)) {
        Write-Host ("Bot directory not found: {0}" -f $botDir) -ForegroundColor Red
        return
    }
    if (-not (Test-Path $scriptPath)) {
        Write-Host ("Script not found: {0}" -f $scriptPath) -ForegroundColor Red
        return
    }

    $existingPid = Get-LockPid -lockPath $lockPath
    if ($existingPid -and (Test-PidAlive -procId $existingPid)) {
        Write-Host ("{0} already running (PID: {1})." -f $botKey, $existingPid) -ForegroundColor Cyan
        return
    }

    $workerProcs = @(Get-CimInstance Win32_Process | Where-Object {
        ($_.Name -eq 'python.exe' -or $_.Name -eq 'pythonw.exe') -and $_.CommandLine -and $_.CommandLine -match $entry.WorkerPattern
    })
    if ($workerProcs.Count -gt 0) {
        Write-Host ("{0} already running (worker PID: {1})." -f $botKey, $workerProcs[0].ProcessId) -ForegroundColor Cyan
        return
    }

    if (Test-Path $lockPath) {
        Remove-Item $lockPath -Force -ErrorAction SilentlyContinue
    }

    Write-Host ("Starting {0}..." -f $botKey) -ForegroundColor Yellow
    Start-Process -FilePath python -ArgumentList $entry.Script -WorkingDirectory $botDir -WindowStyle Hidden
    Write-Host ("{0} started." -f $botKey) -ForegroundColor Green
}

function Restart-SingleBot {
    param([string]$botKey)
    Stop-SingleBot -botKey $botKey
    Start-Sleep -Seconds 2
    Start-SingleBot -botKey $botKey
}

function Stop-BotStack {
    Assert-LiveSafety -actionName 'stop'
    foreach ($botKey in $stackOrder) {
        Stop-SingleBot -botKey $botKey
    }
}

function Start-BotStack {
    foreach ($botKey in $stackOrder) {
        Start-SingleBot -botKey $botKey
        Start-Sleep -Seconds 1
    }
}

if ($Bot -ne '') {
    $botList = $Bot -split ',' | ForEach-Object { $_.Trim().ToLower() }
    switch ($Action) {
        'stop' {
            foreach ($botKey in $botList) { Stop-SingleBot -botKey $botKey }
        }
        'start' {
            foreach ($botKey in $botList) { Start-SingleBot -botKey $botKey }
        }
        'restart' {
            foreach ($botKey in $botList) { Restart-SingleBot -botKey $botKey }
        }
    }
} else {
    switch ($Action) {
        'stop' { Stop-BotStack }
        'start' { Start-BotStack }
        'restart' {
            Stop-BotStack
            Start-Sleep -Seconds 2
            Start-BotStack
        }
    }
}
