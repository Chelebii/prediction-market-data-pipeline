param(
    [ValidateSet('start','stop','restart','status')]
    [string]$Action = 'status',
    [string]$Targets = 'scanner,reference,resolution'
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir '..\..')
Set-Location $repoRoot

$ensureExeScript = Join-Path $repoRoot 'control\scripts\ensure_btc5m_process_exes.ps1'
$exeMap = (& $ensureExeScript -EmitJson -Quiet | ConvertFrom-Json)
$fallbackPythonExe = (Get-Command python).Source
$venvSitePackages = Join-Path $repoRoot '.venv\Lib\site-packages'

$collectorMap = @{
    'scanner' = @{
        Script = 'polymarket_scanner\btc_5min_clob_scanner.py'
        WorkingDir = 'polymarket_scanner'
        Lock = 'polymarket_scanner\btc_5min_clob_scanner.lock'
        Pattern = 'btc_5min_clob_scanner\.py'
        ExeKey = 'scanner'
    }
    'reference' = @{
        Script = 'scripts\btc5m_reference_collector.py'
        WorkingDir = '.'
        Lock = 'runtime\locks\btc5m_reference_collector.lock'
        Pattern = 'btc5m_reference_collector\.py'
        ExeKey = 'reference'
    }
    'resolution' = @{
        Script = 'scripts\btc5m_resolution_collector.py'
        WorkingDir = '.'
        Lock = 'runtime\locks\btc5m_resolution_collector.lock'
        Pattern = 'btc5m_resolution_collector\.py'
        ExeKey = 'resolution'
    }
}

function Get-LockInfo {
    param([string]$LockPath)

    if (-not (Test-Path $LockPath)) { return $null }
    try {
        $raw = Get-Content $LockPath -Raw -ErrorAction Stop
        if (-not $raw) { return $null }
        $trimmed = $raw.Trim()
        if ($trimmed -match '^\d+$') {
            return @{
                pid = [int]$trimmed
                image_name = $null
                exe_path = $null
            }
        }
        $payload = $trimmed | ConvertFrom-Json -ErrorAction SilentlyContinue
        if ($payload -and $payload.pid) {
            return @{
                pid = [int]$payload.pid
                image_name = if ($payload.image_name) { [string]$payload.image_name } else { $null }
                exe_path = if ($payload.exe_path) { [string]$payload.exe_path } else { $null }
            }
        }
    } catch {}
    return $null
}

function Test-PidAlive {
    param(
        [int]$ProcId,
        [string]$ExpectedImageName = '',
        [string]$ExpectedExePath = ''
    )

    if (-not $ProcId) { return $false }
    try {
        $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $ProcId" -ErrorAction Stop
        if (-not $proc) { return $false }
        if ($ExpectedImageName -and $proc.Name -ne $ExpectedImageName) { return $false }
        if ($ExpectedExePath) {
            if (-not $proc.ExecutablePath) { return $true }
            return ([System.IO.Path]::GetFullPath($proc.ExecutablePath).ToLowerInvariant() -eq [System.IO.Path]::GetFullPath($ExpectedExePath).ToLowerInvariant())
        }
        return $true
    } catch {
        return $false
    }
}

function Get-WorkerProcesses {
    param(
        [string]$Pattern,
        [string]$ExpectedImageName = '',
        [string]$ExpectedExePath = ''
    )

    return @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
        $nameMatches = $false
        if ($ExpectedImageName) {
            $nameMatches = $_.Name -eq $ExpectedImageName
        } else {
            $nameMatches = ($_.Name -eq 'python.exe' -or $_.Name -eq 'pythonw.exe')
        }
        if (-not $nameMatches) { return $false }
        if ($ExpectedExePath -and $_.ExecutablePath) {
            $procExe = [System.IO.Path]::GetFullPath($_.ExecutablePath).ToLowerInvariant()
            $wantExe = [System.IO.Path]::GetFullPath($ExpectedExePath).ToLowerInvariant()
            if ($procExe -ne $wantExe) { return $false }
        }
        $_.CommandLine -and $_.CommandLine -match $Pattern
    })
}

function Get-CollectorExeInfo {
    param($Entry)

    $exeInfo = $exeMap.($Entry.ExeKey)
    if ($exeInfo -and $exeInfo.exe_path) {
        return @{
            path = [string]$exeInfo.exe_path
            image_name = [string]$exeInfo.image_name
        }
    }
    return @{
        path = $fallbackPythonExe
        image_name = [System.IO.Path]::GetFileName($fallbackPythonExe).ToLowerInvariant()
    }
}

function Get-OrDefault {
    param(
        $Value,
        $DefaultValue
    )

    if ($null -eq $Value -or [string]::IsNullOrWhiteSpace([string]$Value)) {
        return $DefaultValue
    }
    return $Value
}

function Set-Btc5mPythonPath {
    $entries = @($repoRoot.Path)
    if (Test-Path $venvSitePackages) {
        $entries += $venvSitePackages
    }

    $currentPythonPath = [Environment]::GetEnvironmentVariable('PYTHONPATH', 'Process')
    if ($currentPythonPath) {
        $entries += ($currentPythonPath -split ';')
    }

    $uniqueEntries = @()
    foreach ($entry in $entries) {
        if (-not $entry) {
            continue
        }
        try {
            $normalized = [System.IO.Path]::GetFullPath([string]$entry)
        } catch {
            $normalized = [string]$entry
        }
        if (-not $normalized) {
            continue
        }
        if ($uniqueEntries -notcontains $normalized) {
            $uniqueEntries += $normalized
        }
    }

    $previousValue = $currentPythonPath
    $env:PYTHONPATH = ($uniqueEntries -join ';')
    return $previousValue
}

function Restore-Btc5mPythonPath {
    param($PreviousValue)

    if ($null -eq $PreviousValue) {
        Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue
        return
    }
    $env:PYTHONPATH = [string]$PreviousValue
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
    $exeInfo = Get-CollectorExeInfo -Entry $entry

    $lockInfo = Get-LockInfo -LockPath $lockPath
    $expectedImage = Get-OrDefault $lockInfo.image_name $exeInfo.image_name
    $expectedExe = Get-OrDefault $lockInfo.exe_path $exeInfo.path
    if ($lockInfo -and (Test-PidAlive -ProcId $lockInfo.pid -ExpectedImageName $expectedImage -ExpectedExePath $expectedExe)) {
        Write-Host "$Name already running (PID $($lockInfo.pid))." -ForegroundColor Cyan
        return
    }

    $workers = Get-WorkerProcesses -Pattern $entry.Pattern -ExpectedImageName $exeInfo.image_name -ExpectedExePath $exeInfo.path
    if ($workers.Count -gt 0) {
        Write-Host "$Name already running (PID $($workers[0].ProcessId))." -ForegroundColor Cyan
        return
    }

    $legacyWorkers = Get-WorkerProcesses -Pattern $entry.Pattern
    if ($legacyWorkers.Count -gt 0) {
        Write-Host "$Name already running on legacy python.exe (PID $($legacyWorkers[0].ProcessId))." -ForegroundColor Cyan
        return
    }

    if (Test-Path $lockPath) {
        Remove-Item $lockPath -Force -ErrorAction SilentlyContinue
    }

    if (-not (Test-Path $exeInfo.path)) {
        throw "collector executable missing for ${Name}: $($exeInfo.path)"
    }

    Write-Host "Starting $Name with $($exeInfo.image_name)..." -ForegroundColor Yellow
    $previousPythonPath = Set-Btc5mPythonPath
    try {
        Start-Process -FilePath $exeInfo.path -ArgumentList $scriptPath -WorkingDirectory $workingDir -WindowStyle Hidden
    } finally {
        Restore-Btc5mPythonPath -PreviousValue $previousPythonPath
    }
    Start-Sleep -Seconds 1
    $newLockInfo = Get-LockInfo -LockPath $lockPath
    $newExpectedImage = Get-OrDefault $newLockInfo.image_name $exeInfo.image_name
    $newExpectedExe = Get-OrDefault $newLockInfo.exe_path $exeInfo.path
    if ($newLockInfo -and (Test-PidAlive -ProcId $newLockInfo.pid -ExpectedImageName $newExpectedImage -ExpectedExePath $newExpectedExe)) {
        Write-Host "$Name started (PID $($newLockInfo.pid), image=$newExpectedImage)." -ForegroundColor Green
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
    $exeInfo = Get-CollectorExeInfo -Entry $entry
    $lockInfo = Get-LockInfo -LockPath $lockPath

    Write-Host "Stopping $Name..." -ForegroundColor Yellow

    $expectedImage = Get-OrDefault $lockInfo.image_name $exeInfo.image_name
    $expectedExe = Get-OrDefault $lockInfo.exe_path $exeInfo.path
    if ($lockInfo -and (Test-PidAlive -ProcId $lockInfo.pid -ExpectedImageName $expectedImage -ExpectedExePath $expectedExe)) {
        Stop-Process -Id $lockInfo.pid -Force -ErrorAction SilentlyContinue
        Write-Host "  lock PID $($lockInfo.pid) stopped." -ForegroundColor DarkGray
    }

    $workers = Get-WorkerProcesses -Pattern $entry.Pattern -ExpectedImageName $exeInfo.image_name -ExpectedExePath $exeInfo.path
    foreach ($proc in $workers) {
        Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
        Write-Host "  worker PID $($proc.ProcessId) stopped." -ForegroundColor DarkGray
    }

    $legacyWorkers = Get-WorkerProcesses -Pattern $entry.Pattern
    foreach ($proc in $legacyWorkers) {
        if ($workers.ProcessId -contains $proc.ProcessId) { continue }
        Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
        Write-Host "  legacy worker PID $($proc.ProcessId) stopped." -ForegroundColor DarkGray
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
    $exeInfo = Get-CollectorExeInfo -Entry $entry
    $lockInfo = Get-LockInfo -LockPath $lockPath
    $workers = Get-WorkerProcesses -Pattern $entry.Pattern -ExpectedImageName $exeInfo.image_name -ExpectedExePath $exeInfo.path
    $isRunning = $false
    $pidText = '-'
    $imageText = $exeInfo.image_name

    $expectedImage = Get-OrDefault $lockInfo.image_name $exeInfo.image_name
    $expectedExe = Get-OrDefault $lockInfo.exe_path $exeInfo.path
    if ($lockInfo -and (Test-PidAlive -ProcId $lockInfo.pid -ExpectedImageName $expectedImage -ExpectedExePath $expectedExe)) {
        $isRunning = $true
        $pidText = [string]$lockInfo.pid
        if ($lockInfo.image_name) {
            $imageText = [string]$lockInfo.image_name
        }
    } elseif ($workers.Count -gt 0) {
        $isRunning = $true
        $pidText = [string]$workers[0].ProcessId
        $imageText = [string]$workers[0].Name
    } else {
        $legacyWorkers = Get-WorkerProcesses -Pattern $entry.Pattern
        if ($legacyWorkers.Count -gt 0) {
            $isRunning = $true
            $pidText = [string]$legacyWorkers[0].ProcessId
            $imageText = [string]$legacyWorkers[0].Name
        }
    }

    $status = if ($isRunning) { 'RUNNING' } else { 'STOPPED' }
    $color = if ($isRunning) { 'Green' } else { 'DarkYellow' }
    Write-Host ("{0,-12} {1,-8} pid={2} image={3} lock={4}" -f $Name, $status, $pidText, $imageText, $lockPath) -ForegroundColor $color
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
