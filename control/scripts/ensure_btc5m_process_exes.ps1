param(
    [switch]$EmitJson,
    [switch]$Quiet
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir '..\..')

function Write-EnsureLog {
    param(
        [string]$Message,
        [string]$Level = 'INFO'
    )

    if ($Quiet) {
        return
    }

    $color = switch ($Level) {
        'WARN' { 'Yellow' }
        'ERROR' { 'Red' }
        'OK' { 'Green' }
        default { 'Gray' }
    }
    Write-Host "BTC5M-EXE | $Message" -ForegroundColor $color
}

function Resolve-PythonCommandPath {
    try {
        return (Get-Command python -ErrorAction Stop).Source
    } catch {
        return $null
    }
}

function Get-VenvBasePythonExe {
    $pyvenvCfg = Join-Path $repoRoot '.venv\pyvenv.cfg'
    if (-not (Test-Path $pyvenvCfg)) {
        return $null
    }

    $cfgLines = Get-Content $pyvenvCfg -ErrorAction SilentlyContinue
    foreach ($line in $cfgLines) {
        if ($line -match '^\s*executable\s*=\s*(.+?)\s*$') {
            $candidate = $Matches[1].Trim()
            if ($candidate -and (Test-Path $candidate) -and ($candidate -notlike '*\Microsoft\WindowsApps\*')) {
                return [System.IO.Path]::GetFullPath($candidate)
            }
        }
    }

    foreach ($line in $cfgLines) {
        if ($line -match '^\s*home\s*=\s*(.+?)\s*$') {
            $homeDir = $Matches[1].Trim()
            if ($homeDir) {
                $candidate = Join-Path $homeDir 'python.exe'
                if (Test-Path $candidate) {
                    return [System.IO.Path]::GetFullPath($candidate)
                }
            }
        }
    }

    return $null
}

function Test-IsVenvRedirector {
    param([string]$CandidatePath)

    if (-not $CandidatePath) {
        return $false
    }

    try {
        $fullPath = [System.IO.Path]::GetFullPath($CandidatePath)
    } catch {
        return $false
    }

    $parentDir = Split-Path -Parent $fullPath
    $venvRoot = Split-Path -Parent $parentDir
    $cfgPath = Join-Path $venvRoot 'pyvenv.cfg'
    return (([System.IO.Path]::GetFileName($fullPath)).ToLowerInvariant() -eq 'python.exe' -and (Test-Path $cfgPath))
}

function Select-BasePythonExe {
    $candidates = @(
        [Environment]::GetEnvironmentVariable('BTC5M_BASE_PYTHON_EXE_PATH', 'Process'),
        [Environment]::GetEnvironmentVariable('BTC5M_BASE_PYTHON_EXE_PATH', 'User'),
        [Environment]::GetEnvironmentVariable('BTC5M_BASE_PYTHON_EXE_PATH', 'Machine'),
        (Get-VenvBasePythonExe),
        (& python -c "import sys; print(sys.executable)" 2>$null),
        (Resolve-PythonCommandPath),
        "$env:LOCALAPPDATA\Python\pythoncore-3.14-64\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python314\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe"
    )

    foreach ($candidate in $candidates) {
        if (-not $candidate) {
            continue
        }
        try {
            $fullPath = [System.IO.Path]::GetFullPath([string]$candidate)
        } catch {
            continue
        }
        if (-not (Test-Path $fullPath)) {
            continue
        }
        if ($fullPath -like '*\Microsoft\WindowsApps\*') {
            continue
        }
        if (Test-IsVenvRedirector -CandidatePath $fullPath) {
            continue
        }
        return $fullPath
    }

    throw 'Unable to resolve a usable Python executable for BTC5M process EXE creation.'
}

function Sync-ProcessExe {
    param(
        [string]$SourcePath,
        [string]$TargetPath
    )

    $sourceItem = Get-Item -LiteralPath $SourcePath -ErrorAction Stop
    $targetItem = Get-Item -LiteralPath $TargetPath -ErrorAction SilentlyContinue

    if (-not $targetItem) {
        Copy-Item -LiteralPath $SourcePath -Destination $TargetPath -Force
        return 'copy'
    }

    $needsRefresh = (
        $targetItem.Length -ne $sourceItem.Length -or
        $targetItem.LastWriteTimeUtc -lt $sourceItem.LastWriteTimeUtc
    )
    if ($needsRefresh) {
        Copy-Item -LiteralPath $SourcePath -Destination $TargetPath -Force
        return 'refresh'
    }

    return 'existing'
}

$basePythonExe = Select-BasePythonExe
$pythonDir = Split-Path -Parent $basePythonExe

$processDefs = [ordered]@{
    scanner = @{
        env_var = 'BTC5M_SCANNER_EXE_PATH'
        file_name = 'btc5m-scanner.exe'
    }
    reference = @{
        env_var = 'BTC5M_REFERENCE_EXE_PATH'
        file_name = 'btc5m-reference.exe'
    }
    resolution = @{
        env_var = 'BTC5M_RESOLUTION_EXE_PATH'
        file_name = 'btc5m-resolution.exe'
    }
    healthcheck = @{
        env_var = 'BTC5M_HEALTHCHECK_EXE_PATH'
        file_name = 'btc5m-healthcheck.exe'
    }
    audit = @{
        env_var = 'BTC5M_AUDIT_EXE_PATH'
        file_name = 'btc5m-dataset-audit.exe'
    }
    backup = @{
        env_var = 'BTC5M_BACKUP_EXE_PATH'
        file_name = 'btc5m-backup-dataset.exe'
    }
}

function Ensure-CollectorExe {
    param(
        [string]$Name,
        [hashtable]$Definition
    )

    $configuredPath = [Environment]::GetEnvironmentVariable($Definition.env_var, 'Process')
    if (-not $configuredPath) {
        $configuredPath = [Environment]::GetEnvironmentVariable($Definition.env_var, 'User')
    }
    if (-not $configuredPath) {
        $configuredPath = [Environment]::GetEnvironmentVariable($Definition.env_var, 'Machine')
    }
    if (-not $configuredPath) {
        $configuredPath = Join-Path $pythonDir $Definition.file_name
    }

    $targetPath = [System.IO.Path]::GetFullPath($configuredPath)
    $targetDir = Split-Path -Parent $targetPath
    if (-not (Test-Path $targetDir)) {
        New-Item -ItemType Directory -Path $targetDir -Force | Out-Null
    }

    $mode = Sync-ProcessExe -SourcePath $basePythonExe -TargetPath $targetPath

    $result = [ordered]@{
        name = $Name
        env_var = $Definition.env_var
        base_python_exe = $basePythonExe
        exe_path = $targetPath
        image_name = [System.IO.Path]::GetFileName($targetPath).ToLowerInvariant()
        mode = $mode
    }

    if ($mode -eq 'existing') {
        Write-EnsureLog "$Name executable ready | path=$targetPath | mode=existing" 'OK'
    } else {
        Write-EnsureLog "$Name executable created | path=$targetPath | mode=$mode" 'OK'
    }

    return $result
}

$results = [ordered]@{}
foreach ($entry in $processDefs.GetEnumerator()) {
    $results[$entry.Key] = Ensure-CollectorExe -Name $entry.Key -Definition $entry.Value
}

if ($EmitJson) {
    $results | ConvertTo-Json -Depth 6 -Compress
}
