param(
    [switch]$EmitJson,
    [switch]$Quiet
)

$ErrorActionPreference = 'Stop'

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

$basePythonExe = (Get-Command python -ErrorAction Stop).Source
$pythonDir = Split-Path -Parent $basePythonExe

$collectorDefs = [ordered]@{
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

    $mode = 'existing'
    if (-not (Test-Path $targetPath)) {
        try {
            New-Item -ItemType HardLink -Path $targetPath -Target $basePythonExe -Force | Out-Null
            $mode = 'hardlink'
        } catch {
            Copy-Item -Path $basePythonExe -Destination $targetPath -Force
            $mode = 'copy'
        }
    }

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
foreach ($entry in $collectorDefs.GetEnumerator()) {
    $results[$entry.Key] = Ensure-CollectorExe -Name $entry.Key -Definition $entry.Value
}

if ($EmitJson) {
    $results | ConvertTo-Json -Depth 6 -Compress
}
