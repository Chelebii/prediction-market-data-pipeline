param(
    [ValidateSet('register','unregister','status')]
    [string]$Action = 'status'
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir '..\..')
Set-Location $repoRoot

$pythonExe = (Get-Command python).Source
$controlScript = Join-Path $repoRoot 'control\scripts\btc5m_collection_control.ps1'
$startupSource = Join-Path $repoRoot 'control\scripts\start_btc5m_collectors.cmd'
$startupDir = Join-Path $env:APPDATA 'Microsoft\Windows\Start Menu\Programs\Startup'
$startupTarget = Join-Path $startupDir 'start_btc5m_collectors.cmd'
$healthScript = Join-Path $repoRoot 'scripts\btc5m_healthcheck.py'
$auditScript = Join-Path $repoRoot 'scripts\btc5m_audit_dataset.py'
$backupScript = Join-Path $repoRoot 'scripts\btc5m_backup_dataset.py'

$tasks = @(
    @{
        Name = '5minbots BTC5M Health Check'
        Schedule = '/SC MINUTE /MO 5'
        Command = "$pythonExe $healthScript"
    },
    @{
        Name = '5minbots BTC5M Dataset Audit'
        Schedule = '/SC MINUTE /MO 15'
        Command = "$pythonExe $auditScript --lookback-hours 48 --max-markets 250 --include-active"
    },
    @{
        Name = '5minbots BTC5M Dataset Backup'
        Schedule = '/SC HOURLY /MO 1'
        Command = "$pythonExe $backupScript"
    }
)

function Invoke-Schtasks {
    param([string]$Arguments)
    $full = "schtasks $Arguments"
    cmd.exe /c $full
}

switch ($Action) {
    'register' {
        New-Item -ItemType Directory -Force -Path $startupDir | Out-Null
        Copy-Item $startupSource $startupTarget -Force
        foreach ($task in $tasks) {
            Invoke-Schtasks "/Create /TN `"$($task.Name)`" $($task.Schedule) /TR `"$($task.Command)`" /F"
        }
    }
    'unregister' {
        if (Test-Path $startupTarget) {
            Remove-Item $startupTarget -Force -ErrorAction SilentlyContinue
        }
        foreach ($task in $tasks) {
            Invoke-Schtasks "/Delete /TN `"$($task.Name)`" /F"
        }
    }
    'status' {
        Write-Host "`n=== Startup folder entry ===" -ForegroundColor Cyan
        if (Test-Path $startupTarget) {
            Get-Item $startupTarget | Select-Object FullName,Length,LastWriteTime
        } else {
            Write-Host "Startup entry missing." -ForegroundColor DarkYellow
        }
        foreach ($task in $tasks) {
            Write-Host "`n=== $($task.Name) ===" -ForegroundColor Cyan
            cmd.exe /c "schtasks /Query /FO LIST /TN `"$($task.Name)`""
        }
    }
}
