param(
    [ValidateSet('register','unregister','status')]
    [string]$Action = 'status'
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir '..\..')
Set-Location $repoRoot

$controlScript = Join-Path $repoRoot 'control\scripts\btc5m_collection_control.ps1'
$startupSource = Join-Path $repoRoot 'control\scripts\start_btc5m_collectors.cmd'
$startupDir = Join-Path $env:APPDATA 'Microsoft\Windows\Start Menu\Programs\Startup'
$startupTarget = Join-Path $startupDir 'start_btc5m_collectors.cmd'
$healthRunner = Join-Path $repoRoot 'control\scripts\run_btc5m_healthcheck.cmd'
$auditRunner = Join-Path $repoRoot 'control\scripts\run_btc5m_dataset_audit.cmd'
$backupRunner = Join-Path $repoRoot 'control\scripts\run_btc5m_backup_dataset.cmd'
$healthHiddenRunner = Join-Path $repoRoot 'control\scripts\run_btc5m_healthcheck_hidden.vbs'
$auditHiddenRunner = Join-Path $repoRoot 'control\scripts\run_btc5m_dataset_audit_hidden.vbs'
$backupHiddenRunner = Join-Path $repoRoot 'control\scripts\run_btc5m_backup_dataset_hidden.vbs'
$wscriptExe = Join-Path $env:SystemRoot 'System32\wscript.exe'

$tasks = @(
    @{
        Name = '5minbots BTC5M Health Check'
        ScheduleArgs = @('/SC', 'MINUTE', '/MO', '5', '/ST', '00:03')
        Command = '"' + $wscriptExe + '" //B //Nologo "' + $healthHiddenRunner + '"'
    },
    @{
        Name = '5minbots BTC5M Dataset Audit'
        ScheduleArgs = @('/SC', 'MINUTE', '/MO', '15', '/ST', '00:01')
        Command = '"' + $wscriptExe + '" //B //Nologo "' + $auditHiddenRunner + '"'
    },
    @{
        Name = '5minbots BTC5M Dataset Backup'
        ScheduleArgs = @('/SC', 'HOURLY', '/MO', '6', '/ST', '00:10')
        Command = '"' + $wscriptExe + '" //B //Nologo "' + $backupHiddenRunner + '"'
    }
)

function Invoke-Schtasks {
    param([string[]]$Arguments)

    & schtasks.exe @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "schtasks failed: $($Arguments -join ' ')"
    }
}

function Set-TaskOperationalSettings {
    param([string]$TaskName)

    $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
    $settings = $task.Settings
    $settings.DisallowStartIfOnBatteries = $false
    $settings.StopIfGoingOnBatteries = $false
    $settings.StartWhenAvailable = $true
    Set-ScheduledTask -TaskName $TaskName -Settings $settings | Out-Null
}

switch ($Action) {
    'register' {
        New-Item -ItemType Directory -Force -Path $startupDir | Out-Null
        Copy-Item $startupSource $startupTarget -Force
        foreach ($task in $tasks) {
            $args = @('/Create', '/TN', $task.Name) + $task.ScheduleArgs + @('/TR', $task.Command, '/F')
            Invoke-Schtasks $args
            Set-TaskOperationalSettings -TaskName $task.Name
        }
    }
    'unregister' {
        if (Test-Path $startupTarget) {
            Remove-Item $startupTarget -Force -ErrorAction SilentlyContinue
        }
        foreach ($task in $tasks) {
            Invoke-Schtasks @('/Delete', '/TN', $task.Name, '/F')
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
            & schtasks.exe /Query /FO LIST /TN $task.Name
        }
    }
}
