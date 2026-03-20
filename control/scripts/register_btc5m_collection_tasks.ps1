param(
    [ValidateSet('register','unregister','status')]
    [string]$Action = 'status'
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir '..\..')
Set-Location $repoRoot

$startupDir = Join-Path $env:APPDATA 'Microsoft\Windows\Start Menu\Programs\Startup'
$startupTarget = Join-Path $startupDir 'start_prediction_market_data_pipeline_btc5m_collectors.cmd'
$healthRunner = Join-Path $repoRoot 'control\scripts\run_btc5m_healthcheck.cmd'
$auditRunner = Join-Path $repoRoot 'control\scripts\run_btc5m_dataset_audit.cmd'
$backupRunner = Join-Path $repoRoot 'control\scripts\run_btc5m_backup_dataset.cmd'
$healthHiddenRunner = Join-Path $repoRoot 'control\scripts\run_btc5m_healthcheck_hidden.vbs'
$auditHiddenRunner = Join-Path $repoRoot 'control\scripts\run_btc5m_dataset_audit_hidden.vbs'
$backupHiddenRunner = Join-Path $repoRoot 'control\scripts\run_btc5m_backup_dataset_hidden.vbs'
$wscriptExe = Join-Path $env:SystemRoot 'System32\wscript.exe'

$tasks = @(
    @{
        Name = 'Prediction Market Data Pipeline BTC5M Health Check'
        ScheduleArgs = @('/SC', 'MINUTE', '/MO', '5', '/ST', '00:03')
        Command = '"' + $wscriptExe + '" //B //Nologo "' + $healthHiddenRunner + '"'
    },
    @{
        Name = 'Prediction Market Data Pipeline BTC5M Dataset Audit'
        ScheduleArgs = @('/SC', 'MINUTE', '/MO', '15', '/ST', '00:01')
        Command = '"' + $wscriptExe + '" //B //Nologo "' + $auditHiddenRunner + '"'
    },
    @{
        Name = 'Prediction Market Data Pipeline BTC5M Dataset Backup'
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

function Remove-TaskIfExists {
    param([string]$TaskName)

    if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
        & schtasks.exe /Delete /TN $TaskName /F | Out-Null
    }
}

function Get-LegacyTaskNames {
    $expectedSuffixes = @(
        'BTC5M Health Check',
        'BTC5M Dataset Audit',
        'BTC5M Dataset Backup'
    )
    $currentNames = @($tasks | ForEach-Object { $_.Name })
    $legacyNames = @()
    foreach ($scheduledTask in @(Get-ScheduledTask -ErrorAction SilentlyContinue)) {
        if ($scheduledTask.TaskName -in $currentNames) {
            continue
        }
        foreach ($suffix in $expectedSuffixes) {
            if ($scheduledTask.TaskName -like "*$suffix") {
                $legacyNames += $scheduledTask.TaskName
                break
            }
        }
    }
    return @($legacyNames | Select-Object -Unique)
}

function Get-LegacyStartupTargets {
    return @(
        Get-ChildItem -Path $startupDir -Filter '*btc5m_collectors.cmd' -ErrorAction SilentlyContinue |
            Where-Object { $_.FullName -ne $startupTarget } |
            Select-Object -ExpandProperty FullName
    )
}

function Write-StartupStub {
    param(
        [string]$TargetPath,
        [string]$RepoRootPath
    )

    $startupScriptPath = Join-Path $repoRootPath 'control\scripts\start_btc5m_collectors.cmd'
    $content = @"
@echo off
setlocal
call "$startupScriptPath"
"@
    Set-Content -Path $TargetPath -Value $content -Encoding ASCII
}

function Set-TaskOperationalSettings {
    param([string]$TaskName)

    $settings = New-ScheduledTaskSettingsSet `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -StartWhenAvailable `
        -ExecutionTimeLimit (New-TimeSpan -Seconds 0) `
        -DontStopOnIdleEnd `
        -IdleDuration (New-TimeSpan -Minutes 10) `
        -IdleWaitTimeout (New-TimeSpan -Hours 1) `
        -MultipleInstances IgnoreNew `
        -Priority 7
    Set-ScheduledTask -TaskName $TaskName -Settings $settings | Out-Null
}

switch ($Action) {
    'register' {
        New-Item -ItemType Directory -Force -Path $startupDir | Out-Null
        foreach ($legacyTarget in (@(Get-LegacyStartupTargets) + $startupTarget | Select-Object -Unique)) {
            if (Test-Path $legacyTarget) {
                Remove-Item $legacyTarget -Force -ErrorAction SilentlyContinue
            }
        }
        Write-StartupStub -TargetPath $startupTarget -RepoRootPath $repoRoot.Path
        foreach ($legacyTaskName in @(Get-LegacyTaskNames)) {
            Remove-TaskIfExists -TaskName $legacyTaskName
        }
        foreach ($task in $tasks) {
            $args = @('/Create', '/TN', $task.Name) + $task.ScheduleArgs + @('/TR', $task.Command, '/F')
            Invoke-Schtasks $args
            Set-TaskOperationalSettings -TaskName $task.Name
        }
    }
    'unregister' {
        foreach ($legacyTarget in (@(Get-LegacyStartupTargets) + $startupTarget | Select-Object -Unique)) {
            if (Test-Path $legacyTarget) {
                Remove-Item $legacyTarget -Force -ErrorAction SilentlyContinue
            }
        }
        foreach ($legacyTaskName in @(Get-LegacyTaskNames)) {
            Remove-TaskIfExists -TaskName $legacyTaskName
        }
        foreach ($task in $tasks) {
            Remove-TaskIfExists -TaskName $task.Name
        }
    }
    'status' {
        Write-Host "`n=== Startup folder entry ===" -ForegroundColor Cyan
        $startupEntries = @(@(Get-LegacyStartupTargets) + $startupTarget | Select-Object -Unique | Where-Object { Test-Path $_ })
        if ($startupEntries.Count -gt 0) {
            Get-Item $startupEntries | Select-Object FullName,Length,LastWriteTime
        } else {
            Write-Host "Startup entry missing." -ForegroundColor DarkYellow
        }
        foreach ($taskName in @(@($tasks | ForEach-Object { $_.Name }) + @(Get-LegacyTaskNames) | Select-Object -Unique)) {
            if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
                Write-Host "`n=== $taskName ===" -ForegroundColor Cyan
                & schtasks.exe /Query /FO LIST /TN $taskName
            }
        }
    }
}
