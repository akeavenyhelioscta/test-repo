# Registers the ICE next-day gas settles Task Scheduler job.
# Fires every 15 min from 06:00 to 10:00 MT (last fire 09:45), daily. The
# Python orchestration's weekday gate backstops weekend misfires.
#
# Requires: Administrator. ICE XL + conda env `helioscta-pjm-da-dev` on the host.

$condaPath   = "$env:USERPROFILE\miniconda3\Scripts\activate.bat"
$condaEnv    = "helioscta-pjm-da-dev"
$repoRoot    = (Resolve-Path "$PSScriptRoot\..\..\..\..").Path
$moduleName  = "backend.orchestration.ice_python.next_day_gas"

$cmdArgs = "/c `"call `"$condaPath`" $condaEnv && cd /d `"$repoRoot`" && python -m $moduleName`""

$action = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument $cmdArgs

# Every 15 min from 06:00 for 4 hours (fires at 06:00, 06:15, …, 09:45 MT).
# Weekday gating lives in Python.
# Repetition is copied from a throwaway -Once trigger because Windows
# PowerShell 5.1 won't let you set .Repetition.Interval directly on a -Daily.
$trigger = New-ScheduledTaskTrigger -Daily -At "06:00"
$trigger.Repetition = (New-ScheduledTaskTrigger -Once -At "06:00" `
    -RepetitionInterval (New-TimeSpan -Minutes 15) `
    -RepetitionDuration (New-TimeSpan -Hours 4)).Repetition

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 30)

Register-ScheduledTask `
    -TaskName "Next Day Gas" `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -TaskPath "\PJM DA\ICE Python\" `
    -Force
