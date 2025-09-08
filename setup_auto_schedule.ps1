# PowerShell script to create Windows Task Scheduler for ETF Data Updates
# Run as Administrator

$taskName = "ETF_Nightly_Update"
$description = "Automated ETF and Stock data update at 2:00 AM daily"
$scriptPath = "C:\etf_data_project\run_nightly_update.bat"

# Check if task already exists
$existingTask = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue

if ($existingTask) {
    Write-Host "Task '$taskName' already exists. Removing old task..." -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
}

# Create the action
$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$scriptPath`"" -WorkingDirectory "C:\etf_data_project"

# Create the trigger (daily at 2:00 AM)
$trigger = New-ScheduledTaskTrigger -Daily -At 2:00AM

# Create settings
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -RunOnlyIfNetworkAvailable -MultipleInstances Parallel

# Create the task principal (run with highest privileges)
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERNAME" -LogonType Interactive -RunLevel Highest

# Register the task
Register-ScheduledTask -TaskName $taskName -Description $description -Action $action -Trigger $trigger -Settings $settings -Principal $principal

Write-Host "✅ Scheduled task '$taskName' created successfully!" -ForegroundColor Green
Write-Host "The task will run daily at 2:00 AM" -ForegroundColor Cyan
Write-Host ""
Write-Host "To test the task immediately, run:" -ForegroundColor Yellow
Write-Host "schtasks /run /tn `"$taskName`"" -ForegroundColor White