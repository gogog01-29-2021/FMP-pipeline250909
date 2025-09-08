# PowerShell script to create scheduled task for ETF updates

$taskName = "ETF_Nightly_Update"
$taskPath = "\"
$scriptPath = "C:\etf_data_project\run_nightly_update.bat"
$time = "02:00"

# Create action
$action = New-ScheduledTaskAction -Execute $scriptPath

# Create trigger for daily at 2:00 AM
$trigger = New-ScheduledTaskTrigger -Daily -At $time

# Create settings
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable

# Register the task
try {
    Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Force
    Write-Host "✅ Scheduled task '$taskName' created successfully!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Task Details:" -ForegroundColor Yellow
    Write-Host "  • Name: $taskName"
    Write-Host "  • Schedule: Daily at $time"
    Write-Host "  • Script: $scriptPath"
    Write-Host ""
    Write-Host "Commands:" -ForegroundColor Yellow
    Write-Host "  • View task: Get-ScheduledTask -TaskName $taskName"
    Write-Host "  • Run now: Start-ScheduledTask -TaskName $taskName"
    Write-Host "  • Disable: Disable-ScheduledTask -TaskName $taskName"
    Write-Host "  • Delete: Unregister-ScheduledTask -TaskName $taskName"
} catch {
    Write-Host "❌ Failed to create scheduled task: $_" -ForegroundColor Red
    Write-Host "Please run this script as Administrator" -ForegroundColor Yellow
}