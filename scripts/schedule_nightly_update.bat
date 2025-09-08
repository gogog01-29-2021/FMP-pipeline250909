@echo off
echo Setting up Windows Task Scheduler for nightly ETF updates...
echo.

REM Create scheduled task to run every night at 2:00 AM
schtasks /create /tn "ETF_Nightly_Update" /tr "C:\etf_data_project\run_nightly_update.bat" /sc daily /st 02:00 /f

echo.
echo ✅ Task scheduled successfully!
echo.
echo The task "ETF_Nightly_Update" will run every night at 2:00 AM
echo.
echo To view the task: schtasks /query /tn "ETF_Nightly_Update"
echo To run manually: schtasks /run /tn "ETF_Nightly_Update"
echo To delete task: schtasks /delete /tn "ETF_Nightly_Update" /f
echo.
pause