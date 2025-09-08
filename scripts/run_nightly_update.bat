@echo off
REM Nightly update batch file

cd /d C:\etf_data_project

REM Start QuestDB if not running
tasklist /FI "IMAGENAME eq questdb.exe" 2>NUL | find /I /N "questdb.exe">NUL
if "%ERRORLEVEL%"=="1" (
    echo Starting QuestDB...
    cd questdb-9.0.3-rt-windows-x86-64\bin
    start /B questdb.exe start -d ..\..\questdb_data
    cd ..\..
    timeout /t 10 /nobreak >nul
)

REM Run Python update script
echo Running nightly ETF update...
python src\nightly_etf_update.py

REM Log completion
echo Update completed at %date% %time% >> logs\scheduler.log

exit /b 0