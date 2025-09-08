@echo off
echo ================================================
echo ETF Data Processor - Network Resilient Version
echo ================================================
echo.

echo Checking Python installation...
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8 or later and add it to your PATH
    pause
    exit /b 1
)

echo.
echo Step 1: Running Network Diagnostics...
echo ----------------------------------------
python network_diagnostics.py

echo.
echo Step 2: Running Network Configuration (requires admin rights)...
echo ----------------------------------------
powershell -ExecutionPolicy Bypass -File configure_network.ps1 -Force

echo.
echo Step 3: Running Network Resilient ETF Processor...
echo ----------------------------------------
python etf_network_resilient.py

echo.
echo ================================================
echo Processing completed! Check the output above.
echo ================================================
echo.
pause
