@echo off
echo ================================================
echo ETF Data Pipeline - Clean Working Version
echo ================================================
echo.

echo Checking Python installation...
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    pause
    exit /b 1
)

echo.
echo Running ETF Data Processor...
echo ----------------------------------------
python working_etf_processor.py

echo.
echo ================================================
echo Processing completed! 
echo Check the data/ folder for results.
echo ================================================
echo.

echo Files created:
dir data\csv\*.csv /b 2>nul
dir data\parquet\*.parquet /b 2>nul
dir data\etf_analysis_summary_*.csv /b 2>nul

echo.
pause
