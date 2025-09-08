@echo off
echo ========================================
echo ETF Data Analysis Pipeline
echo ========================================
echo.

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python and try again
    pause
    exit /b 1
)

echo Python is available
echo.

REM Install requirements
echo Installing Python packages...
python -m pip install -r requirements.txt
if errorlevel 1 (
    echo ERROR: Failed to install requirements
    pause
    exit /b 1
)
echo.

REM Check if .env file exists
if not exist .env (
    echo WARNING: .env file not found
    if exist .env.example (
        echo Creating .env from template...
        copy .env.example .env >nul
        echo.
        echo IMPORTANT: Please edit .env file and add your FMP_API_KEY
        echo Get your API key from: https://financialmodelingprep.com/
        echo.
        pause
    ) else (
        echo ERROR: No .env.example file found
        pause
        exit /b 1
    )
)

REM Create data directories
if not exist data mkdir data
if not exist data\parquet mkdir data\parquet
if not exist data\csv mkdir data\csv

echo Data directories created
echo.

REM Run the complete analysis pipeline
echo Starting ETF data analysis...
python run_analysis.py

if errorlevel 1 (
    echo.
    echo ERROR: ETF analysis failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo Analysis completed successfully!
echo ========================================
echo.
echo Check the following locations for results:
echo - data\parquet\ (Parquet files)
echo - data\csv\ (CSV files) 
echo - data\etf_analysis_results.csv (Analysis summary)
echo - QuestDB Web Console: http://localhost:9000
echo.
pause
