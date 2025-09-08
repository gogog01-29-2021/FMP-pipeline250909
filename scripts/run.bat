@echo off
REM ETF Data Project - Windows Batch Scripts

REM Setup script
:SETUP
echo Setting up ETF Data Project...
python setup.py
if %ERRORLEVEL% neq 0 (
    echo Setup failed!
    pause
    exit /b 1
)
echo Setup completed successfully!
goto :EOF

REM Install dependencies
:INSTALL
echo Installing dependencies...
pip install -r requirements.txt
if %ERRORLEVEL% neq 0 (
    echo Installation failed!
    pause
    exit /b 1
)
echo Dependencies installed successfully!
goto :EOF

REM Run main pipeline
:RUN
echo Running ETF data pipeline...
python main.py
if %ERRORLEVEL% neq 0 (
    echo Pipeline failed!
    pause
    exit /b 1
)
echo Pipeline completed successfully!
goto :EOF

REM Run analysis
:ANALYZE
echo Running ETF analysis...
python etf_analyzer.py
if %ERRORLEVEL% neq 0 (
    echo Analysis failed!
    pause
    exit /b 1
)
echo Analysis completed successfully!
goto :EOF

REM Run examples
:EXAMPLES
echo Running examples...
python examples.py
if %ERRORLEVEL% neq 0 (
    echo Examples failed!
    pause
    exit /b 1
)
echo Examples completed successfully!
goto :EOF

REM Main menu
echo ETF Data Project - Batch Runner
echo ================================
echo.
echo Choose an option:
echo 1. Setup project
echo 2. Install dependencies
echo 3. Run main pipeline
echo 4. Run analysis
echo 5. Run examples
echo 6. Exit
echo.
set /p choice="Enter your choice (1-6): "

if "%choice%"=="1" call :SETUP
if "%choice%"=="2" call :INSTALL
if "%choice%"=="3" call :RUN
if "%choice%"=="4" call :ANALYZE
if "%choice%"=="5" call :EXAMPLES
if "%choice%"=="6" exit /b 0

echo Invalid choice!
pause
goto :EOF
