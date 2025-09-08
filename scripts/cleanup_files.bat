@echo off
echo ================================================
echo ETF Data Pipeline - File Cleanup Script
echo ================================================
echo.
echo This will DELETE the following FAILED files:
echo.

echo Network-related files (all blocked by firewall):
echo - etf_fmp_processor.py
echo - etf_network_resilient.py
echo - etf_fmp_network_fix.py
echo - etf_downloader.py
echo - etf_yfinance_backup.py
echo - network_diagnostics.py
echo - configure_network.ps1
echo - simple_api_test.py
echo - debug_fmp_api.py
echo - simple_connectivity_test.py
echo.

echo Unused/redundant files:
echo - etf_processor_lite.py
echo - etf_analyzer.py
echo - examples.py
echo - main.py
echo - simple_test.py
echo.

set /p choice="Do you want to delete these files? (Y/N): "
if /i "%choice%"=="Y" (
    echo.
    echo Deleting failed network files...
    del etf_fmp_processor.py 2>nul
    del etf_network_resilient.py 2>nul
    del etf_fmp_network_fix.py 2>nul
    del etf_downloader.py 2>nul
    del etf_yfinance_backup.py 2>nul
    del network_diagnostics.py 2>nul
    del configure_network.ps1 2>nul
    del simple_api_test.py 2>nul
    del debug_fmp_api.py 2>nul
    del simple_connectivity_test.py 2>nul
    del run_network_resilient.bat 2>nul
    
    echo Deleting unused files...
    del etf_processor_lite.py 2>nul
    del etf_analyzer.py 2>nul
    del examples.py 2>nul
    del main.py 2>nul
    del simple_test.py 2>nul
    del network_diagnostics_report.json 2>nul
    
    echo.
    echo ✓ Cleanup completed!
    echo.
    echo REMAINING ESSENTIAL FILES:
    dir *.py /b
    dir *.bat /b
    echo.
) else (
    echo Cleanup cancelled.
)

pause
