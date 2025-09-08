#!/usr/bin/env python3
"""
Organize old files into Legacy folder
Keep only premium working files in main directory
"""

import os
import sys
import shutil
from pathlib import Path

# Fix Windows encoding
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def organize_files():
    # Files to move to Legacy folder
    legacy_files = [
        'debug_fmp_api.py',
        'download_questdb.py',
        'etf_analyzer.py',
        'etf_downloader.py',
        'etf_fmp_network_fix.py',
        'etf_fmp_processor.py',
        'etf_network_resilient.py',
        'etf_processor_lite.py',
        'etf_yfinance_backup.py',
        'examples.py',
        'fmp_stable_processor.py',
        'intraday_1min_processor.py',
        'network_diagnostics.py',
        'run_analysis.py',
        'simple_api_test.py',
        'simple_connectivity_test.py',
        'simple_test.py',
        'test_etf_downloader.py',
        'test_fmp_api.py',
        'test_setup.py',
        'working_etf_processor.py',
        'etf_yfinance_questdb.py',
        'organize_project.py',
        'API_TIER_EXPLANATION.md',
        'COMPLETE_PIPELINE_DOCUMENTATION.md',
        'FILE_ORGANIZATION.md',
        'FINAL_SYSTEM_STATUS.md',
        'HOW_TO_RUN.md',
        'NETWORK_TROUBLESHOOTING.md',
        'PIPELINE_STATUS_REPORT.md',
        'README_ETF_ANALYSIS.md',
        'README_NETWORK_RESILIENT.md',
        'SCHEDULE_STATUS.md'
    ]
    
    # Create Legacy folder
    legacy_dir = Path('Legacy')
    legacy_dir.mkdir(exist_ok=True)
    
    moved = 0
    skipped = 0
    
    # Move files
    for file in legacy_files:
        if Path(file).exists():
            try:
                shutil.move(file, legacy_dir / file)
                print(f"✓ Moved: {file}")
                moved += 1
            except Exception as e:
                print(f"✗ Error moving {file}: {e}")
                skipped += 1
        else:
            skipped += 1
    
    print(f"\n📁 Organized: {moved} files moved to Legacy folder")
    print(f"⚠️ Skipped: {skipped} files (not found or error)")
    
    # List remaining files in main directory
    print("\n📂 Files remaining in main directory:")
    main_files = [
        '.env',
        'premium_unified_processor.py',
        'questdb_manager.py',
        'nightly_etf_update.py',
        'fmp_final_processor.py',
        'check_questdb_data.py',
        'start_questdb.bat',
        'requirements.txt'
    ]
    
    for file in main_files:
        if Path(file).exists():
            print(f"  ✓ {file}")

if __name__ == "__main__":
    organize_files()