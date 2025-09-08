#!/usr/bin/env python3
"""
Organize ETF project files into clean structure
"""

import os
import sys
import shutil
from pathlib import Path

# Fix Windows encoding
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def organize_project():
    # Create folder structure
    base_dir = Path("etf_pipeline_working")
    
    folders = [
        base_dir / "src",
        base_dir / "scripts",
        base_dir / "data" / "csv",
        base_dir / "data" / "parquet",
        base_dir / "logs",
        base_dir / "config"
    ]
    
    for folder in folders:
        folder.mkdir(parents=True, exist_ok=True)
    
    # Copy working files
    files_to_copy = {
        # Core Python files
        "fmp_final_processor.py": base_dir / "src" / "fmp_processor.py",
        "nightly_etf_update.py": base_dir / "src" / "nightly_updater.py",
        "questdb_manager.py": base_dir / "src" / "questdb_manager.py",
        
        # Configuration
        ".env": base_dir / "config" / ".env",
        "requirements.txt": base_dir / "config" / "requirements.txt",
        
        # Scripts
        "start_questdb.bat": base_dir / "scripts" / "start_questdb.bat",
        "run_nightly_update.bat": base_dir / "scripts" / "run_nightly_update.bat",
        "create_schedule.ps1": base_dir / "scripts" / "create_schedule.ps1",
    }
    
    copied = []
    skipped = []
    
    for src, dst in files_to_copy.items():
        if Path(src).exists():
            shutil.copy2(src, dst)
            copied.append(str(dst))
            print(f"✅ Copied: {src} -> {dst}")
        else:
            skipped.append(src)
            print(f"⚠️ Skipped (not found): {src}")
    
    # Copy sample data files
    csv_files = list(Path("data/csv").glob("*_ohlcv_5y.csv"))[:3]  # Copy 3 samples
    for csv in csv_files:
        if csv.exists():
            shutil.copy2(csv, base_dir / "data" / "csv" / csv.name)
            print(f"✅ Copied sample: {csv.name}")
    
    parquet_files = list(Path("data/parquet").glob("*_ohlcv_5y.parquet"))[:3]  # Copy 3 samples
    for pq in parquet_files:
        if pq.exists():
            shutil.copy2(pq, base_dir / "data" / "parquet" / pq.name)
            print(f"✅ Copied sample: {pq.name}")
    
    print(f"\n📁 Project organized in: {base_dir}")
    print(f"✅ Copied {len(copied)} files")
    if skipped:
        print(f"⚠️ Skipped {len(skipped)} files")
    
    return base_dir

if __name__ == "__main__":
    organize_project()