#!/usr/bin/env python3
"""
Main entry point for ETF Data Pipeline
Run this to execute the complete pipeline
"""

import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

def main():
    print("=" * 60)
    print("ETF DATA PIPELINE - MAIN MENU")
    print("=" * 60)
    print("\nWhat would you like to do?\n")
    print("1. Fetch latest data from FMP API")
    print("2. Load data to QuestDB")
    print("3. Run nightly update")
    print("4. Check system status")
    print("5. Exit")
    
    choice = input("\nEnter choice (1-5): ")
    
    if choice == "1":
        print("\nFetching data from FMP API...")
        from fmp_processor import FMPFinalProcessor
        processor = FMPFinalProcessor()
        processor.process_all()
        
    elif choice == "2":
        print("\nLoading data to QuestDB...")
        from questdb_manager import QuestDBManager
        manager = QuestDBManager()
        if manager.check_connection():
            manager.create_tables()
            manager.load_all_csv_files()
            manager.query_data()
        
    elif choice == "3":
        print("\nRunning nightly update...")
        from nightly_updater import NightlyETFUpdater
        updater = NightlyETFUpdater()
        updater.run_update()
        
    elif choice == "4":
        print("\nChecking system status...")
        check_status()
        
    elif choice == "5":
        print("\nExiting...")
        sys.exit(0)
    
    else:
        print("\nInvalid choice!")

def check_status():
    """Check system status"""
    print("\n" + "=" * 60)
    print("SYSTEM STATUS CHECK")
    print("=" * 60)
    
    # Check QuestDB
    try:
        from questdb_manager import QuestDBManager
        manager = QuestDBManager()
        if manager.check_connection():
            print("✅ QuestDB: Running")
        else:
            print("❌ QuestDB: Not running")
    except:
        print("❌ QuestDB: Error")
    
    # Check data files
    csv_files = list(Path("data/csv").glob("*.csv"))
    parquet_files = list(Path("data/parquet").glob("*.parquet"))
    
    print(f"📁 CSV Files: {len(csv_files)}")
    print(f"📁 Parquet Files: {len(parquet_files)}")
    
    # Check API
    env_path = Path("config/.env")
    if env_path.exists():
        print("✅ API Key: Configured")
    else:
        print("❌ API Key: Not configured")
    
    # Check scheduler
    import subprocess
    try:
        result = subprocess.run(
            ["schtasks", "/query", "/tn", "ETF_Nightly_Update"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print("✅ Scheduler: Active (2:00 AM daily)")
        else:
            print("⚠️ Scheduler: Not configured")
    except:
        print("⚠️ Scheduler: Unable to check")
    
    print("\n" + "=" * 60)
    print("Available Symbols:")
    print("ETF: SPY")
    print("Stocks: AAPL, MSFT, NVDA, TSLA, META, GOOGL, AMZN, JPM, JNJ, V, UNH")
    print("=" * 60)

if __name__ == "__main__":
    # Fix Windows encoding
    if sys.platform == 'win32':
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    
    main()