#!/usr/bin/env python3
"""
Simple Test Script - No emojis, just basic execution
"""

import sys
sys.path.append('src')

from create_unified_schema import UnifiedSchemaCreator
from unified_data_crawler import UnifiedDataCrawler
from load_to_questdb import QuestDBLoader

print("="*80)
print("STARTING SIMPLE TEST")
print("Testing with 3 symbols, 1 year of data")
print("="*80)

# Step 1: Create schema
print("\n[1/3] Creating QuestDB schema...")
try:
    schema_creator = UnifiedSchemaCreator()
    # Just try to connect, skip the full create process if it fails
    if schema_creator.check_connection():
        schema_creator.create_all_tables()
        print("Schema creation complete")
    else:
        print("QuestDB not running - you'll need to start it manually")
        print("Run: cd questdb-9.0.3-rt-windows-x86-64/bin && questdb.exe start")
        sys.exit(1)
except Exception as e:
    print(f"Schema creation had issues but continuing: {e}")

# Step 2: Crawl data
print("\n[2/3] Crawling data...")
try:
    crawler = UnifiedDataCrawler(override_years=1)
    # Get first 3 priority-1 symbols
    symbols = crawler.symbols_df[crawler.symbols_df['priority'] == 1].head(3)

    for idx, row in symbols.iterrows():
        print(f"\nCrawling {row['symbol']}...")
        crawler.crawl_symbol(row)

    print("Data crawling complete")
except Exception as e:
    print(f"Crawling error: {e}")
    import traceback
    traceback.print_exc()

# Step 3: Load to QuestDB
print("\n[3/3] Loading data to QuestDB...")
try:
    loader = QuestDBLoader()
    loader.load_all_files(data_dir='data/csv', file_type='csv', limit=10)
    print("Data loading complete")
except Exception as e:
    print(f"Loading error: {e}")
    import traceback
    traceback.print_exc()

# Verify
print("\n" + "="*80)
print("TEST COMPLETE")
print("="*80)
print("\nTo verify data:")
print("1. Open http://localhost:9000")
print("2. Run: SELECT COUNT(*) FROM ohlcv_unified;")
