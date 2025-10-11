#!/usr/bin/env python3
"""
Raw Data Checker - Show actual unformatted API responses and data
No pretty printing, just raw reality
"""

import pandas as pd
import requests
import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def get_raw_api_data():
    """Get raw API data without formatting"""
    api_key = os.getenv('FMP_API_KEY')
    symbols = ['SPY', 'QQQ', 'AAPL']

    print("=== RAW API CALLING LOG ===")

    for symbol in symbols:
        print(f"\n--- RAW API CALL FOR {symbol} ---")

        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}"
        params = {
            'apikey': api_key,
            'from': (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
            'to': datetime.now().strftime('%Y-%m-%d')
        }

        print(f"URL: {url}")
        print(f"PARAMS: {params}")

        try:
            response = requests.get(url, params=params, timeout=10)
            print(f"STATUS CODE: {response.status_code}")
            print(f"HEADERS: {dict(response.headers)}")
            print(f"RESPONSE SIZE: {len(response.content)} bytes")

            # Raw JSON response
            data = response.json()
            print(f"RAW JSON STRUCTURE:")
            print(f"  Keys: {list(data.keys())}")

            if 'historical' in data:
                historical = data['historical']
                print(f"  Historical records: {len(historical)}")
                print(f"  First record raw:")
                print(f"    {historical[0] if historical else 'NONE'}")
                print(f"  Sample of first 3 raw records:")
                for i, record in enumerate(historical[:3]):
                    print(f"    [{i}]: {record}")

                # Show the DataFrame conversion
                df = pd.DataFrame(historical[:5])
                print(f"\n  PANDAS DATAFRAME (first 2 rows):")
                print(df.head(2).to_string())

                print(f"\n  DATAFRAME INFO:")
                print(f"    Shape: {df.shape}")
                print(f"    Columns: {list(df.columns)}")
                print(f"    Dtypes: {df.dtypes.to_dict()}")

            else:
                print(f"  NO HISTORICAL DATA FOUND")

        except Exception as e:
            print(f"ERROR: {e}")
            print(f"ERROR TYPE: {type(e)}")

def check_existing_data_files():
    """Check what data files actually exist"""
    print("\n\n=== RAW DATA FILES CHECK ===")

    data_dirs = ['data', 'data/csv', 'data/parquet', 'logs']

    for dir_path in data_dirs:
        if os.path.exists(dir_path):
            files = os.listdir(dir_path)
            print(f"\n{dir_path}/ contains:")
            if files:
                for file in files:
                    file_path = os.path.join(dir_path, file)
                    if os.path.isfile(file_path):
                        size = os.path.getsize(file_path)
                        modified = datetime.fromtimestamp(os.path.getmtime(file_path))
                        print(f"  {file} ({size} bytes, modified: {modified})")
            else:
                print(f"  (empty)")
        else:
            print(f"\n{dir_path}/ does not exist")

def check_log_files():
    """Check actual log files"""
    print("\n\n=== RAW LOG FILES ===")

    log_patterns = ['*.log', 'logs/*.log', '*processor*.log']

    import glob
    for pattern in log_patterns:
        files = glob.glob(pattern)
        if files:
            for log_file in files:
                print(f"\nLOG FILE: {log_file}")
                try:
                    with open(log_file, 'r') as f:
                        content = f.read()
                        print(f"SIZE: {len(content)} characters")
                        print(f"LAST 500 CHARS:")
                        print(content[-500:] if len(content) > 500 else content)
                except Exception as e:
                    print(f"  Error reading: {e}")

def main():
    print("RAW DATA INSPECTION - No Pretty Formatting")
    print("=" * 60)

    # Check raw API responses
    get_raw_api_data()

    # Check existing files
    check_existing_data_files()

    # Check logs
    check_log_files()

    print("\n" + "=" * 60)
    print("RAW INSPECTION COMPLETE")

if __name__ == "__main__":
    main()