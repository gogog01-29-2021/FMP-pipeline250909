#!/usr/bin/env python3
"""
Load Data to QuestDB
Reads CSV/Parquet files and loads them into QuestDB unified table
"""

import pandas as pd
import psycopg2
from pathlib import Path
import sys
import io
import logging
from datetime import datetime
from typing import List, Optional

# Fix Windows encoding
if sys.platform == 'win32':
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except (ValueError, AttributeError):
        pass  # Already wrapped or not available

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/etl_loader_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)

class QuestDBLoader:
    """Load data from CSV/Parquet into QuestDB"""

    def __init__(self):
        self.db_params = {
            'host': 'localhost',
            'port': 8812,
            'database': 'qdb',
            'user': 'admin',
            'password': 'quest'
        }

    def check_connection(self):
        """Check if QuestDB is accessible"""
        try:
            conn = psycopg2.connect(**self.db_params)
            logging.info("✅ QuestDB connection successful!")
            conn.close()
            return True
        except Exception as e:
            logging.error(f"❌ QuestDB connection failed: {e}")
            return False

    def get_existing_records(self, symbol: str, timeframe: str) -> int:
        """Count existing records for symbol/timeframe"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT COUNT(*) FROM ohlcv_unified
                WHERE symbol = %s AND timeframe = %s
            """, (symbol, timeframe))

            count = cursor.fetchone()[0]

            cursor.close()
            conn.close()
            return count

        except Exception as e:
            logging.error(f"❌ Error checking existing records: {e}")
            return 0

    def load_csv_to_questdb(self, csv_path: Path) -> bool:
        """Load a single CSV file into QuestDB"""
        try:
            # Read CSV
            df = pd.read_csv(csv_path)
            logging.info(f"📥 Read {len(df)} records from {csv_path.name}")

            if df.empty:
                logging.warning(f"⚠️  Empty file: {csv_path.name}")
                return False

            # Extract metadata from filename or dataframe
            symbol = df['symbol'].iloc[0] if 'symbol' in df.columns else csv_path.stem.split('_')[0]
            timeframe = df['timeframe'].iloc[0] if 'timeframe' in df.columns else csv_path.stem.split('_')[1]

            # Check existing records
            existing_count = self.get_existing_records(symbol, timeframe)

            if existing_count > 0:
                logging.info(f"ℹ️  {symbol} ({timeframe}): {existing_count} existing records - skipping duplicates")

            # Connect to QuestDB
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()

            inserted = 0
            skipped = 0
            errors = 0

            # Insert records
            for _, row in df.iterrows():
                try:
                    # Prepare values
                    symbol_val = row.get('symbol', symbol)
                    asset_class_val = row.get('asset_class', 'N/A')
                    index_membership_val = row.get('index_membership', 'N/A')
                    timestamp_val = pd.to_datetime(row['date'])
                    timeframe_val = row.get('timeframe', timeframe)
                    open_val = float(row['open']) if pd.notna(row.get('open')) else None
                    high_val = float(row['high']) if pd.notna(row.get('high')) else None
                    low_val = float(row['low']) if pd.notna(row.get('low')) else None
                    close_val = float(row['close']) if pd.notna(row.get('close')) else None
                    volume_val = int(row['volume']) if pd.notna(row.get('volume')) else 0
                    adj_close_val = float(row['adj_close']) if 'adj_close' in row and pd.notna(row['adj_close']) else None
                    vwap_val = float(row['vwap']) if pd.notna(row.get('vwap')) else None
                    returns_val = float(row['returns']) if 'returns' in row and pd.notna(row['returns']) else None
                    log_returns_val = float(row['log_returns']) if 'log_returns' in row and pd.notna(row['log_returns']) else None
                    data_source_val = row.get('data_source', 'FMP')
                    inserted_at_val = pd.to_datetime(row['inserted_at']) if 'inserted_at' in row else datetime.now()

                    # Check if record already exists
                    cursor.execute("""
                        SELECT COUNT(*) FROM ohlcv_unified
                        WHERE symbol = %s AND timeframe = %s AND timestamp = %s
                    """, (symbol_val, timeframe_val, timestamp_val))

                    exists = cursor.fetchone()[0] > 0

                    if exists:
                        skipped += 1
                        continue

                    # Insert new record
                    insert_sql = """
                        INSERT INTO ohlcv_unified (
                            symbol, asset_class, index_membership, timestamp, timeframe,
                            open, high, low, close, volume,
                            adj_close, vwap, returns, log_returns,
                            data_source, inserted_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

                    cursor.execute(insert_sql, (
                        symbol_val, asset_class_val, index_membership_val, timestamp_val, timeframe_val,
                        open_val, high_val, low_val, close_val, volume_val,
                        adj_close_val, vwap_val, returns_val, log_returns_val,
                        data_source_val, inserted_at_val
                    ))

                    inserted += 1

                    # Commit every 1000 records for performance
                    if inserted % 1000 == 0:
                        conn.commit()
                        logging.info(f"  💾 Committed {inserted} records...")

                except Exception as e:
                    logging.debug(f"  ⚠️  Error inserting row: {e}")
                    errors += 1
                    continue

            # Final commit
            conn.commit()
            cursor.close()
            conn.close()

            logging.info(f"✅ {symbol} ({timeframe}): Inserted {inserted}, Skipped {skipped}, Errors {errors}")
            return True

        except Exception as e:
            logging.error(f"❌ Failed to load {csv_path.name}: {e}")
            return False

    def load_parquet_to_questdb(self, parquet_path: Path) -> bool:
        """Load a single Parquet file into QuestDB"""
        try:
            # Read Parquet
            df = pd.read_parquet(parquet_path)
            logging.info(f"📥 Read {len(df)} records from {parquet_path.name}")

            # Convert to CSV in memory and use same logic
            temp_csv = parquet_path.with_suffix('.csv')
            df.to_csv(temp_csv, index=False)

            result = self.load_csv_to_questdb(temp_csv)

            # Clean up temp file
            if temp_csv.exists():
                temp_csv.unlink()

            return result

        except Exception as e:
            logging.error(f"❌ Failed to load {parquet_path.name}: {e}")
            return False

    def load_all_files(self, data_dir: str = 'data/csv', file_type: str = 'csv',
                       asset_class: Optional[str] = None, limit: Optional[int] = None):
        """Load all CSV or Parquet files from directory"""
        logging.info("\n" + "="*80)
        logging.info(f"🚀 LOADING {file_type.upper()} FILES TO QUESTDB")
        logging.info("="*80)

        # Check connection
        if not self.check_connection():
            logging.error("❌ Cannot connect to QuestDB. Exiting.")
            return

        # Find files
        data_path = Path(data_dir)

        if asset_class:
            pattern = f"{asset_class}/*.{file_type}"
        else:
            pattern = f"**/*.{file_type}"

        files = list(data_path.glob(pattern))

        if limit:
            files = files[:limit]

        logging.info(f"📁 Found {len(files)} {file_type} files")

        if not files:
            logging.warning("⚠️  No files found to load")
            return

        # Load each file
        success_count = 0
        failed_files = []

        for idx, file_path in enumerate(files, 1):
            logging.info(f"\n[{idx}/{len(files)}] Loading {file_path.name}...")

            try:
                if file_type == 'csv':
                    result = self.load_csv_to_questdb(file_path)
                elif file_type == 'parquet':
                    result = self.load_parquet_to_questdb(file_path)
                else:
                    logging.error(f"❌ Unknown file type: {file_type}")
                    continue

                if result:
                    success_count += 1
                else:
                    failed_files.append(file_path.name)

            except Exception as e:
                logging.error(f"❌ Failed to load {file_path.name}: {e}")
                failed_files.append(file_path.name)

        # Summary
        logging.info("\n" + "="*80)
        logging.info("✅ ETL LOAD COMPLETE")
        logging.info("="*80)
        logging.info(f"✅ Successful: {success_count}/{len(files)}")
        logging.info(f"❌ Failed: {len(failed_files)}")

        if failed_files:
            logging.info(f"\n⚠️  Failed files: {', '.join(failed_files[:10])}")

    def verify_data(self):
        """Verify loaded data in QuestDB"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()

            logging.info("\n" + "="*80)
            logging.info("🔍 VERIFYING DATA IN QUESTDB")
            logging.info("="*80)

            # Total records
            cursor.execute("SELECT COUNT(*) FROM ohlcv_unified")
            total_records = cursor.fetchone()[0]
            logging.info(f"\n📊 Total Records: {total_records:,}")

            # Records by asset class
            cursor.execute("""
                SELECT asset_class, COUNT(*) as count
                FROM ohlcv_unified
                GROUP BY asset_class
                ORDER BY count DESC
            """)

            logging.info("\n📊 Records by Asset Class:")
            for asset_class, count in cursor.fetchall():
                logging.info(f"  {asset_class}: {count:,}")

            # Records by timeframe
            cursor.execute("""
                SELECT timeframe, COUNT(*) as count
                FROM ohlcv_unified
                GROUP BY timeframe
                ORDER BY count DESC
            """)

            logging.info("\n📊 Records by Timeframe:")
            for timeframe, count in cursor.fetchall():
                logging.info(f"  {timeframe}: {count:,}")

            # Top 10 symbols by record count
            cursor.execute("""
                SELECT symbol, asset_class, COUNT(*) as count
                FROM ohlcv_unified
                GROUP BY symbol, asset_class
                ORDER BY count DESC
                LIMIT 10
            """)

            logging.info("\n📊 Top 10 Symbols by Record Count:")
            for symbol, asset_class, count in cursor.fetchall():
                logging.info(f"  {symbol} ({asset_class}): {count:,}")

            # Date range
            cursor.execute("""
                SELECT MIN(timestamp) as min_date, MAX(timestamp) as max_date
                FROM ohlcv_unified
            """)

            min_date, max_date = cursor.fetchone()
            logging.info(f"\n📅 Date Range: {min_date} to {max_date}")

            cursor.close()
            conn.close()

            logging.info("\n" + "="*80)

        except Exception as e:
            logging.error(f"❌ Verification failed: {e}")

def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Load Data to QuestDB')
    parser.add_argument('--type', choices=['csv', 'parquet'], default='csv', help='File type to load')
    parser.add_argument('--asset-class', type=str, help='Load specific asset class only')
    parser.add_argument('--limit', type=int, help='Limit number of files to load')
    parser.add_argument('--verify', action='store_true', help='Verify data after loading')

    args = parser.parse_args()

    loader = QuestDBLoader()

    # Load files
    loader.load_all_files(
        data_dir=f'data/{args.type}',
        file_type=args.type,
        asset_class=args.asset_class,
        limit=args.limit
    )

    # Verify if requested
    if args.verify:
        loader.verify_data()

if __name__ == "__main__":
    main()
