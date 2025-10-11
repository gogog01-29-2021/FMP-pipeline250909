#!/usr/bin/env python3
"""
Premium Unified ETF/Stock Data Processor
Handles daily and 1-minute data for indices and S&P 500 stocks
Modular design with all features in one file
"""

import pandas as pd
import numpy as np
import requests
import psycopg2
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime, timedelta
from pathlib import Path
import os
import sys
import time
import json
import logging
from typing import List, Dict, Optional, Tuple
from dotenv import load_dotenv

# Fix Windows encoding
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Load environment
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/premium_processor_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)

class PremiumDataProcessor:
    """Unified processor for all premium FMP data features"""
    
    def __init__(self):
        self.api_key = os.getenv('FMP_API_KEY')
        if not self.api_key:
            raise ValueError("FMP_API_KEY not found in .env")
        
        # API endpoints
        self.base_url = "https://financialmodelingprep.com/api/v3"
        
        # Target indices and ETFs
        self.indices = {
            'SPY': {'name': 'S&P 500 ETF', 'expense_ratio': 0.0945},
            'QQQ': {'name': 'NASDAQ-100 ETF', 'expense_ratio': 0.20},
            'IWM': {'name': 'Russell 2000 ETF', 'expense_ratio': 0.19},
            'VXX': {'name': 'CBOE Volatility ETF', 'expense_ratio': 0.83}
        }
        
        # S&P 500 top components (add more as needed)
        self.sp500_stocks = [
            'AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOGL', 'GOOG',
            'BRK.B', 'LLY', 'JPM', 'TSLA', 'UNH', 'XOM', 'JNJ', 'V',
            'PG', 'MA', 'HD', 'CVX', 'AVGO', 'MRK', 'ABBV', 'COST', 'PEP'
        ]
        
        # Setup directories
        self.setup_directories()
        
        # QuestDB connection params
        self.db_params = {
            'host': 'localhost',
            'port': 8812,
            'database': 'qdb',
            'user': 'admin',
            'password': 'quest'
        }
        
        logging.info("Premium Processor initialized")
    
    def setup_directories(self):
        """Create necessary directories"""
        dirs = [
            Path('data/daily/csv'),
            Path('data/daily/parquet'),
            Path('data/1min/csv'),
            Path('data/1min/parquet'),
            Path('logs')
        ]
        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)
    
    # ==================== DAILY DATA FUNCTIONS ====================
    
    def fetch_daily_data(self, symbol: str, period: str = '1M') -> Optional[pd.DataFrame]:
        """Fetch daily OHLCV data for given period (1M, 3M, 1Y, 5Y)"""
        
        # Calculate date range
        end_date = datetime.now()
        if period == '1M':
            start_date = end_date - timedelta(days=30)
        elif period == '3M':
            start_date = end_date - timedelta(days=90)
        elif period == '1Y':
            start_date = end_date - timedelta(days=365)
        else:  # 5Y
            start_date = end_date - timedelta(days=5*365)
        
        url = f"{self.base_url}/historical-price-full/{symbol}"
        params = {
            'from': start_date.strftime('%Y-%m-%d'),
            'to': end_date.strftime('%Y-%m-%d'),
            'apikey': self.api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if 'historical' in data:
                    df = pd.DataFrame(data['historical'])
                    df['date'] = pd.to_datetime(df['date'])
                    df['symbol'] = symbol
                    df = df.sort_values('date')
                    logging.info(f"✅ {symbol}: Fetched {len(df)} daily records")
                    return df
            else:
                logging.error(f"❌ {symbol}: HTTP {response.status_code}")
        except Exception as e:
            logging.error(f"❌ {symbol}: Error - {e}")
        
        return None
    
    def calculate_metrics(self, df: pd.DataFrame) -> Dict:
        """Calculate volatility and other metrics"""
        if df is None or len(df) < 20:
            return {}
        
        df['returns'] = df['close'].pct_change()
        
        # Monthly volatility (21 trading days)
        monthly_vol = df['returns'].tail(21).std() * np.sqrt(252) * 100
        
        # Annual volatility
        annual_vol = df['returns'].std() * np.sqrt(252) * 100
        
        return {
            'volatility_monthly': round(monthly_vol, 2),
            'volatility_annual': round(annual_vol, 2),
            'avg_volume': int(df['volume'].mean()),
            'latest_price': round(df['close'].iloc[-1], 2),
            'price_change_1m': round((df['close'].iloc[-1] / df['close'].iloc[-21] - 1) * 100, 2) if len(df) > 21 else 0
        }
    
    # ==================== 1-MINUTE DATA FUNCTIONS ====================
    
    def fetch_1min_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """Fetch 1-minute intraday data (last trading day)"""
        
        url = f"{self.base_url}/historical-chart/1min/{symbol}"
        params = {'apikey': self.api_key}
        
        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    df = pd.DataFrame(data)
                    df['date'] = pd.to_datetime(df['date'])
                    df['symbol'] = symbol
                    df = df.sort_values('date')
                    logging.info(f"✅ {symbol}: Fetched {len(df)} 1-min records")
                    return df
            else:
                logging.error(f"❌ {symbol}: 1-min HTTP {response.status_code}")
        except Exception as e:
            logging.error(f"❌ {symbol}: 1-min error - {e}")
        
        return None
    
    # ==================== QUESTDB FUNCTIONS ====================
    
    def setup_questdb_tables(self):
        """Create or verify QuestDB tables"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            # Daily table (already exists, just verify)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ohlcv1d (
                    symbol STRING,
                    date TIMESTAMP,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume LONG,
                    timestamp TIMESTAMP
                ) timestamp(timestamp) PARTITION BY MONTH
            """)
            
            # 1-minute table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ohlcv1min (
                    symbol STRING,
                    date TIMESTAMP,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume LONG,
                    timestamp TIMESTAMP
                ) timestamp(timestamp) PARTITION BY DAY
            """)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logging.info("✅ QuestDB tables ready")
            return True
            
        except Exception as e:
            logging.error(f"❌ QuestDB setup failed: {e}")
            return False
    
    def upsert_daily_data(self, df: pd.DataFrame, symbol: str):
        """Insert or update daily data in QuestDB"""
        if df is None or df.empty:
            return False
        
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            inserted = 0
            updated = 0
            
            for _, row in df.iterrows():
                # Check if record exists
                cursor.execute("""
                    SELECT COUNT(*) FROM ohlcv1d 
                    WHERE symbol = %s AND date = %s
                """, (symbol, row['date']))
                
                exists = cursor.fetchone()[0] > 0
                
                if not exists:
                    # Insert new record
                    cursor.execute("""
                        INSERT INTO ohlcv1d (symbol, date, open, high, low, close, volume, timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        symbol, row['date'],
                        float(row['open']), float(row['high']),
                        float(row['low']), float(row['close']),
                        int(row['volume']), row['date']
                    ))
                    inserted += 1
                else:
                    updated += 1
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logging.info(f"📊 {symbol}: Inserted {inserted}, Skipped {updated} daily records")
            return True
            
        except Exception as e:
            logging.error(f"❌ QuestDB daily insert failed: {e}")
            return False
    
    def upsert_1min_data(self, df: pd.DataFrame, symbol: str):
        """Insert 1-minute data (cleanup handled separately due to QuestDB limitations)"""
        if df is None or df.empty:
            return False
        
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            # Note: QuestDB doesn't support DELETE operations
            # Cleanup is handled via partition dropping or table recreation
            # See questdb_cleanup_fix.py for details
            
            # Insert new data
            inserted = 0
            updated = 0
            for _, row in df.iterrows():
                try:
                    # Check if record exists
                    cursor.execute("""
                        SELECT 1 FROM ohlcv1min 
                        WHERE symbol = %s AND datetime = %s
                    """, (symbol, row['date']))
                    
                    exists = cursor.fetchone()
                    
                    if not exists:
                        cursor.execute("""
                            INSERT INTO ohlcv1min (symbol, datetime, open, high, low, close, volume, timestamp)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            symbol, row['date'],
                            float(row['open']), float(row['high']),
                            float(row['low']), float(row['close']),
                            int(row.get('volume', 0)), row['date']
                        ))
                        inserted += 1
                    else:
                        updated += 1
                except Exception as e:
                    logging.debug(f"Skip duplicate or error: {e}")
                    pass
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logging.info(f"📊 {symbol}: Inserted {inserted}, Skipped {updated} 1-min records")
            return True
            
        except Exception as e:
            logging.error(f"❌ QuestDB 1-min insert failed: {e}")
            return False
    
    # ==================== SAVE FUNCTIONS ====================
    
    def save_to_formats(self, df: pd.DataFrame, symbol: str, data_type: str):
        """Save data to CSV and Parquet formats"""
        if df is None or df.empty:
            return False
        
        timestamp = datetime.now().strftime('%Y%m%d')
        
        # Determine paths based on data type
        if data_type == 'daily':
            csv_path = Path(f'data/daily/csv/{symbol}_daily_{timestamp}.csv')
            parquet_path = Path(f'data/daily/parquet/{symbol}_daily_{timestamp}.parquet')
        else:  # 1min
            csv_path = Path(f'data/1min/csv/{symbol}_1min_{timestamp}.csv')
            parquet_path = Path(f'data/1min/parquet/{symbol}_1min_{timestamp}.parquet')
        
        try:
            # Save CSV
            df.to_csv(csv_path, index=False)
            logging.info(f"💾 Saved CSV: {csv_path}")
            
            # Save Parquet
            table = pa.Table.from_pandas(df)
            pq.write_table(table, parquet_path, compression='snappy')
            logging.info(f"💾 Saved Parquet: {parquet_path}")
            
            return True
            
        except Exception as e:
            logging.error(f"❌ Save failed: {e}")
            return False
    
    # ==================== MAIN PROCESSING FUNCTIONS ====================
    
    def process_indices(self):
        """Process all index ETFs with daily data"""
        logging.info("=" * 60)
        logging.info("📊 Processing Index ETFs (Daily)")
        logging.info("=" * 60)
        
        results = []
        
        for symbol, info in self.indices.items():
            logging.info(f"\n🏷️ Processing {symbol} - {info['name']}")
            
            # Fetch 1 month daily data
            df = self.fetch_daily_data(symbol, '1M')
            
            if df is not None:
                # Calculate metrics
                metrics = self.calculate_metrics(df)
                
                # Save to formats
                self.save_to_formats(df, symbol, 'daily')
                
                # Update QuestDB
                self.upsert_daily_data(df, symbol)
                
                # Store results
                results.append({
                    'symbol': symbol,
                    'name': info['name'],
                    'expense_ratio': info['expense_ratio'],
                    **metrics
                })
            
            time.sleep(0.5)  # Rate limiting
        
        # Sort by volatility (high) and expense ratio (low)
        results_df = pd.DataFrame(results)
        results_df = results_df.sort_values(
            by=['volatility_monthly', 'expense_ratio'],
            ascending=[False, True]
        )
        
        logging.info("\n📊 INDEX ETF RANKINGS (High Vol → Low Expense)")
        for _, row in results_df.iterrows():
            logging.info(f"{row['symbol']}: Vol={row['volatility_monthly']}%, Expense={row['expense_ratio']}%")
        
        return results_df
    
    def process_sp500_1min(self, limit: int = 10):
        """Process S&P 500 stocks with 1-minute data"""
        logging.info("=" * 60)
        logging.info("📊 Processing S&P 500 Stocks (1-Minute)")
        logging.info("=" * 60)
        
        processed = 0
        
        for symbol in self.sp500_stocks[:limit]:  # Limit to avoid rate limits
            logging.info(f"\n🏷️ Processing {symbol} (1-min)")
            
            # Fetch 1-minute data
            df = self.fetch_1min_data(symbol)
            
            if df is not None:
                # Save to formats
                self.save_to_formats(df, symbol, '1min')
                
                # Update QuestDB
                self.upsert_1min_data(df, symbol)
                
                processed += 1
            
            time.sleep(1)  # Rate limiting
        
        logging.info(f"\n✅ Processed {processed} S&P 500 stocks with 1-min data")
        return processed
    
    def run_nightly_update(self):
        """Main function for nightly scheduled updates"""
        logging.info("🌙 STARTING NIGHTLY UPDATE")
        
        # Setup QuestDB tables
        self.setup_questdb_tables()
        
        # Process index ETFs (daily)
        index_results = self.process_indices()
        
        # Process S&P 500 stocks (1-minute)
        sp500_processed = self.process_sp500_1min(limit=10)
        
        # Generate summary report
        report_path = Path(f'data/daily_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
        index_results.to_csv(report_path, index=False)
        
        logging.info("=" * 60)
        logging.info("✅ NIGHTLY UPDATE COMPLETE")
        logging.info(f"📊 Processed {len(index_results)} indices")
        logging.info(f"📊 Processed {sp500_processed} S&P 500 stocks")
        logging.info(f"📄 Report saved: {report_path}")
        logging.info("=" * 60)

# ==================== STANDALONE FUNCTIONS ====================

def main():
    """Main entry point"""
    processor = PremiumDataProcessor()
    processor.run_nightly_update()

def schedule_task():
    """Create Windows scheduled task"""
    import subprocess
    
    task_name = "Premium_ETF_Update"
    script_path = os.path.abspath(__file__)
    
    # Create scheduled task for 2:00 AM daily
    cmd = f'schtasks /create /tn "{task_name}" /tr "python {script_path}" /sc daily /st 02:00 /f'
    
    try:
        subprocess.run(cmd, shell=True, check=True)
        print(f"✅ Scheduled task '{task_name}' created for 2:00 AM daily")
    except:
        print("❌ Failed to create scheduled task (run as Administrator)")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Premium ETF Data Processor')
    parser.add_argument('--schedule', action='store_true', help='Setup scheduled task')
    parser.add_argument('--test', action='store_true', help='Test with limited data')
    
    args = parser.parse_args()
    
    if args.schedule:
        schedule_task()
    elif args.test:
        processor = PremiumDataProcessor()
        processor.process_indices()
        processor.process_sp500_1min(limit=2)
    else:
        main()