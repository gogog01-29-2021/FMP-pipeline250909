#!/usr/bin/env python3
"""
Nightly ETF Data Update Script
Fetches latest data from FMP and updates QuestDB
"""

import pandas as pd
import numpy as np
import requests
import psycopg2
from datetime import datetime, timedelta
from pathlib import Path
import os
import sys
import time
import logging
from dotenv import load_dotenv

# Setup logging
log_dir = Path('./logs')
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/etf_update_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)

# Fix Windows encoding
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Load environment
load_dotenv()

class NightlyETFUpdater:
    def __init__(self):
        self.api_key = os.getenv('FMP_API_KEY')
        self.base_url = "https://financialmodelingprep.com/stable"
        
        # Symbols to update nightly
        self.symbols = [
            'SPY',   # S&P 500 ETF
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA',  # Tech giants
            'META', 'TSLA', 'JPM', 'JNJ', 'V', 'UNH'   # Other majors
        ]
        
        self.data_dir = Path('./data')
        self.parquet_dir = self.data_dir / 'parquet'
        self.csv_dir = self.data_dir / 'csv'
        
        for dir_path in [self.data_dir, self.parquet_dir, self.csv_dir]:
            dir_path.mkdir(exist_ok=True)
    
    def fetch_latest_data(self, symbol, days=5):
        """Fetch latest data for symbol"""
        url = f"{self.base_url}/historical-price-eod/full"
        params = {
            'symbol': symbol,
            'apikey': self.api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=30, verify=False)
            
            if response.status_code == 200:
                data = response.json()
                
                if isinstance(data, list) and len(data) > 0:
                    df = pd.DataFrame(data)
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.sort_values('date')
                    
                    # Get last N days
                    cutoff_date = datetime.now() - timedelta(days=days)
                    df = df[df['date'] >= cutoff_date]
                    
                    # Add symbol column
                    df['symbol'] = symbol
                    
                    logging.info(f"✅ {symbol}: Fetched {len(df)} records")
                    return df
                else:
                    logging.warning(f"⚠️ {symbol}: No data returned")
            else:
                logging.error(f"❌ {symbol}: HTTP {response.status_code}")
                
        except Exception as e:
            logging.error(f"❌ {symbol}: Error - {e}")
        
        return None
    
    def update_questdb(self, df, symbol):
        """Update QuestDB with latest data"""
        try:
            conn = psycopg2.connect(
                host='localhost',
                port=8812,
                database='qdb',
                user='admin',
                password='quest'
            )
            
            cursor = conn.cursor()
            
            inserted = 0
            for _, row in df.iterrows():
                try:
                    # Check if record exists
                    check_sql = """
                    SELECT COUNT(*) FROM ohlcv1d 
                    WHERE symbol = %s AND date = %s
                    """
                    cursor.execute(check_sql, (symbol, row['date']))
                    exists = cursor.fetchone()[0] > 0
                    
                    if not exists:
                        # Insert new record
                        insert_sql = """
                        INSERT INTO ohlcv1d (symbol, date, open, high, low, close, volume, timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        values = (
                            symbol,
                            row['date'],
                            float(row['open']),
                            float(row['high']),
                            float(row['low']),
                            float(row['close']),
                            int(row['volume']),
                            row['date']
                        )
                        
                        cursor.execute(insert_sql, values)
                        inserted += 1
                        
                except Exception as e:
                    logging.warning(f"  Skip row: {e}")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            if inserted > 0:
                logging.info(f"📊 {symbol}: Inserted {inserted} new records to QuestDB")
            else:
                logging.info(f"📊 {symbol}: No new records to insert")
            
            return True
            
        except Exception as e:
            logging.error(f"❌ QuestDB update failed: {e}")
            return False
    
    def calculate_metrics(self, symbol):
        """Calculate latest metrics from QuestDB"""
        try:
            conn = psycopg2.connect(
                host='localhost',
                port=8812,
                database='qdb',
                user='admin',
                password='quest'
            )
            
            cursor = conn.cursor()
            
            # Get last 30 days of data
            cursor.execute("""
                SELECT date, close, volume
                FROM ohlcv1d
                WHERE symbol = %s
                ORDER BY date DESC
                LIMIT 30
            """, (symbol,))
            
            data = cursor.fetchall()
            
            if data:
                df = pd.DataFrame(data, columns=['date', 'close', 'volume'])
                df = df.sort_values('date')
                
                # Calculate metrics
                df['returns'] = df['close'].pct_change()
                volatility = df['returns'].std() * np.sqrt(252) * 100
                avg_volume = df['volume'].mean()
                latest_price = df['close'].iloc[-1]
                
                cursor.close()
                conn.close()
                
                return {
                    'volatility_30d': round(volatility, 2),
                    'avg_volume_30d': int(avg_volume),
                    'latest_price': round(latest_price, 2)
                }
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logging.error(f"❌ Metrics calculation failed: {e}")
        
        return None
    
    def generate_report(self):
        """Generate daily report"""
        try:
            conn = psycopg2.connect(
                host='localhost',
                port=8812,
                database='qdb',
                user='admin',
                password='quest'
            )
            
            cursor = conn.cursor()
            
            # Get summary
            cursor.execute("""
                SELECT symbol, 
                       COUNT(*) as total_records,
                       MIN(date) as first_date,
                       MAX(date) as last_date
                FROM ohlcv1d
                GROUP BY symbol
                ORDER BY symbol
            """)
            
            summary = cursor.fetchall()
            
            report = []
            for symbol, count, first_date, last_date in summary:
                metrics = self.calculate_metrics(symbol)
                
                report.append({
                    'symbol': symbol,
                    'records': count,
                    'date_range': f"{first_date} to {last_date}",
                    'latest_price': metrics['latest_price'] if metrics else None,
                    'volatility_30d': metrics['volatility_30d'] if metrics else None,
                    'avg_volume_30d': metrics['avg_volume_30d'] if metrics else None
                })
            
            cursor.close()
            conn.close()
            
            # Save report
            report_df = pd.DataFrame(report)
            report_path = self.data_dir / f"daily_report_{datetime.now().strftime('%Y%m%d')}.csv"
            report_df.to_csv(report_path, index=False)
            
            logging.info(f"📄 Report saved to {report_path}")
            
            # Print summary
            logging.info("\n" + "="*60)
            logging.info("📊 DAILY UPDATE SUMMARY")
            logging.info("="*60)
            
            for row in report:
                logging.info(f"\n{row['symbol']}:")
                logging.info(f"  Records: {row['records']}")
                logging.info(f"  Latest: ${row['latest_price']}")
                logging.info(f"  Vol 30d: {row['volatility_30d']}%")
                logging.info(f"  Avg Vol: {row['avg_volume_30d']:,}")
            
            return report_df
            
        except Exception as e:
            logging.error(f"❌ Report generation failed: {e}")
            return None
    
    def run_update(self):
        """Run nightly update"""
        logging.info("="*60)
        logging.info(f"🌙 NIGHTLY UPDATE STARTED - {datetime.now()}")
        logging.info("="*60)
        
        success_count = 0
        fail_count = 0
        
        for symbol in self.symbols:
            logging.info(f"\n📊 Updating {symbol}...")
            
            # Fetch latest data
            df = self.fetch_latest_data(symbol, days=5)
            
            if df is not None:
                # Update QuestDB
                if self.update_questdb(df, symbol):
                    success_count += 1
                else:
                    fail_count += 1
            else:
                fail_count += 1
            
            # Rate limiting
            time.sleep(0.5)
        
        # Generate report
        self.generate_report()
        
        logging.info("\n" + "="*60)
        logging.info(f"✅ UPDATE COMPLETE")
        logging.info(f"   Success: {success_count}")
        logging.info(f"   Failed: {fail_count}")
        logging.info(f"   Time: {datetime.now()}")
        logging.info("="*60)
        
        return success_count > 0

def main():
    updater = NightlyETFUpdater()
    updater.run_update()

if __name__ == "__main__":
    main()