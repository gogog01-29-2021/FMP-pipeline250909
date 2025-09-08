#!/usr/bin/env python3
"""
1-Minute Intraday Data Processor for Indices and Stocks
Fetches and stores high-frequency data in QuestDB
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

# Fix Windows encoding
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Load environment
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class IntradayDataProcessor:
    def __init__(self):
        self.api_key = os.getenv('FMP_API_KEY')
        self.base_url = "https://financialmodelingprep.com/stable"
        
        # Index symbols for 1-minute data
        self.indices = {
            '^GSPC': 'S&P 500 Index',
            '^IXIC': 'NASDAQ Composite',
            '^DJI': 'Dow Jones Industrial',
            '^RUT': 'Russell 2000'
        }
        
        # ETFs as alternatives (these work with free API)
        self.etf_alternatives = {
            'SPY': 'S&P 500 ETF',
            'QQQ': 'NASDAQ-100 ETF',
            'IWM': 'Russell 2000 ETF',
            'VXX': 'Volatility Index ETF'
        }
        
        # Top stocks for 1-minute data
        self.stocks = ['AAPL', 'MSFT', 'NVDA', 'TSLA', 'META']
        
        logging.info("Intraday processor initialized")
    
    def test_1min_endpoint(self, symbol):
        """Test if 1-minute data is available for symbol"""
        url = f"{self.base_url}/historical-chart/1min"
        params = {
            'symbol': symbol,
            'apikey': self.api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=10, verify=False)
            
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and len(data) > 0:
                    logging.info(f"✅ 1-min data available for {symbol}")
                    return True
                else:
                    logging.warning(f"⚠️ Empty response for {symbol}")
            elif response.status_code == 402:
                logging.warning(f"💰 Payment required for {symbol}")
            else:
                logging.error(f"❌ HTTP {response.status_code} for {symbol}")
                
        except Exception as e:
            logging.error(f"❌ Error testing {symbol}: {e}")
        
        return False
    
    def fetch_1min_data(self, symbol):
        """Fetch 1-minute intraday data"""
        url = f"{self.base_url}/historical-chart/1min"
        params = {
            'symbol': symbol,
            'apikey': self.api_key
        }
        
        logging.info(f"Fetching 1-min data for {symbol}...")
        
        try:
            response = requests.get(url, params=params, timeout=30, verify=False)
            
            if response.status_code == 200:
                data = response.json()
                
                if isinstance(data, list) and len(data) > 0:
                    df = pd.DataFrame(data)
                    
                    # Parse datetime
                    df['datetime'] = pd.to_datetime(df['date'])
                    df['symbol'] = symbol
                    
                    # Ensure numeric types
                    numeric_cols = ['open', 'high', 'low', 'close', 'volume']
                    for col in numeric_cols:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # Sort by time
                    df = df.sort_values('datetime')
                    
                    logging.info(f"✅ Fetched {len(df)} 1-min records for {symbol}")
                    return df
                else:
                    logging.warning(f"No data returned for {symbol}")
            else:
                logging.error(f"HTTP {response.status_code} for {symbol}")
                
        except Exception as e:
            logging.error(f"Error fetching {symbol}: {e}")
        
        return None
    
    def setup_questdb_1min_table(self):
        """Create QuestDB table for 1-minute data"""
        try:
            conn = psycopg2.connect(
                host='localhost',
                port=8812,
                database='qdb',
                user='admin',
                password='quest'
            )
            
            cursor = conn.cursor()
            
            # Create 1-minute table
            create_sql = """
            CREATE TABLE IF NOT EXISTS ohlcv1min (
                symbol STRING,
                datetime TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume LONG,
                timestamp TIMESTAMP
            ) timestamp(timestamp) PARTITION BY DAY
            """
            
            cursor.execute(create_sql)
            conn.commit()
            
            logging.info("✅ Created ohlcv1min table in QuestDB")
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            logging.error(f"❌ QuestDB setup failed: {e}")
            return False
    
    def insert_1min_data(self, df, symbol):
        """Insert 1-minute data to QuestDB"""
        if df is None or df.empty:
            return False
        
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
                    sql = """
                    INSERT INTO ohlcv1min 
                    (symbol, datetime, open, high, low, close, volume, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    values = (
                        symbol,
                        row['datetime'],
                        float(row['open']),
                        float(row['high']),
                        float(row['low']),
                        float(row['close']),
                        int(row.get('volume', 0)),
                        row['datetime']
                    )
                    
                    cursor.execute(sql, values)
                    inserted += 1
                    
                except Exception as e:
                    logging.warning(f"Skip row: {e}")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logging.info(f"📊 Inserted {inserted} 1-min records for {symbol}")
            return True
            
        except Exception as e:
            logging.error(f"❌ Insert failed: {e}")
            return False
    
    def save_1min_parquet(self, df, symbol):
        """Save 1-minute data to Parquet"""
        if df is None or df.empty:
            return False
        
        try:
            # Create directory
            parquet_dir = Path('./data/parquet_1min')
            parquet_dir.mkdir(exist_ok=True)
            
            # Save file
            filename = parquet_dir / f"{symbol}_1min_{datetime.now().strftime('%Y%m%d')}.parquet"
            df.to_parquet(filename, compression='snappy')
            
            logging.info(f"💾 Saved {symbol} to {filename}")
            return True
            
        except Exception as e:
            logging.error(f"❌ Parquet save failed: {e}")
            return False
    
    def process_all(self):
        """Process all available symbols"""
        logging.info("="*60)
        logging.info("🚀 Starting 1-Minute Data Processing")
        logging.info("="*60)
        
        # Setup QuestDB table
        self.setup_questdb_1min_table()
        
        # Test what's available
        available = []
        
        # Test indices
        logging.info("\n📊 Testing Index Symbols...")
        for symbol, name in self.indices.items():
            if self.test_1min_endpoint(symbol):
                available.append((symbol, name))
            time.sleep(0.5)
        
        # Test ETFs
        logging.info("\n📊 Testing ETF Alternatives...")
        for symbol, name in self.etf_alternatives.items():
            if self.test_1min_endpoint(symbol):
                available.append((symbol, name))
            time.sleep(0.5)
        
        # Test stocks
        logging.info("\n📊 Testing Stock Symbols...")
        for symbol in self.stocks:
            if self.test_1min_endpoint(symbol):
                available.append((symbol, f"{symbol} Stock"))
            time.sleep(0.5)
        
        if not available:
            logging.error("❌ No 1-minute data available with current API")
            return None
        
        # Process available symbols
        results = []
        for symbol, name in available:
            logging.info(f"\n📈 Processing {symbol} - {name}")
            
            # Fetch data
            df = self.fetch_1min_data(symbol)
            
            if df is not None:
                # Save to Parquet
                self.save_1min_parquet(df, symbol)
                
                # Insert to QuestDB
                self.insert_1min_data(df, symbol)
                
                results.append({
                    'symbol': symbol,
                    'name': name,
                    'records': len(df),
                    'latest': df['datetime'].max(),
                    'earliest': df['datetime'].min()
                })
            
            time.sleep(1)  # Rate limiting
        
        # Summary
        if results:
            logging.info("\n" + "="*60)
            logging.info("📊 1-MINUTE DATA SUMMARY")
            logging.info("="*60)
            
            for r in results:
                logging.info(f"\n{r['symbol']} - {r['name']}")
                logging.info(f"  Records: {r['records']}")
                logging.info(f"  Range: {r['earliest']} to {r['latest']}")
            
            logging.info(f"\n✅ Processed {len(results)} symbols")
        
        return results

def main():
    processor = IntradayDataProcessor()
    results = processor.process_all()
    
    if results:
        logging.info("\n🎉 1-Minute data processing complete!")
        logging.info("📊 Check QuestDB table: ohlcv1min")
        logging.info("💾 Parquet files in: data/parquet_1min/")
    else:
        logging.info("\n❌ No 1-minute data processed")
        logging.info("💰 Upgrade API key for full access")

if __name__ == "__main__":
    main()