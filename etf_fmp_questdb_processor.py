#!/usr/bin/env python3
"""
ETF Data Processor with FMP API and QuestDB Integration
Handles network issues and saves data to Parquet, CSV, and QuestDB
"""

import pandas as pd
import numpy as np
import requests
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime, timedelta
from pathlib import Path
import os
import sys
import time
import json
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

# Fix Windows console encoding for emojis
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load environment variables
load_dotenv()

class ETFDataProcessor:
    def __init__(self):
        self.fmp_api_key = os.getenv('FMP_API_KEY')
        if not self.fmp_api_key:
            raise ValueError("FMP_API_KEY not found in .env file")
        
        self.base_url = "https://financialmodelingprep.com/api/v3"
        
        # S&P 500 ETFs to analyze
        self.sp500_etfs = [
            'SPY',  # SPDR S&P 500 ETF
            'VOO',  # Vanguard S&P 500 ETF
            'IVV',  # iShares Core S&P 500 ETF
            'SPLG', # SPDR Portfolio S&P 500 ETF
            'RSP',  # Invesco S&P 500 Equal Weight ETF
            'SSO',  # ProShares Ultra S&P 500
            'UPRO', # ProShares UltraPro S&P 500
            'SH',   # ProShares Short S&P 500
            'SDS',  # ProShares UltraShort S&P 500
            'SPXU', # ProShares UltraPro Short S&P 500
            'SPXL', # Direxion Daily S&P 500 Bull 3X
            'SPXS'  # Direxion Daily S&P 500 Bear 3X
        ]
        
        # Setup directories
        self.data_dir = Path('./data')
        self.parquet_dir = self.data_dir / 'parquet'
        self.csv_dir = self.data_dir / 'csv'
        
        for dir_path in [self.data_dir, self.parquet_dir, self.csv_dir]:
            dir_path.mkdir(exist_ok=True)
        
        # Setup session with retry logic
        self.session = self._create_session()
        
        print(f"✅ ETF Data Processor initialized")
        print(f"📊 Processing {len(self.sp500_etfs)} S&P 500 ETFs")
    
    def _create_session(self):
        """Create requests session with retry strategy"""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set headers to avoid 403 errors
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://financialmodelingprep.com/'
        })
        
        return session
    
    def fetch_historical_data(self, symbol, years=5):
        """Fetch 5 years of historical OHLCV data"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=years * 365)
        
        url = f"{self.base_url}/historical-price-full/{symbol}"
        params = {
            'from': start_date.strftime('%Y-%m-%d'),
            'to': end_date.strftime('%Y-%m-%d'),
            'apikey': self.fmp_api_key
        }
        
        print(f"📥 Fetching {symbol} data...")
        
        try:
            response = self.session.get(url, params=params, timeout=30, verify=False)
            
            if response.status_code == 200:
                data = response.json()
                if 'historical' in data:
                    df = pd.DataFrame(data['historical'])
                    df['date'] = pd.to_datetime(df['date'])
                    df['symbol'] = symbol
                    df = df.sort_values('date')
                    
                    # Ensure numeric types
                    numeric_cols = ['open', 'high', 'low', 'close', 'adjClose', 'volume']
                    for col in numeric_cols:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    print(f"  ✅ Fetched {len(df)} records for {symbol}")
                    return df
                else:
                    print(f"  ⚠️ No historical data in response for {symbol}")
            else:
                print(f"  ❌ HTTP {response.status_code} for {symbol}")
                
        except Exception as e:
            print(f"  ❌ Error fetching {symbol}: {str(e)}")
        
        return None
    
    def fetch_etf_profile(self, symbol):
        """Fetch ETF profile information including expense ratio"""
        url = f"{self.base_url}/profile/{symbol}"
        params = {'apikey': self.fmp_api_key}
        
        try:
            response = self.session.get(url, params=params, timeout=30, verify=False)
            
            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    return data[0]
        except:
            pass
        
        return None
    
    def calculate_metrics(self, df):
        """Calculate volatility and other metrics"""
        if df is None or len(df) < 20:
            return {}
        
        # Calculate returns
        df['returns'] = df['close'].pct_change()
        
        # Annualized volatility (252 trading days)
        volatility = df['returns'].std() * np.sqrt(252)
        
        # Recent 30-day volatility
        recent_volatility = df['returns'].tail(30).std() * np.sqrt(252)
        
        # Average volume
        avg_volume = df['volume'].mean()
        
        # Price range
        price_range = df['close'].max() - df['close'].min()
        
        return {
            'volatility_annual': round(volatility, 4),
            'volatility_30d': round(recent_volatility, 4),
            'avg_volume': int(avg_volume),
            'price_range': round(price_range, 2),
            'latest_price': round(df['close'].iloc[-1], 2),
            'total_records': len(df)
        }
    
    def save_to_parquet(self, df, symbol):
        """Save DataFrame to Parquet format"""
        if df is None:
            return False
        
        parquet_path = self.parquet_dir / f"{symbol}_ohlcv_5y.parquet"
        
        try:
            # Select and order columns
            columns = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']
            if 'adjClose' in df.columns:
                columns.append('adjClose')
            
            df_save = df[columns].copy()
            
            # Convert to PyArrow table and save
            table = pa.Table.from_pandas(df_save)
            pq.write_table(table, parquet_path, compression='snappy')
            
            print(f"  💾 Saved Parquet: {parquet_path.name}")
            return True
            
        except Exception as e:
            print(f"  ❌ Failed to save Parquet: {e}")
            return False
    
    def save_to_csv(self, df, symbol):
        """Save DataFrame to CSV format"""
        if df is None:
            return False
        
        csv_path = self.csv_dir / f"{symbol}_ohlcv_5y.csv"
        
        try:
            df.to_csv(csv_path, index=False)
            print(f"  💾 Saved CSV: {csv_path.name}")
            return True
        except Exception as e:
            print(f"  ❌ Failed to save CSV: {e}")
            return False
    
    def setup_questdb_table(self):
        """Create QuestDB table for OHLCV data"""
        try:
            import psycopg2
            
            conn = psycopg2.connect(
                host=os.getenv('QUESTDB_HOST', 'localhost'),
                port=8812,  # PostgreSQL wire protocol port
                database='qdb',
                user=os.getenv('QUESTDB_USERNAME', 'admin'),
                password=os.getenv('QUESTDB_PASSWORD', 'quest')
            )
            
            cursor = conn.cursor()
            
            # Create table
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS ohlcv1d (
                symbol SYMBOL,
                date TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume LONG,
                adjClose DOUBLE,
                timestamp TIMESTAMP
            ) timestamp(timestamp) PARTITION BY MONTH;
            """
            
            cursor.execute(create_table_sql)
            conn.commit()
            
            print("✅ QuestDB table 'ohlcv1d' created successfully")
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            print(f"⚠️ QuestDB setup skipped (not running?): {e}")
            return False
    
    def insert_to_questdb(self, df, symbol):
        """Insert data into QuestDB"""
        try:
            import psycopg2
            
            conn = psycopg2.connect(
                host=os.getenv('QUESTDB_HOST', 'localhost'),
                port=8812,
                database='qdb',
                user=os.getenv('QUESTDB_USERNAME', 'admin'),
                password=os.getenv('QUESTDB_PASSWORD', 'quest')
            )
            
            cursor = conn.cursor()
            
            # Prepare data for insertion
            for _, row in df.iterrows():
                insert_sql = """
                INSERT INTO ohlcv1d (symbol, date, open, high, low, close, volume, adjClose, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                values = (
                    symbol,
                    row['date'],
                    float(row['open']),
                    float(row['high']),
                    float(row['low']),
                    float(row['close']),
                    int(row['volume']),
                    float(row.get('adjClose', row['close'])),
                    row['date']  # Use date as timestamp
                )
                
                cursor.execute(insert_sql, values)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"  📊 Inserted {len(df)} records to QuestDB")
            return True
            
        except Exception as e:
            print(f"  ⚠️ QuestDB insert skipped: {e}")
            return False
    
    def process_all_etfs(self):
        """Process all S&P 500 ETFs"""
        print("\n" + "="*60)
        print("🚀 Starting ETF Data Processing")
        print("="*60)
        
        # Setup QuestDB table
        self.setup_questdb_table()
        
        results = []
        
        for symbol in self.sp500_etfs:
            print(f"\n📊 Processing {symbol}...")
            
            # Fetch historical data
            df = self.fetch_historical_data(symbol, years=5)
            
            if df is not None:
                # Calculate metrics
                metrics = self.calculate_metrics(df)
                
                # Get ETF profile for expense ratio
                profile = self.fetch_etf_profile(symbol)
                expense_ratio = None
                if profile:
                    # Try different fields for expense ratio
                    expense_ratio = profile.get('expenseRatio') or profile.get('managementFee')
                
                # Save to different formats
                parquet_saved = self.save_to_parquet(df, symbol)
                csv_saved = self.save_to_csv(df, symbol)
                questdb_saved = self.insert_to_questdb(df, symbol)
                
                # Store results
                result = {
                    'symbol': symbol,
                    'name': profile.get('companyName', '') if profile else '',
                    'expense_ratio': expense_ratio,
                    **metrics,
                    'parquet_saved': parquet_saved,
                    'csv_saved': csv_saved,
                    'questdb_saved': questdb_saved
                }
                
                results.append(result)
            else:
                print(f"  ⚠️ Skipping {symbol} - no data available")
            
            # Rate limiting
            time.sleep(0.5)
        
        # Create summary DataFrame
        if results:
            summary_df = pd.DataFrame(results)
            
            # Sort by volatility (descending) and expense ratio (ascending)
            summary_df['expense_ratio_clean'] = pd.to_numeric(summary_df['expense_ratio'], errors='coerce')
            summary_df = summary_df.sort_values(
                by=['volatility_annual', 'expense_ratio_clean'],
                ascending=[False, True]
            )
            
            # Save summary
            summary_path = self.data_dir / f"etf_analysis_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            summary_df.to_csv(summary_path, index=False)
            
            # Print results
            print("\n" + "="*60)
            print("📊 ETF ANALYSIS RESULTS")
            print("Sorted by: High Volatility → Low Expense Ratio")
            print("="*60)
            
            for _, row in summary_df.head(10).iterrows():
                print(f"\n🏷️ {row['symbol']}")
                if row['name']:
                    print(f"   Name: {row['name'][:50]}")
                print(f"   📈 Volatility (Annual): {row['volatility_annual']:.2%}")
                print(f"   📊 Volatility (30d): {row['volatility_30d']:.2%}")
                if row['expense_ratio']:
                    print(f"   💰 Expense Ratio: {row['expense_ratio']}")
                print(f"   💵 Latest Price: ${row['latest_price']}")
                print(f"   📊 Avg Volume: {row['avg_volume']:,.0f}")
                print(f"   📁 Storage: Parquet {'✅' if row['parquet_saved'] else '❌'} | CSV {'✅' if row['csv_saved'] else '❌'} | QuestDB {'✅' if row['questdb_saved'] else '❌'}")
            
            print(f"\n💾 Summary saved to: {summary_path}")
            print(f"✅ Processing complete! Processed {len(results)} ETFs")
            
            return summary_df
        
        return None

def main():
    try:
        processor = ETFDataProcessor()
        results = processor.process_all_etfs()
        
        if results is not None:
            print("\n🎉 SUCCESS! All data processed and saved")
            print(f"📁 Check the 'data' folder for:")
            print(f"   • Parquet files in data/parquet/")
            print(f"   • CSV files in data/csv/")
            print(f"   • Summary analysis in data/")
            print(f"   • QuestDB table 'ohlcv1d' (if QuestDB is running)")
        else:
            print("\n❌ Processing failed - check error messages above")
            
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()