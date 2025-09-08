#!/usr/bin/env python3
"""
FMP Stable API ETF Processor with QuestDB Integration
Uses the new stable endpoints for FMP API
Fetches 5-year data and saves to Parquet, CSV, and QuestDB
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
import urllib3

# Fix Windows console encoding
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load environment variables
load_dotenv()

class FMPStableProcessor:
    def __init__(self):
        self.api_key = os.getenv('FMP_API_KEY')
        if not self.api_key:
            raise ValueError("FMP_API_KEY not found in .env file")
        
        # Base URL for stable API
        self.base_url = "https://financialmodelingprep.com/stable"
        
        # S&P 500 ETFs with known expense ratios
        self.sp500_etfs = {
            'SPY': {'name': 'SPDR S&P 500 ETF', 'expense_ratio': 0.0945},
            'VOO': {'name': 'Vanguard S&P 500 ETF', 'expense_ratio': 0.03},
            'IVV': {'name': 'iShares Core S&P 500 ETF', 'expense_ratio': 0.03},
            'SPLG': {'name': 'SPDR Portfolio S&P 500 ETF', 'expense_ratio': 0.02},
            'RSP': {'name': 'Invesco S&P 500 Equal Weight', 'expense_ratio': 0.20},
            'SSO': {'name': 'ProShares Ultra S&P 500', 'expense_ratio': 0.89},
            'UPRO': {'name': 'ProShares UltraPro S&P 500', 'expense_ratio': 0.92},
            'SH': {'name': 'ProShares Short S&P 500', 'expense_ratio': 0.88},
            'SDS': {'name': 'ProShares UltraShort S&P 500', 'expense_ratio': 0.90},
            'SPXU': {'name': 'ProShares UltraPro Short S&P', 'expense_ratio': 0.90}
        }
        
        # Setup directories
        self.data_dir = Path('./data')
        self.parquet_dir = self.data_dir / 'parquet'
        self.csv_dir = self.data_dir / 'csv'
        
        for dir_path in [self.data_dir, self.parquet_dir, self.csv_dir]:
            dir_path.mkdir(exist_ok=True)
        
        print("✅ FMP Stable API Processor initialized")
        print(f"📊 Processing {len(self.sp500_etfs)} S&P 500 ETFs")
    
    def fetch_historical_data(self, symbol):
        """Fetch historical data using stable API endpoint"""
        url = f"{self.base_url}/historical-price-eod/full"
        params = {
            'symbol': symbol,
            'apikey': self.api_key
        }
        
        print(f"📥 Fetching {symbol} data...")
        
        try:
            response = requests.get(url, params=params, timeout=30, verify=False)
            
            if response.status_code == 200:
                data = response.json()
                
                if isinstance(data, list) and len(data) > 0:
                    df = pd.DataFrame(data)
                    
                    # Convert date to datetime
                    df['date'] = pd.to_datetime(df['date'])
                    
                    # Sort by date
                    df = df.sort_values('date')
                    
                    # Filter for last 5 years
                    five_years_ago = datetime.now() - timedelta(days=5*365)
                    df = df[df['date'] >= five_years_ago]
                    
                    # Ensure numeric types
                    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'vwap']
                    for col in numeric_cols:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    print(f"  ✅ Fetched {len(df)} records")
                    return df
                else:
                    print(f"  ⚠️ No data returned")
            else:
                print(f"  ❌ HTTP {response.status_code}")
                
        except Exception as e:
            print(f"  ❌ Error: {e}")
        
        return None
    
    def fetch_etf_info(self, symbol):
        """Fetch ETF information from stable API"""
        url = f"{self.base_url}/etf/info"
        params = {
            'symbol': symbol,
            'apikey': self.api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=30, verify=False)
            
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and len(data) > 0:
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
        
        # Annualized volatility
        volatility = df['returns'].std() * np.sqrt(252)
        
        # 30-day volatility
        recent_vol = df['returns'].tail(30).std() * np.sqrt(252) if len(df) >= 30 else volatility
        
        # Average volume
        avg_volume = df['volume'].mean()
        
        return {
            'volatility': round(volatility * 100, 2),
            'volatility_30d': round(recent_vol * 100, 2),
            'avg_volume': int(avg_volume),
            'latest_price': round(df['close'].iloc[-1], 2),
            'total_records': len(df),
            'date_range': f"{df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}"
        }
    
    def save_parquet(self, df, symbol):
        """Save to Parquet format"""
        try:
            # Select columns for saving
            save_cols = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']
            if 'vwap' in df.columns:
                save_cols.append('vwap')
            
            df_save = df[save_cols].copy()
            
            path = self.parquet_dir / f"{symbol}_ohlcv_5y.parquet"
            table = pa.Table.from_pandas(df_save)
            pq.write_table(table, path, compression='snappy')
            print(f"  💾 Saved Parquet: {path.name}")
            return True
        except Exception as e:
            print(f"  ❌ Parquet error: {e}")
            return False
    
    def save_csv(self, df, symbol):
        """Save to CSV format"""
        try:
            path = self.csv_dir / f"{symbol}_ohlcv_5y.csv"
            df.to_csv(path, index=False)
            print(f"  💾 Saved CSV: {path.name}")
            return True
        except Exception as e:
            print(f"  ❌ CSV error: {e}")
            return False
    
    def setup_questdb(self):
        """Setup QuestDB table"""
        try:
            import psycopg2
            
            conn = psycopg2.connect(
                host='localhost',
                port=8812,
                database='qdb',
                user='admin',
                password='quest'
            )
            
            cursor = conn.cursor()
            
            # Drop and recreate table
            cursor.execute("DROP TABLE IF EXISTS ohlcv1d")
            
            create_sql = """
            CREATE TABLE ohlcv1d (
                symbol SYMBOL,
                date TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume LONG,
                vwap DOUBLE,
                timestamp TIMESTAMP
            ) timestamp(timestamp) PARTITION BY MONTH
            """
            
            cursor.execute(create_sql)
            conn.commit()
            
            cursor.close()
            conn.close()
            
            print("✅ QuestDB table 'ohlcv1d' created")
            return True
            
        except Exception as e:
            print(f"⚠️ QuestDB not available: {e}")
            return False
    
    def insert_questdb(self, df, symbol):
        """Insert data into QuestDB"""
        try:
            import psycopg2
            
            conn = psycopg2.connect(
                host='localhost',
                port=8812,
                database='qdb',
                user='admin',
                password='quest'
            )
            
            cursor = conn.cursor()
            
            for _, row in df.iterrows():
                sql = """
                INSERT INTO ohlcv1d (symbol, date, open, high, low, close, volume, vwap, timestamp)
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
                    float(row.get('vwap', 0)),
                    row['date']
                )
                
                cursor.execute(sql, values)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"  📊 Inserted {len(df)} records to QuestDB")
            return True
            
        except Exception as e:
            print(f"  ⚠️ QuestDB insert skipped: {e}")
            return False
    
    def process_all(self):
        """Process all ETFs"""
        print("\n" + "="*60)
        print("🚀 Starting FMP Stable API Processing")
        print("="*60)
        
        # Test API connection
        print("\n🔍 Testing API connection...")
        test_df = self.fetch_historical_data('SPY')
        if test_df is None:
            print("❌ API connection failed. Check your API key.")
            return None
        print("✅ API connection successful!")
        
        # Setup QuestDB
        questdb_available = self.setup_questdb()
        
        results = []
        
        # Process SPY first (already fetched)
        symbol = 'SPY'
        info = self.sp500_etfs[symbol]
        print(f"\n📊 Processing {symbol} - {info['name']}")
        
        if test_df is not None:
            metrics = self.calculate_metrics(test_df)
            parquet_ok = self.save_parquet(test_df, symbol)
            csv_ok = self.save_csv(test_df, symbol)
            questdb_ok = self.insert_questdb(test_df, symbol) if questdb_available else False
            
            # Try to get updated expense ratio from API
            etf_info = self.fetch_etf_info(symbol)
            expense_ratio = info['expense_ratio']
            if etf_info and 'expenseRatio' in etf_info:
                expense_ratio = float(etf_info['expenseRatio'])
            
            results.append({
                'symbol': symbol,
                'name': info['name'],
                'expense_ratio': expense_ratio,
                **metrics,
                'parquet': parquet_ok,
                'csv': csv_ok,
                'questdb': questdb_ok
            })
        
        # Process remaining ETFs
        for symbol, info in list(self.sp500_etfs.items())[1:]:
            print(f"\n📊 Processing {symbol} - {info['name']}")
            
            df = self.fetch_historical_data(symbol)
            
            if df is not None:
                metrics = self.calculate_metrics(df)
                parquet_ok = self.save_parquet(df, symbol)
                csv_ok = self.save_csv(df, symbol)
                questdb_ok = self.insert_questdb(df, symbol) if questdb_available else False
                
                # Try to get updated expense ratio
                etf_info = self.fetch_etf_info(symbol)
                expense_ratio = info['expense_ratio']
                if etf_info and 'expenseRatio' in etf_info:
                    expense_ratio = float(etf_info['expenseRatio'])
                
                results.append({
                    'symbol': symbol,
                    'name': info['name'],
                    'expense_ratio': expense_ratio,
                    **metrics,
                    'parquet': parquet_ok,
                    'csv': csv_ok,
                    'questdb': questdb_ok
                })
            
            # Rate limiting
            time.sleep(0.5)
        
        # Create summary
        if results:
            summary = pd.DataFrame(results)
            
            # Sort by volatility (high) and expense ratio (low)
            summary = summary.sort_values(
                by=['volatility', 'expense_ratio'],
                ascending=[False, True]
            )
            
            # Save summary
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            summary_path = self.data_dir / f"etf_fmp_analysis_{timestamp}.csv"
            summary.to_csv(summary_path, index=False)
            
            # Print results
            print("\n" + "="*60)
            print("📊 ETF ANALYSIS RESULTS (FMP Stable API)")
            print("Sorted by: High Volatility → Low Expense Ratio")
            print("="*60)
            
            for idx, row in summary.iterrows():
                print(f"\n🏷️ {row['symbol']} - {row['name']}")
                print(f"   📈 Volatility: {row['volatility']:.2f}%")
                print(f"   💰 Expense Ratio: {row['expense_ratio']:.4f}%")
                print(f"   💵 Latest Price: ${row['latest_price']:.2f}")
                print(f"   📊 Avg Volume: {row['avg_volume']:,.0f}")
                print(f"   📅 Date Range: {row['date_range']}")
                status = []
                if row['parquet']: status.append('Parquet ✅')
                if row['csv']: status.append('CSV ✅')
                if row['questdb']: status.append('QuestDB ✅')
                print(f"   💾 Saved: {' | '.join(status)}")
            
            print(f"\n💾 Summary saved to: {summary_path}")
            print(f"✅ Processing complete! Processed {len(results)} ETFs")
            
            # Top recommendation
            if len(summary) > 0:
                top = summary.iloc[0]
                print(f"\n🏆 TOP PICK: {top['symbol']} - {top['name']}")
                print(f"   Best combination of high volatility ({top['volatility']:.2f}%) and low fees ({top['expense_ratio']:.4f}%)")
            
            return summary
        
        return None

def main():
    try:
        processor = FMPStableProcessor()
        results = processor.process_all()
        
        if results is not None:
            print("\n🎉 SUCCESS! All FMP data processed")
            print(f"📁 Check the 'data' folder for:")
            print(f"   • Parquet files in data/parquet/")
            print(f"   • CSV files in data/csv/")
            print(f"   • Analysis summary in data/")
            if any(results['questdb']):
                print(f"   • QuestDB table 'ohlcv1d'")
        else:
            print("\n❌ No data processed")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()