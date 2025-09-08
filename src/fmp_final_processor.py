#!/usr/bin/env python3
"""
Final FMP ETF Processor with QuestDB Integration
Tests multiple symbols and uses available ones
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

class FMPFinalProcessor:
    def __init__(self):
        self.api_key = os.getenv('FMP_API_KEY')
        if not self.api_key:
            raise ValueError("FMP_API_KEY not found in .env file")
        
        # Base URL for stable API
        self.base_url = "https://financialmodelingprep.com/stable"
        
        # Test symbols - mix of ETFs and popular stocks
        self.test_symbols = [
            'SPY', 'QQQ', 'IWM', 'DIA', 'VTI', 'VOO', 'IVV',  # ETFs
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA',  # Tech stocks
            'BRK-B', 'JPM', 'JNJ', 'V', 'PG', 'UNH', 'MA'  # Other large caps
        ]
        
        # Known expense ratios for ETFs
        self.etf_expense_ratios = {
            'SPY': 0.0945,
            'QQQ': 0.20,
            'IWM': 0.19,
            'DIA': 0.16,
            'VTI': 0.03,
            'VOO': 0.03,
            'IVV': 0.03
        }
        
        # Setup directories
        self.data_dir = Path('./data')
        self.parquet_dir = self.data_dir / 'parquet'
        self.csv_dir = self.data_dir / 'csv'
        
        for dir_path in [self.data_dir, self.parquet_dir, self.csv_dir]:
            dir_path.mkdir(exist_ok=True)
        
        print("✅ FMP Final Processor initialized")
        print(f"📊 Testing {len(self.test_symbols)} symbols")
    
    def test_symbol_availability(self, symbol):
        """Quick test if symbol data is available"""
        url = f"{self.base_url}/historical-price-eod/full"
        params = {
            'symbol': symbol,
            'apikey': self.api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=10, verify=False)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and len(data) > 0:
                    return True
            return False
        except:
            return False
    
    def fetch_historical_data(self, symbol):
        """Fetch historical data for symbol"""
        url = f"{self.base_url}/historical-price-eod/full"
        params = {
            'symbol': symbol,
            'apikey': self.api_key
        }
        
        print(f"📥 Fetching {symbol}...")
        
        try:
            response = requests.get(url, params=params, timeout=30, verify=False)
            
            if response.status_code == 200:
                data = response.json()
                
                if isinstance(data, list) and len(data) > 0:
                    df = pd.DataFrame(data)
                    
                    # Convert date
                    df['date'] = pd.to_datetime(df['date'])
                    
                    # Sort by date
                    df = df.sort_values('date')
                    
                    # Filter for last 5 years
                    five_years_ago = datetime.now() - timedelta(days=5*365)
                    df = df[df['date'] >= five_years_ago]
                    
                    # Ensure numeric types
                    numeric_cols = ['open', 'high', 'low', 'close', 'volume']
                    for col in numeric_cols:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    print(f"  ✅ Fetched {len(df)} records")
                    return df
                else:
                    print(f"  ⚠️ No data")
            elif response.status_code == 402:
                print(f"  💰 Payment required")
            else:
                print(f"  ❌ HTTP {response.status_code}")
                
        except Exception as e:
            print(f"  ❌ Error: {e}")
        
        return None
    
    def calculate_metrics(self, df):
        """Calculate volatility metrics"""
        if df is None or len(df) < 20:
            return {}
        
        # Calculate returns
        df['returns'] = df['close'].pct_change()
        
        # Annualized volatility
        volatility = df['returns'].std() * np.sqrt(252)
        
        # 30-day volatility
        recent_vol = df['returns'].tail(30).std() * np.sqrt(252) if len(df) >= 30 else volatility
        
        return {
            'volatility': round(volatility * 100, 2),
            'volatility_30d': round(recent_vol * 100, 2),
            'avg_volume': int(df['volume'].mean()),
            'latest_price': round(df['close'].iloc[-1], 2),
            'total_records': len(df),
            'date_range': f"{df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}"
        }
    
    def save_parquet(self, df, symbol):
        """Save to Parquet"""
        try:
            save_cols = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']
            df_save = df[save_cols].copy()
            
            path = self.parquet_dir / f"{symbol.replace('-', '_')}_ohlcv_5y.parquet"
            table = pa.Table.from_pandas(df_save)
            pq.write_table(table, path, compression='snappy')
            print(f"  💾 Parquet: {path.name}")
            return True
        except Exception as e:
            print(f"  ❌ Parquet error: {e}")
            return False
    
    def save_csv(self, df, symbol):
        """Save to CSV"""
        try:
            path = self.csv_dir / f"{symbol.replace('-', '_')}_ohlcv_5y.csv"
            df.to_csv(path, index=False)
            print(f"  💾 CSV: {path.name}")
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
            
            # Create table if not exists
            create_sql = """
            CREATE TABLE IF NOT EXISTS ohlcv1d (
                symbol SYMBOL,
                date TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume LONG,
                timestamp TIMESTAMP
            ) timestamp(timestamp) PARTITION BY MONTH
            """
            
            cursor.execute(create_sql)
            conn.commit()
            
            cursor.close()
            conn.close()
            
            print("✅ QuestDB table ready")
            return True
            
        except Exception as e:
            print(f"⚠️ QuestDB not available: {e}")
            return False
    
    def insert_questdb(self, df, symbol):
        """Insert to QuestDB"""
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
            
            # Delete existing data for this symbol
            cursor.execute(f"DELETE FROM ohlcv1d WHERE symbol = '{symbol}'")
            
            # Insert new data
            for _, row in df.iterrows():
                sql = """
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
                
                cursor.execute(sql, values)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"  📊 QuestDB: {len(df)} records")
            return True
            
        except Exception as e:
            print(f"  ⚠️ QuestDB skip: {e}")
            return False
    
    def process_all(self):
        """Process all available symbols"""
        print("\n" + "="*60)
        print("🚀 Starting FMP Processing")
        print("="*60)
        
        # Test which symbols are available
        print("\n🔍 Testing symbol availability...")
        available_symbols = []
        
        for symbol in self.test_symbols:
            print(f"  Testing {symbol}...", end='')
            if self.test_symbol_availability(symbol):
                print(" ✅ Available")
                available_symbols.append(symbol)
            else:
                print(" ❌ Not available")
            time.sleep(0.2)  # Rate limiting
        
        if not available_symbols:
            print("❌ No symbols available with current API key")
            return None
        
        print(f"\n✅ Found {len(available_symbols)} available symbols")
        
        # Setup QuestDB
        questdb_available = self.setup_questdb()
        
        results = []
        
        # Process available symbols
        for symbol in available_symbols:
            print(f"\n📊 Processing {symbol}")
            
            df = self.fetch_historical_data(symbol)
            
            if df is not None:
                metrics = self.calculate_metrics(df)
                
                # Save data
                parquet_ok = self.save_parquet(df, symbol)
                csv_ok = self.save_csv(df, symbol)
                questdb_ok = self.insert_questdb(df, symbol) if questdb_available else False
                
                # Get expense ratio if ETF
                expense_ratio = self.etf_expense_ratios.get(symbol)
                is_etf = symbol in self.etf_expense_ratios
                
                results.append({
                    'symbol': symbol,
                    'type': 'ETF' if is_etf else 'Stock',
                    'expense_ratio': expense_ratio,
                    **metrics,
                    'parquet': parquet_ok,
                    'csv': csv_ok,
                    'questdb': questdb_ok
                })
            
            time.sleep(0.5)  # Rate limiting
        
        # Create summary
        if results:
            summary = pd.DataFrame(results)
            
            # Separate ETFs and stocks
            etfs = summary[summary['type'] == 'ETF'].copy()
            stocks = summary[summary['type'] == 'Stock'].copy()
            
            # Sort ETFs by volatility and expense ratio
            if not etfs.empty:
                etfs = etfs.sort_values(
                    by=['volatility', 'expense_ratio'],
                    ascending=[False, True]
                )
            
            # Sort stocks by volatility
            if not stocks.empty:
                stocks = stocks.sort_values('volatility', ascending=False)
            
            # Save summary
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            summary_path = self.data_dir / f"fmp_analysis_{timestamp}.csv"
            summary.to_csv(summary_path, index=False)
            
            # Print ETF results
            if not etfs.empty:
                print("\n" + "="*60)
                print("📊 ETF ANALYSIS (High Volatility → Low Expense)")
                print("="*60)
                
                for _, row in etfs.iterrows():
                    print(f"\n🏷️ {row['symbol']}")
                    print(f"   📈 Volatility: {row['volatility']:.2f}%")
                    if row['expense_ratio']:
                        print(f"   💰 Expense Ratio: {row['expense_ratio']:.4f}%")
                    print(f"   💵 Latest Price: ${row['latest_price']:.2f}")
                    print(f"   📊 Avg Volume: {row['avg_volume']:,.0f}")
            
            # Print stock results
            if not stocks.empty:
                print("\n" + "="*60)
                print("📊 STOCK ANALYSIS (Sorted by Volatility)")
                print("="*60)
                
                for _, row in stocks.head(5).iterrows():
                    print(f"\n🏷️ {row['symbol']}")
                    print(f"   📈 Volatility: {row['volatility']:.2f}%")
                    print(f"   💵 Latest Price: ${row['latest_price']:.2f}")
                    print(f"   📊 Avg Volume: {row['avg_volume']:,.0f}")
            
            print(f"\n💾 Summary saved to: {summary_path}")
            print(f"✅ Processed {len(results)} symbols successfully!")
            
            # Best ETF recommendation
            if not etfs.empty:
                top_etf = etfs.iloc[0]
                print(f"\n🏆 TOP ETF: {top_etf['symbol']}")
                print(f"   Volatility: {top_etf['volatility']:.2f}%")
                if top_etf['expense_ratio']:
                    print(f"   Expense: {top_etf['expense_ratio']:.4f}%")
            
            return summary
        
        return None

def main():
    try:
        processor = FMPFinalProcessor()
        results = processor.process_all()
        
        if results is not None:
            print("\n🎉 SUCCESS!")
            print(f"📁 Data saved in:")
            print(f"   • Parquet: data/parquet/")
            print(f"   • CSV: data/csv/")
            print(f"   • Summary: data/")
            if any(results['questdb']):
                print(f"   • QuestDB: ohlcv1d table")
        else:
            print("\n❌ Processing failed")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()