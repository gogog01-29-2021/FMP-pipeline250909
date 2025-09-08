#!/usr/bin/env python3
"""
ETF Data Processor - QuestDB Optional Version
This version works with or without QuestDB running
"""

import pandas as pd
import requests
import json
import os
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

class ETFDataProcessorLite:
    def __init__(self):
        self.fmp_api_key = os.getenv('FMP_API_KEY')
        if not self.fmp_api_key or self.fmp_api_key == 'your_actual_fmp_api_key_here':
            raise ValueError("Please set your FMP_API_KEY in the .env file")
        
        self.base_url = "https://financialmodelingprep.com/api/v3"
        
        # ETF symbols for your specified indices
        self.etf_symbols = {
            'SPY': 'SPDR S&P 500 ETF Trust',  # S&P 500 index
            'VXX': 'iPath Series B S&P 500 VIX Short-Term Futures ETN',  # VIX volatility
            'QQQ': 'Invesco QQQ Trust ETF',  # Nasdaq 100 index
            'IWM': 'iShares Russell 2000 ETF'  # Russell 2000 index
        }
        
        # Setup directories
        self.data_dir = Path('./data')
        self.parquet_dir = Path('./data/parquet')
        self.csv_dir = Path('./data/csv')
        
        # Create directories
        self.data_dir.mkdir(exist_ok=True)
        self.parquet_dir.mkdir(exist_ok=True)
        self.csv_dir.mkdir(exist_ok=True)
        
        print(f"✅ Initialized ETF Data Processor (Lite)")
        print(f"📁 Data directory: {self.data_dir}")
        print(f"🎯 Processing ETFs: {list(self.etf_symbols.keys())}")
    
    def get_historical_data(self, symbol, years=5):
        """Fetch 5 years of historical price data"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=years*365 + 30)
        
        url = f"{self.base_url}/historical-price-full/{symbol}"
        params = {
            'from': start_date.strftime('%Y-%m-%d'),
            'to': end_date.strftime('%Y-%m-%d'),
            'apikey': self.fmp_api_key
        }
        
        print(f"📊 Fetching {years} years of data for {symbol}...")
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if 'historical' in data and data['historical']:
                df = pd.DataFrame(data['historical'])
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date').reset_index(drop=True)
                
                print(f"✅ Successfully fetched {len(df)} data points for {symbol}")
                print(f"📅 Date range: {df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}")
                return df
            else:
                print(f"❌ No historical data found for {symbol}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching historical data for {symbol}: {e}")
            return None
        except Exception as e:
            print(f"❌ Unexpected error processing {symbol}: {e}")
            return None
    
    def calculate_volatility(self, df, window=252):
        """Calculate annualized volatility"""
        if df is None or len(df) < window:
            return None
        
        df_copy = df.copy()
        df_copy['returns'] = df_copy['close'].pct_change()
        df_copy['volatility'] = df_copy['returns'].rolling(window=window).std() * np.sqrt(252)
        
        recent_volatility = df_copy['volatility'].dropna().iloc[-1]
        return round(recent_volatility, 4)
    
    def get_etf_expense_ratio(self, symbol):
        """Try to get expense ratio from multiple FMP endpoints"""
        endpoints = [
            f"{self.base_url}/etf-info",
            f"{self.base_url}/profile/{symbol}"
        ]
        
        for endpoint in endpoints:
            try:
                if 'etf-info' in endpoint:
                    params = {'symbol': symbol, 'apikey': self.fmp_api_key}
                else:
                    params = {'apikey': self.fmp_api_key}
                    endpoint = f"{endpoint}"
                
                response = requests.get(endpoint, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                if data and len(data) > 0:
                    info = data[0]
                    expense_ratio = info.get('expenseRatio') or info.get('managementFee')
                    if expense_ratio:
                        return float(expense_ratio)
                        
            except Exception as e:
                continue
        
        return None
    
    def save_to_parquet(self, df, symbol):
        """Save DataFrame to Parquet format"""
        parquet_path = self.parquet_dir / f"{symbol}_5y_ohlcv.parquet"
        
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            # Ensure proper data types
            df_clean = df.copy()
            df_clean['date'] = pd.to_datetime(df_clean['date'])
            
            # Convert to proper types for Parquet
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_cols:
                if col in df_clean.columns:
                    df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
            
            table = pa.Table.from_pandas(df_clean)
            pq.write_table(table, parquet_path, compression='snappy')
            
            print(f"✅ Saved Parquet: {parquet_path}")
            return parquet_path
            
        except Exception as e:
            print(f"❌ Error saving Parquet for {symbol}: {e}")
            return None
    
    def save_to_csv(self, df, symbol):
        """Save DataFrame to CSV format"""
        csv_path = self.csv_dir / f"{symbol}_5y_ohlcv.csv"
        
        try:
            df.to_csv(csv_path, index=False)
            print(f"✅ Saved CSV: {csv_path}")
            return csv_path
            
        except Exception as e:
            print(f"❌ Error saving CSV for {symbol}: {e}")
            return None
    
    def process_single_etf(self, symbol):
        """Process a single ETF"""
        print(f"\n{'='*60}")
        print(f"Processing {symbol} - {self.etf_symbols[symbol]}")
        print(f"{'='*60}")
        
        # Get historical data
        df = self.get_historical_data(symbol, years=5)
        if df is None:
            return None
        
        # Calculate volatility
        volatility = self.calculate_volatility(df)
        
        # Try to get expense ratio
        expense_ratio = self.get_etf_expense_ratio(symbol)
        
        # Save data in both formats
        parquet_saved = self.save_to_parquet(df, symbol)
        csv_saved = self.save_to_csv(df, symbol)
        
        # Rate limiting for API
        time.sleep(1)
        
        return {
            'symbol': symbol,
            'name': self.etf_symbols[symbol],
            'volatility': volatility,
            'expense_ratio': expense_ratio,
            'data_points': len(df),
            'date_range': f"{df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}",
            'parquet_saved': parquet_saved is not None,
            'csv_saved': csv_saved is not None,
            'avg_volume': int(df['volume'].mean()),
            'latest_price': float(df['close'].iloc[-1])
        }
    
    def process_all_etfs(self):
        """Process all ETFs and create analysis"""
        print(f"\n🚀 Starting ETF Data Processing (Lite Version)")
        print(f"🎯 Target ETFs: {list(self.etf_symbols.keys())}")
        print(f"📊 Data period: 5 years")
        print(f"💾 Output formats: Parquet, CSV")
        print(f"🏦 QuestDB: Will be skipped if not available")
        
        etf_results = []
        
        for symbol in self.etf_symbols.keys():
            result = self.process_single_etf(symbol)
            if result:
                etf_results.append(result)
        
        if not etf_results:
            print("❌ No ETF data was processed successfully")
            return None
        
        # Create analysis DataFrame
        analysis_df = pd.DataFrame(etf_results)
        
        # Sort by volatility (descending) and expense ratio (ascending)
        analysis_df['volatility_sort'] = analysis_df['volatility'].fillna(0)
        analysis_df['expense_ratio_sort'] = analysis_df['expense_ratio'].fillna(999)
        
        analysis_df = analysis_df.sort_values(
            ['volatility_sort', 'expense_ratio_sort'], 
            ascending=[False, True]
        )
        
        # Drop helper columns
        analysis_df = analysis_df.drop(['volatility_sort', 'expense_ratio_sort'], axis=1)
        
        # Save analysis results
        analysis_path = self.data_dir / "etf_analysis_results.csv"
        analysis_df.to_csv(analysis_path, index=False)
        
        # Print results
        print(f"\n{'='*80}")
        print("📊 ETF ANALYSIS RESULTS")
        print("📈 Ranked by: Higher Volatility → Lower Expense Ratio")
        print(f"{'='*80}")
        
        for _, row in analysis_df.iterrows():
            print(f"\n🏷️  {row['symbol']} - {row['name']}")
            print(f"   📊 Volatility: {row['volatility']:.4f}" if row['volatility'] else "   📊 Volatility: N/A")
            print(f"   💰 Expense Ratio: {row['expense_ratio']:.4f}%" if row['expense_ratio'] else "   💰 Expense Ratio: N/A")
            print(f"   💵 Latest Price: ${row['latest_price']:.2f}")
            print(f"   📊 Avg Volume: {row['avg_volume']:,}")
            print(f"   📈 Data Points: {row['data_points']:,}")
            print(f"   📅 Date Range: {row['date_range']}")
            print(f"   💾 Files: Parquet: {'✅' if row['parquet_saved'] else '❌'} | CSV: {'✅' if row['csv_saved'] else '❌'}")
        
        print(f"\n💾 Analysis saved to: {analysis_path}")
        print(f"📁 Parquet files: {self.parquet_dir}")
        print(f"📁 CSV files: {self.csv_dir}")
        
        return analysis_df

def main():
    """Main execution function"""
    try:
        processor = ETFDataProcessorLite()
        results = processor.process_all_etfs()
        
        if results is not None:
            print(f"\n🎉 ETF DATA PROCESSING COMPLETED!")
            print(f"📊 Successfully processed {len(results)} ETFs")
            print(f"📁 Check data/ folder for all output files")
            print(f"")
            print(f"🔧 Next steps:")
            print(f"   1. ✅ Data files ready in Parquet and CSV formats")
            print(f"   2. 🗄️  Optional: Start QuestDB for database features") 
            print(f"   3. 📊 Use data for analysis, visualization, trading strategies")
            
            # Show file summary
            parquet_files = list(Path('./data/parquet').glob('*.parquet'))
            csv_files = list(Path('./data/csv').glob('*.csv'))
            
            print(f"\n📋 Files created:")
            print(f"   📊 Parquet files: {len(parquet_files)}")
            print(f"   📄 CSV files: {len(csv_files)}")
            print(f"   📈 Analysis file: etf_analysis_results.csv")
            
        else:
            print(f"\n❌ ETF data processing failed")
            
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        print("Please check your .env file and ensure FMP_API_KEY is set")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
