import pandas as pd
import requests
import json
import os
import numpy as np
from datetime import datetime, timedelta
from questdb.ingress import Sender, IngressError, TimestampNanos
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
import time
from pathlib import Path

# Load environment variables
load_dotenv()

class ETFDataProcessor:
    def __init__(self):
        self.fmp_api_key = os.getenv('FMP_API_KEY')
        if not self.fmp_api_key or self.fmp_api_key == 'your_fmp_api_key_here':
            raise ValueError("Please set your FMP_API_KEY in the .env file")
        
        self.base_url = "https://financialmodelingprep.com/api/v3"
        
        # ETF symbols for your specified indices
        # Using most liquid ETFs for better data availability
        self.etf_symbols = {
            'SPY': 'SPDR S&P 500 ETF Trust',  # S&P 500 index
            'VXX': 'iPath Series B S&P 500 VIX Short-Term Futures ETN',  # VIX volatility
            'QQQ': 'Invesco QQQ Trust ETF',  # Nasdaq 100 index
            'IWM': 'iShares Russell 2000 ETF'  # Russell 2000 index
        }
        
        # Setup directories
        self.data_dir = Path(os.getenv('DATA_DIR', './data'))
        self.parquet_dir = Path(os.getenv('PARQUET_DIR', './data/parquet'))
        self.csv_dir = Path(os.getenv('CSV_DIR', './data/csv'))
        
        # Create directories
        self.data_dir.mkdir(exist_ok=True)
        self.parquet_dir.mkdir(exist_ok=True)
        self.csv_dir.mkdir(exist_ok=True)
        
        print(f"Initialized ETF Data Processor")
        print(f"Data directory: {self.data_dir}")
        print(f"Processing ETFs: {list(self.etf_symbols.keys())}")
    
    def get_etf_profile(self, symbol):
        """Get ETF profile including expense ratio"""
        url = f"{self.base_url}/etf-holder/{symbol}"
        params = {'apikey': self.fmp_api_key}
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return data if data else None
        except requests.exceptions.RequestException as e:
            print(f"Error fetching ETF profile for {symbol}: {e}")
            return None
    
    def get_etf_info(self, symbol):
        """Get ETF basic information including expense ratio"""
        url = f"{self.base_url}/etf-info"
        params = {
            'symbol': symbol,
            'apikey': self.fmp_api_key
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return data[0] if data and len(data) > 0 else None
        except requests.exceptions.RequestException as e:
            print(f"Error fetching ETF info for {symbol}: {e}")
            return None
    
    def get_historical_data(self, symbol, years=5):
        """Fetch 5 years of historical price data"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=years*365 + 30)  # Add buffer for weekends
        
        url = f"{self.base_url}/historical-price-full/{symbol}"
        params = {
            'from': start_date.strftime('%Y-%m-%d'),
            'to': end_date.strftime('%Y-%m-%d'),
            'apikey': self.fmp_api_key
        }
        
        print(f"Fetching {years} years of data for {symbol}...")
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if 'historical' in data and data['historical']:
                df = pd.DataFrame(data['historical'])
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date').reset_index(drop=True)
                
                print(f"Successfully fetched {len(df)} data points for {symbol}")
                print(f"Date range: {df['date'].min()} to {df['date'].max()}")
                return df
            else:
                print(f"No historical data found for {symbol}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching historical data for {symbol}: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error processing {symbol}: {e}")
            return None
    
    def calculate_volatility(self, df, window=252):
        """Calculate annualized volatility (252 trading days)"""
        if df is None or len(df) < window:
            return None
        
        df_copy = df.copy()
        df_copy['returns'] = df_copy['close'].pct_change()
        
        # Calculate rolling volatility
        df_copy['volatility'] = df_copy['returns'].rolling(window=window).std() * np.sqrt(252)
        
        # Return the most recent volatility value
        recent_volatility = df_copy['volatility'].dropna().iloc[-1]
        return round(recent_volatility, 4)
    
    def save_to_parquet(self, df, symbol):
        """Save DataFrame to Parquet format"""
        parquet_path = self.parquet_dir / f"{symbol}_5y_ohlcv.parquet"
        
        try:
            # Ensure proper data types for Parquet
            df_clean = df.copy()
            df_clean['date'] = pd.to_datetime(df_clean['date'])
            df_clean = df_clean.astype({
                'open': 'float64',
                'high': 'float64', 
                'low': 'float64',
                'close': 'float64',
                'volume': 'int64'
            })
            
            table = pa.Table.from_pandas(df_clean)
            pq.write_table(table, parquet_path, compression='snappy')
            
            print(f"✅ Saved {symbol} data to Parquet: {parquet_path}")
            return parquet_path
            
        except Exception as e:
            print(f"❌ Error saving {symbol} to Parquet: {e}")
            return None
    
    def save_to_csv(self, df, symbol):
        """Save DataFrame to CSV format"""
        csv_path = self.csv_dir / f"{symbol}_5y_ohlcv.csv"
        
        try:
            df.to_csv(csv_path, index=False)
            print(f"✅ Saved {symbol} data to CSV: {csv_path}")
            return csv_path
            
        except Exception as e:
            print(f"❌ Error saving {symbol} to CSV: {e}")
            return None
    
    def create_questdb_table(self, df, symbol):
        """Create and populate QuestDB table (ohlcv1d)"""
        if df is None or df.empty:
            print(f"❌ No data to insert for {symbol}")
            return False
        
        questdb_host = os.getenv('QUESTDB_HOST', 'localhost')
        questdb_port = int(os.getenv('QUESTDB_PORT', '9009'))
        
        try:
            with Sender(questdb_host, questdb_port) as sender:
                for _, row in df.iterrows():
                    sender.row(
                        'ohlcv1d',  # Table name as requested
                        symbols={
                            'symbol': symbol,
                            'name': self.etf_symbols.get(symbol, symbol)
                        },
                        columns={
                            'open': float(row['open']),
                            'high': float(row['high']),
                            'low': float(row['low']),
                            'close': float(row['close']),
                            'volume': int(row['volume']),
                            'adj_close': float(row.get('adjClose', row['close']))
                        },
                        at=TimestampNanos.from_datetime(row['date'])
                    )
                
                sender.flush()
            
            print(f"✅ Successfully inserted {len(df)} records for {symbol} into QuestDB (ohlcv1d table)")
            return True
            
        except IngressError as e:
            print(f"❌ QuestDB ingress error for {symbol}: {e}")
            return False
        except Exception as e:
            print(f"❌ Error inserting {symbol} into QuestDB: {e}")
            return False
    
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
        
        # Get ETF profile for expense ratio
        etf_info = self.get_etf_info(symbol)
        expense_ratio = None
        if etf_info:
            expense_ratio = etf_info.get('expenseRatio', etf_info.get('managementFee'))
        
        # Save data in both formats
        parquet_saved = self.save_to_parquet(df, symbol)
        csv_saved = self.save_to_csv(df, symbol)
        
        # Insert into QuestDB
        questdb_success = self.create_questdb_table(df, symbol)
        
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
            'questdb_inserted': questdb_success
        }
    
    def process_all_etfs(self):
        """Process all ETFs and rank by volatility (higher) and expense ratio (lower)"""
        print(f"\n🚀 Starting ETF Data Processing")
        print(f"Target ETFs: {list(self.etf_symbols.keys())}")
        print(f"Data period: 5 years")
        print(f"Output formats: Parquet, CSV, QuestDB")
        
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
        # Handle None values in sorting
        analysis_df['volatility_sort'] = analysis_df['volatility'].fillna(0)
        analysis_df['expense_ratio_sort'] = analysis_df['expense_ratio'].fillna(999)
        
        analysis_df = analysis_df.sort_values(
            ['volatility_sort', 'expense_ratio_sort'], 
            ascending=[False, True]  # Higher volatility first, lower fees first
        )
        
        # Drop helper columns
        analysis_df = analysis_df.drop(['volatility_sort', 'expense_ratio_sort'], axis=1)
        
        # Save analysis results
        analysis_path = self.data_dir / "etf_analysis_results.csv"
        analysis_df.to_csv(analysis_path, index=False)
        
        # Print results
        print(f"\n{'='*80}")
        print("📊 ETF ANALYSIS RESULTS")
        print("📈 Ranked by: Higher Volatility first, Lower Expense Ratio first")
        print(f"{'='*80}")
        
        for _, row in analysis_df.iterrows():
            print(f"\n🏷️  {row['symbol']} - {row['name']}")
            print(f"   📊 Volatility: {row['volatility']:.4f}" if row['volatility'] else "   📊 Volatility: N/A")
            print(f"   💰 Expense Ratio: {row['expense_ratio']:.4f}%" if row['expense_ratio'] else "   💰 Expense Ratio: N/A")
            print(f"   📈 Data Points: {row['data_points']}")
            print(f"   📅 Date Range: {row['date_range']}")
            print(f"   💾 Files: Parquet: {'✅' if row['parquet_saved'] else '❌'}, CSV: {'✅' if row['csv_saved'] else '❌'}, QuestDB: {'✅' if row['questdb_inserted'] else '❌'}")
        
        print(f"\n💾 Analysis saved to: {analysis_path}")
        print(f"📁 Data files saved to: {self.parquet_dir} and {self.csv_dir}")
        
        return analysis_df

def main():
    """Main execution function"""
    try:
        processor = ETFDataProcessor()
        results = processor.process_all_etfs()
        
        if results is not None:
            print(f"\n🎉 ETF data processing completed successfully!")
            print(f"📊 Processed {len(results)} ETFs")
            print(f"📁 Check the data directories for output files")
            print(f"🗄️  QuestDB table 'ohlcv1d' populated with data")
        else:
            print(f"\n❌ ETF data processing failed")
            
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        print("Please check your .env file and ensure FMP_API_KEY is set")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    main()
