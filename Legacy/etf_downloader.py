import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import time
from dotenv import load_dotenv

load_dotenv()

class ETFDownloader:
    """
    Downloads ETF price data from Financial Modeling Prep API
    """
    
    def __init__(self):
        self.api_key = os.getenv('FMP_API_KEY')
        self.base_url = "https://financialmodelingprep.com/api/v3"
        self.data_dir = os.getenv('DATA_DIR', './data')
        self.parquet_dir = os.getenv('PARQUET_DIR', './data/parquet')
        self.csv_dir = os.getenv('CSV_DIR', './data/csv')
        
        # Create directories if they don't exist
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.parquet_dir, exist_ok=True)
        os.makedirs(self.csv_dir, exist_ok=True)
        
        # ETF symbols with their characteristics
        self.etfs = {
            # S&P 500 ETFs
            'SPY': {'name': 'SPDR S&P 500 ETF', 'expense_ratio': 0.09, 'index': 'S&P 500'},
            'VOO': {'name': 'Vanguard S&P 500 ETF', 'expense_ratio': 0.03, 'index': 'S&P 500'},
            'IVV': {'name': 'iShares Core S&P 500 ETF', 'expense_ratio': 0.03, 'index': 'S&P 500'},
            
            # VIX/Volatility ETFs
            'VIXY': {'name': 'ProShares VIX Short-Term Futures ETF', 'expense_ratio': 0.85, 'index': 'VIX'},
            'VXX': {'name': 'iPath Series B S&P 500 VIX Short-Term Futures ETN', 'expense_ratio': 0.89, 'index': 'VIX'},
            'UVXY': {'name': 'ProShares Ultra VIX Short-Term Futures ETF', 'expense_ratio': 0.95, 'index': 'VIX'},
            
            # NASDAQ 100 ETFs
            'QQQ': {'name': 'Invesco QQQ ETF', 'expense_ratio': 0.20, 'index': 'NASDAQ 100'},
            'QQQM': {'name': 'Invesco NASDAQ 100 ETF', 'expense_ratio': 0.15, 'index': 'NASDAQ 100'},
            'TQQQ': {'name': 'ProShares UltraPro QQQ', 'expense_ratio': 0.95, 'index': 'NASDAQ 100'},
            
            # Russell 2000 ETFs
            'IWM': {'name': 'iShares Russell 2000 ETF', 'expense_ratio': 0.19, 'index': 'Russell 2000'},
            'VTWO': {'name': 'Vanguard Russell 2000 ETF', 'expense_ratio': 0.10, 'index': 'Russell 2000'},
            'TNA': {'name': 'Direxion Daily Small Cap Bull 3X Shares', 'expense_ratio': 0.95, 'index': 'Russell 2000'}
        }
    
    def get_historical_data(self, symbol: str, years: int = 5) -> Optional[pd.DataFrame]:
        """
        Download historical price data for a given ETF symbol
        """
        if not self.api_key:
            print("Error: FMP_API_KEY not found in environment variables")
            return None
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=years * 365)
        
        url = f"{self.base_url}/historical-price-full/{symbol}"
        params = {
            'apikey': self.api_key,
            'from': start_date.strftime('%Y-%m-%d'),
            'to': end_date.strftime('%Y-%m-%d')
        }
        
        try:
            print(f"Downloading data for {symbol}...")
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'historical' not in data or not data['historical']:
                print(f"No data available for {symbol}")
                return None
            
            df = pd.DataFrame(data['historical'])
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
            
            # Add symbol and ETF metadata
            df['symbol'] = symbol
            df['etf_name'] = self.etfs[symbol]['name']
            df['expense_ratio'] = self.etfs[symbol]['expense_ratio']
            df['index_tracked'] = self.etfs[symbol]['index']
            
            # Calculate additional metrics
            df['daily_return'] = df['close'].pct_change()
            df['volatility_20d'] = df['daily_return'].rolling(window=20).std() * np.sqrt(252)
            
            # Reorder columns
            columns_order = ['date', 'symbol', 'etf_name', 'index_tracked', 'expense_ratio',
                           'open', 'high', 'low', 'close', 'adjClose', 'volume',
                           'daily_return', 'volatility_20d', 'unadjustedVolume', 'change', 'changePercent', 'vwap']
            
            # Only include columns that exist in the dataframe
            available_columns = [col for col in columns_order if col in df.columns]
            df = df[available_columns]
            
            return df
            
        except requests.RequestException as e:
            print(f"Error downloading data for {symbol}: {e}")
            return None
        except KeyError as e:
            print(f"Error parsing data for {symbol}: {e}")
            return None
    
    def save_data(self, df: pd.DataFrame, symbol: str, format: str = 'both'):
        """
        Save DataFrame to parquet and/or CSV format
        """
        if df is None or df.empty:
            print(f"No data to save for {symbol}")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_filename = f"{symbol}_5y_daily_{timestamp}"
        
        if format in ['parquet', 'both']:
            parquet_path = os.path.join(self.parquet_dir, f"{base_filename}.parquet")
            df.to_parquet(parquet_path, index=False)
            print(f"Saved {symbol} data to {parquet_path}")
        
        if format in ['csv', 'both']:
            csv_path = os.path.join(self.csv_dir, f"{base_filename}.csv")
            df.to_csv(csv_path, index=False)
            print(f"Saved {symbol} data to {csv_path}")
    
    def download_etf_data(self, symbol: str, years: int = 5, save_format: str = 'both'):
        """
        Download and save data for a single ETF
        """
        if symbol not in self.etfs:
            print(f"ETF symbol {symbol} not found in the supported list")
            return None
        
        df = self.get_historical_data(symbol, years)
        if df is not None:
            self.save_data(df, symbol, save_format)
        
        # Rate limiting to avoid hitting API limits
        time.sleep(1)
        return df
    
    def download_all_etfs(self, years: int = 5, save_format: str = 'both'):
        """
        Download data for all ETFs in the list
        """
        print(f"Starting download of {len(self.etfs)} ETFs...")
        print("="*50)
        
        all_data = []
        
        for symbol in self.etfs.keys():
            df = self.download_etf_data(symbol, years, save_format)
            if df is not None:
                all_data.append(df)
        
        # Combine all data into a single DataFrame
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # Save combined data
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            if save_format in ['parquet', 'both']:
                combined_parquet_path = os.path.join(self.parquet_dir, f"all_etfs_5y_daily_{timestamp}.parquet")
                combined_df.to_parquet(combined_parquet_path, index=False)
                print(f"Saved combined data to {combined_parquet_path}")
            
            if save_format in ['csv', 'both']:
                combined_csv_path = os.path.join(self.csv_dir, f"all_etfs_5y_daily_{timestamp}.csv")
                combined_df.to_csv(combined_csv_path, index=False)
                print(f"Saved combined data to {combined_csv_path}")
            
            print("="*50)
            print(f"Download completed! Total records: {len(combined_df)}")
            return combined_df
        
        return None
    
    def get_etf_summary(self):
        """
        Display summary of available ETFs
        """
        print("Available ETFs:")
        print("="*80)
        for symbol, info in self.etfs.items():
            print(f"{symbol:6} | {info['name']:40} | {info['index']:15} | Fee: {info['expense_ratio']:.2f}%")
        print("="*80)

if __name__ == "__main__":
    downloader = ETFDownloader()
    downloader.get_etf_summary()
    downloader.download_all_etfs()
