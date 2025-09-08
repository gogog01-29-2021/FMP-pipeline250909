#!/usr/bin/env python3
"""
ETF Data Processor - Network-Resilient FMP Version
Handles firewall, proxy, and Windows network restrictions
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
import urllib3
import urllib.parse
import ssl
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Disable SSL warnings for corporate environments
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load environment variables
load_dotenv()

class NetworkResilientETFProcessor:
    def __init__(self):
        self.fmp_api_key = os.getenv('FMP_API_KEY')
        if not self.fmp_api_key or self.fmp_api_key == 'your_actual_fmp_api_key_here':
            raise ValueError("Please set your FMP_API_KEY in the .env file")
        
        self.base_url = "https://financialmodelingprep.com/api/v3"
        
        # ETF symbols
        self.etf_symbols = {
            'SPY': 'SPDR S&P 500 ETF Trust',
            'QQQ': 'Invesco QQQ Trust ETF', 
            'IWM': 'iShares Russell 2000 ETF',
            'VXX': 'iPath Series B S&P 500 VIX Short-Term Futures ETN'
        }
        
        # Setup directories
        self.data_dir = Path('./data')
        self.parquet_dir = Path('./data/parquet')
        self.csv_dir = Path('./data/csv')
        
        # Create directories
        self.data_dir.mkdir(exist_ok=True)
        self.parquet_dir.mkdir(exist_ok=True)
        self.csv_dir.mkdir(exist_ok=True)
        
        # Setup network-resilient session
        self.session = self.create_resilient_session()
        
        print(f"✅ Network-Resilient ETF Processor initialized")
        print(f"📁 Data directory: {self.data_dir}")
        print(f"🎯 ETFs to process: {list(self.etf_symbols.keys())}")
        print(f"🔑 API Key: {self.fmp_api_key[:10]}...")
    
    def create_resilient_session(self):
        """Create a requests session that handles network restrictions"""
        session = requests.Session()
        
        # Retry strategy
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        # Mount adapters with retry
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Headers to mimic browser requests
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
        
        # Handle proxy settings if needed
        # session.proxies = {'http': 'http://proxy:port', 'https': 'https://proxy:port'}
        
        return session
    
    def test_connection(self):
        """Test connection to FMP API"""
        print("🔍 Testing connection to FMP API...")
        
        test_url = f"{self.base_url}/profile/AAPL"
        params = {'apikey': self.fmp_api_key}
        
        try:
            # Try multiple methods
            methods = [
                ("Standard GET", lambda: self.session.get(test_url, params=params, timeout=30)),
                ("GET with verify=False", lambda: self.session.get(test_url, params=params, timeout=30, verify=False)),
                ("GET with different headers", lambda: requests.get(test_url, params=params, timeout=30, 
                                                                   headers={'User-Agent': 'Python-ETF-Analyzer/1.0'}))
            ]
            
            for method_name, method in methods:
                try:
                    print(f"🧪 Trying: {method_name}")
                    response = method()
                    
                    print(f"   📡 Status: {response.status_code}")
                    
                    if response.status_code == 200:
                        data = response.json()
                        if data and len(data) > 0:
                            print(f"   ✅ Success! Got data for AAPL: {data[0].get('companyName', 'N/A')}")
                            return True
                    else:
                        print(f"   ❌ HTTP Error: {response.status_code}")
                        print(f"   📝 Response: {response.text[:200]}")
                        
                except Exception as e:
                    print(f"   ❌ {method_name} failed: {e}")
                    continue
            
            print("❌ All connection methods failed")
            return False
            
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            return False
    
    def make_api_request(self, url, params, retries=3):
        """Make API request with multiple fallback methods"""
        
        for attempt in range(retries):
            try:
                print(f"🌐 API Request (attempt {attempt + 1}): {url}")
                print(f"📊 Params: {params}")
                
                # Try different approaches
                approaches = [
                    # Standard approach
                    lambda: self.session.get(url, params=params, timeout=60),
                    # Without SSL verification (corporate networks)
                    lambda: self.session.get(url, params=params, timeout=60, verify=False),
                    # Direct URL approach
                    lambda: requests.get(f"{url}?{urllib.parse.urlencode(params)}", timeout=60),
                    # With manual headers
                    lambda: requests.get(url, params=params, timeout=60, 
                                       headers={'User-Agent': 'ETF-Data-Fetcher/1.0'})
                ]
                
                for i, approach in enumerate(approaches):
                    try:
                        print(f"   🔄 Trying approach {i + 1}...")
                        response = approach()
                        
                        print(f"   📡 Status: {response.status_code}")
                        
                        if response.status_code == 200:
                            data = response.json()
                            print(f"   ✅ Success! Data received")
                            return data
                        else:
                            print(f"   ❌ HTTP {response.status_code}: {response.text[:200]}")
                            continue
                            
                    except requests.exceptions.SSLError as e:
                        print(f"   ⚠️ SSL Error (approach {i + 1}): {e}")
                        continue
                    except requests.exceptions.Timeout as e:
                        print(f"   ⏰ Timeout (approach {i + 1}): {e}")
                        continue
                    except requests.exceptions.ConnectionError as e:
                        print(f"   🔌 Connection Error (approach {i + 1}): {e}")
                        continue
                    except Exception as e:
                        print(f"   ❌ Error (approach {i + 1}): {e}")
                        continue
                
                print(f"   🔄 All approaches failed for attempt {attempt + 1}")
                time.sleep(2 ** attempt)  # Exponential backoff
                
            except Exception as e:
                print(f"❌ Request attempt {attempt + 1} failed: {e}")
                time.sleep(2 ** attempt)
        
        return None
    
    def get_historical_data(self, symbol, years=5):
        """Fetch historical data with network resilience"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=years*365 + 30)
        
        url = f"{self.base_url}/historical-price-full/{symbol}"
        params = {
            'from': start_date.strftime('%Y-%m-%d'),
            'to': end_date.strftime('%Y-%m-%d'),
            'apikey': self.fmp_api_key
        }
        
        print(f"📊 Fetching {years} years of data for {symbol}...")
        
        data = self.make_api_request(url, params)
        
        if data and 'historical' in data and data['historical']:
            df = pd.DataFrame(data['historical'])
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
            
            print(f"✅ Successfully fetched {len(df)} data points for {symbol}")
            print(f"📅 Date range: {df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}")
            return df
        else:
            print(f"❌ No historical data found for {symbol}")
            return None
    
    def get_etf_info(self, symbol):
        """Get ETF information with network resilience"""
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
                
                data = self.make_api_request(endpoint, params)
                
                if data and len(data) > 0:
                    info = data[0]
                    expense_ratio = info.get('expenseRatio') or info.get('managementFee')
                    if expense_ratio:
                        return float(expense_ratio)
                        
            except Exception as e:
                print(f"⚠️ Error getting ETF info from {endpoint}: {e}")
                continue
        
        return None
    
    def calculate_volatility(self, df, window=252):
        """Calculate annualized volatility"""
        if df is None or len(df) < window:
            return None
        
        df_copy = df.copy()
        df_copy['returns'] = df_copy['close'].pct_change()
        df_copy['volatility'] = df_copy['returns'].rolling(window=window).std() * np.sqrt(252)
        
        recent_volatility = df_copy['volatility'].dropna().iloc[-1] if not df_copy['volatility'].dropna().empty else None
        return round(recent_volatility, 4) if recent_volatility else None
    
    def save_to_parquet(self, df, symbol):
        """Save DataFrame to Parquet format"""
        parquet_path = self.parquet_dir / f"{symbol}_5y_ohlcv.parquet"
        
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            df_clean = df.copy()
            df_clean['date'] = pd.to_datetime(df_clean['date'])
            
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
        
        # Calculate metrics
        volatility = self.calculate_volatility(df)
        expense_ratio = self.get_etf_info(symbol)
        
        # Save data
        parquet_saved = self.save_to_parquet(df, symbol)
        csv_saved = self.save_to_csv(df, symbol)
        
        # Rate limiting
        time.sleep(2)  # Be respectful to API
        
        return {
            'symbol': symbol,
            'name': self.etf_symbols[symbol],
            'volatility': volatility,
            'expense_ratio': expense_ratio,
            'data_points': len(df),
            'date_range': f"{df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}",
            'parquet_saved': parquet_saved is not None,
            'csv_saved': csv_saved is not None,
            'latest_price': float(df['close'].iloc[-1]),
            'avg_volume': int(df['volume'].mean())
        }
    
    def process_all_etfs(self):
        """Process all ETFs with network resilience"""
        print(f"\n🚀 Starting Network-Resilient ETF Processing")
        print(f"🌐 Using FMP API with firewall/proxy handling")
        
        # Test connection first
        if not self.test_connection():
            print("❌ Connection test failed. Please check:")
            print("   • Windows Firewall settings")
            print("   • Corporate proxy configuration")
            print("   • VPN connection")
            print("   • Internet connectivity")
            return None
        
        etf_results = []
        
        for symbol in self.etf_symbols.keys():
            result = self.process_single_etf(symbol)
            if result:
                etf_results.append(result)
        
        if not etf_results:
            print("❌ No ETF data was processed successfully")
            return None
        
        # Create analysis
        analysis_df = pd.DataFrame(etf_results)
        
        # Sort by volatility and expense ratio
        analysis_df['volatility_sort'] = analysis_df['volatility'].fillna(0)
        analysis_df['expense_ratio_sort'] = analysis_df['expense_ratio'].fillna(999)
        
        analysis_df = analysis_df.sort_values(
            ['volatility_sort', 'expense_ratio_sort'], 
            ascending=[False, True]
        ).drop(['volatility_sort', 'expense_ratio_sort'], axis=1)
        
        # Save analysis
        analysis_path = self.data_dir / "etf_analysis_results.csv"
        analysis_df.to_csv(analysis_path, index=False)
        
        # Print results
        print(f"\n{'='*80}")
        print("📊 ETF ANALYSIS RESULTS (FMP Data)")
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
        print(f"📁 Files saved to: {self.parquet_dir} and {self.csv_dir}")
        
        return analysis_df

def main():
    """Main execution with network error handling"""
    try:
        processor = NetworkResilientETFProcessor()
        results = processor.process_all_etfs()
        
        if results is not None:
            print(f"\n🎉 ETF PROCESSING COMPLETED!")
            print(f"📊 Successfully processed {len(results)} ETFs")
            print(f"🌐 Data source: Financial Modeling Prep")
            print(f"📁 Check data/ folder for all files")
        else:
            print(f"\n❌ Processing failed - check network configuration")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
