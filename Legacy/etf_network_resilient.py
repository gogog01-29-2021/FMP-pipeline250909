"""
ETF Data Processor with Maximum Network Resilience
Handles various network configurations, proxies, firewalls, and SSL issues
"""

import pandas as pd
import requests
import urllib3
import urllib.request
import urllib.parse
import ssl
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
import socket
import subprocess
import sys
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class NetworkResistantHTTPAdapter(HTTPAdapter):
    """Custom HTTP adapter with aggressive network resilience"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
    def init_poolmanager(self, *args, **kwargs):
        kwargs.update({
            'ssl_version': ssl.PROTOCOL_TLS,
            'ssl_context': ssl.create_default_context(),
            'cert_reqs': ssl.CERT_NONE,
            'check_hostname': False,
        })
        return super().init_poolmanager(*args, **kwargs)

class ETFNetworkResilientProcessor:
    def __init__(self):
        self.fmp_api_key = os.getenv('FMP_API_KEY')
        if not self.fmp_api_key or self.fmp_api_key == 'your_fmp_api_key_here':
            raise ValueError("Please set your FMP_API_KEY in the .env file")
        
        self.base_url = "https://financialmodelingprep.com/api/v3"
        
        # ETF symbols
        self.etf_symbols = {
            'SPY': 'SPDR S&P 500 ETF Trust',
            'VXX': 'iPath Series B S&P 500 VIX Short-Term Futures ETN',
            'QQQ': 'Invesco QQQ Trust ETF',
            'IWM': 'iShares Russell 2000 ETF'
        }
        
        # Setup directories
        self.data_dir = Path(os.getenv('DATA_DIR', './data'))
        self.parquet_dir = Path(os.getenv('PARQUET_DIR', './data/parquet'))
        self.csv_dir = Path(os.getenv('CSV_DIR', './data/csv'))
        
        # Create directories
        self.data_dir.mkdir(exist_ok=True)
        self.parquet_dir.mkdir(exist_ok=True)
        self.csv_dir.mkdir(exist_ok=True)
        
        # Initialize multiple session types
        self.sessions = self._create_multiple_sessions()
        
        print(f"Initialized Network-Resilient ETF Data Processor")
        print(f"Data directory: {self.data_dir}")
        print(f"Processing ETFs: {list(self.etf_symbols.keys())}")
        
    def _detect_proxy_settings(self):
        """Auto-detect system proxy settings"""
        proxies = {}
        
        # Check environment variables
        for proto in ['http', 'https']:
            proxy_env = os.environ.get(f'{proto}_proxy') or os.environ.get(f'{proto.upper()}_PROXY')
            if proxy_env:
                proxies[proto] = proxy_env
                logger.info(f"Found {proto} proxy in environment: {proxy_env}")
        
        # Check Windows proxy settings using registry (if on Windows)
        if os.name == 'nt':
            try:
                import winreg
                with winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                                    r'Software\Microsoft\Windows\CurrentVersion\Internet Settings') as key:
                    proxy_enable, _ = winreg.QueryValueEx(key, 'ProxyEnable')
                    if proxy_enable:
                        proxy_server, _ = winreg.QueryValueEx(key, 'ProxyServer')
                        proxies['http'] = f'http://{proxy_server}'
                        proxies['https'] = f'http://{proxy_server}'
                        logger.info(f"Found Windows proxy: {proxy_server}")
            except Exception as e:
                logger.debug(f"Could not read Windows proxy settings: {e}")
        
        return proxies if proxies else None
    
    def _create_multiple_sessions(self):
        """Create multiple session configurations for different network scenarios"""
        sessions = {}
        
        # Detect proxy settings
        system_proxies = self._detect_proxy_settings()
        
        # Session 1: Standard with retries
        session1 = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        session1.mount("http://", HTTPAdapter(max_retries=retry_strategy))
        session1.mount("https://", HTTPAdapter(max_retries=retry_strategy))
        session1.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        sessions['standard'] = session1
        
        # Session 2: With detected proxies
        if system_proxies:
            session2 = requests.Session()
            session2.proxies.update(system_proxies)
            session2.mount("http://", HTTPAdapter(max_retries=retry_strategy))
            session2.mount("https://", HTTPAdapter(max_retries=retry_strategy))
            session2.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            sessions['proxy'] = session2
        
        # Session 3: SSL disabled for corporate firewalls
        session3 = requests.Session()
        session3.verify = False
        session3.mount("http://", NetworkResistantHTTPAdapter(max_retries=retry_strategy))
        session3.mount("https://", NetworkResistantHTTPAdapter(max_retries=retry_strategy))
        session3.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        sessions['no_ssl'] = session3
        
        # Session 4: With system trust store
        session4 = requests.Session()
        session4.mount("http://", HTTPAdapter(max_retries=retry_strategy))
        session4.mount("https://", HTTPAdapter(max_retries=retry_strategy))
        session4.headers.update({
            'User-Agent': 'ETF-Processor/1.0',
            'Accept': 'application/json',
            'Connection': 'close'
        })
        sessions['trust_store'] = session4
        
        logger.info(f"Created {len(sessions)} session configurations")
        return sessions
    
    def _make_resilient_request(self, url, params=None):
        """Make HTTP request using multiple strategies"""
        params = params or {}
        
        # Add API key to params
        params['apikey'] = self.fmp_api_key
        
        errors = []
        
        # Try each session configuration with aggressive timeout and error handling
        for session_name, session in self.sessions.items():
            try:
                print(f"Trying {session_name} session for {url}")
                logger.info(f"Trying {session_name} session for {url}")
                
                response = session.get(url, params=params, timeout=10)
                print(f"{session_name}: Got response {response.status_code}")
                
                if response.status_code == 200:
                    print(f"SUCCESS with {session_name} session!")
                    logger.info(f"Success with {session_name} session")
                    return response.json()
                else:
                    error_msg = f"{session_name}: HTTP {response.status_code} - {response.text[:100]}"
                    errors.append(error_msg)
                    print(f"FAILED: {error_msg}")
                    logger.warning(error_msg)
                    
            except Exception as e:
                error_msg = f"{session_name}: {str(e)}"
                errors.append(error_msg)
                print(f"ERROR: {error_msg}")
                logger.warning(error_msg)
                
        # Try urllib as final fallback
        try:
            print("Trying urllib fallback method...")
            logger.info("Trying urllib fallback")
            
            # Create SSL context that ignores certificate errors
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            # Build URL with params
            url_with_params = f"{url}?{urllib.parse.urlencode(params)}"
            print(f"Full URL: {url_with_params[:100]}...")
            
            request = urllib.request.Request(
                url_with_params,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
            )
            
            with urllib.request.urlopen(request, timeout=15, context=ssl_context) as response:
                data = response.read().decode('utf-8')
                print("SUCCESS with urllib fallback!")
                logger.info("Success with urllib fallback")
                return json.loads(data)
                
        except Exception as e:
            error_msg = f"urllib fallback: {str(e)}"
            errors.append(error_msg)
            print(f"FAILED: {error_msg}")
            logger.error(error_msg)
        
        # If all methods fail, raise exception with all errors
        raise Exception(f"All network methods failed. Errors: {'; '.join(errors)}")
    
    def test_network_connectivity(self):
        """Test network connectivity and API access"""
        print("Testing network connectivity...")
        
        # Test basic internet connectivity
        test_sites = [
            ('Google DNS', '8.8.8.8', 53),
            ('Cloudflare DNS', '1.1.1.1', 53),
            ('FMP API', 'financialmodelingprep.com', 443)
        ]
        
        for name, host, port in test_sites:
            try:
                socket.create_connection((host, port), timeout=5).close()
                print(f"✓ {name} connectivity: OK")
            except Exception as e:
                print(f"✗ {name} connectivity: FAILED - {e}")
        
        # Test FMP API with simple endpoint
        try:
            test_url = f"{self.base_url}/symbol/SPY"
            result = self._make_resilient_request(test_url)
            if isinstance(result, list) and len(result) > 0:
                print("✓ FMP API connectivity: OK")
                return True
            else:
                print("✗ FMP API returned unexpected data")
                return False
        except Exception as e:
            print(f"✗ FMP API connectivity: FAILED - {e}")
            return False
    
    def get_etf_profile(self, symbol):
        """Get ETF profile including expense ratio"""
        try:
            # Try multiple profile endpoints
            endpoints = [
                f"{self.base_url}/etf-holder/{symbol}",
                f"{self.base_url}/profile/{symbol}",
                f"{self.base_url}/company/profile/{symbol}"
            ]
            
            for endpoint in endpoints:
                try:
                    data = self._make_resilient_request(endpoint)
                    if data:
                        logger.info(f"Got profile data for {symbol} from {endpoint}")
                        return data
                except Exception as e:
                    logger.warning(f"Profile endpoint {endpoint} failed: {e}")
                    continue
            
            logger.warning(f"All profile endpoints failed for {symbol}")
            return None
            
        except Exception as e:
            logger.error(f"Error getting ETF profile for {symbol}: {e}")
            return None
    
    def get_historical_data(self, symbol, years=5):
        """Get historical price data for specified years"""
        try:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=years * 365)
            
            url = f"{self.base_url}/historical-price-full/{symbol}"
            params = {
                'from': start_date.strftime('%Y-%m-%d'),
                'to': end_date.strftime('%Y-%m-%d')
            }
            
            logger.info(f"Fetching historical data for {symbol} from {start_date} to {end_date}")
            
            data = self._make_resilient_request(url, params)
            
            if 'historical' in data and len(data['historical']) > 0:
                logger.info(f"Got {len(data['historical'])} records for {symbol}")
                return data['historical']
            else:
                logger.warning(f"No historical data found for {symbol}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching historical data for {symbol}: {e}")
            return None
    
    def calculate_metrics(self, historical_data):
        """Calculate volatility and other metrics"""
        if not historical_data or len(historical_data) < 30:
            return None
        
        try:
            df = pd.DataFrame(historical_data)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            
            # Calculate daily returns
            df['daily_return'] = df['close'].pct_change()
            
            # Calculate metrics
            metrics = {
                'volatility': df['daily_return'].std() * np.sqrt(252),  # Annualized volatility
                'avg_volume': df['volume'].mean(),
                'price_range': df['high'].max() - df['low'].min(),
                'total_return': (df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0],
                'sharpe_ratio': (df['daily_return'].mean() * 252) / (df['daily_return'].std() * np.sqrt(252)) if df['daily_return'].std() > 0 else 0
            }
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error calculating metrics: {e}")
            return None
    
    def save_to_files(self, symbol, data, file_prefix="historical_data"):
        """Save data to both Parquet and CSV formats"""
        try:
            if not data:
                logger.warning(f"No data to save for {symbol}")
                return False
            
            df = pd.DataFrame(data)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            
            # Generate filenames with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            base_filename = f"{symbol}_{file_prefix}_{timestamp}"
            
            # Save as Parquet
            parquet_file = self.parquet_dir / f"{base_filename}.parquet"
            df.to_parquet(parquet_file, index=False)
            logger.info(f"Saved Parquet: {parquet_file}")
            
            # Save as CSV
            csv_file = self.csv_dir / f"{base_filename}.csv"
            df.to_csv(csv_file, index=False)
            logger.info(f"Saved CSV: {csv_file}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving data for {symbol}: {e}")
            return False
    
    def load_to_questdb(self, symbol, data):
        """Load data into QuestDB"""
        try:
            if not data:
                logger.warning(f"No data to load for {symbol}")
                return False
            
            df = pd.DataFrame(data)
            df['date'] = pd.to_datetime(df['date'])
            
            # Connect to QuestDB
            with Sender('localhost', 9009) as sender:
                for _, row in df.iterrows():
                    sender.row(
                        'etf_prices',
                        symbols={
                            'symbol': symbol,
                            'name': self.etf_symbols.get(symbol, symbol)
                        },
                        columns={
                            'open': row['open'],
                            'high': row['high'],
                            'low': row['low'],
                            'close': row['close'],
                            'volume': row['volume'],
                            'adj_close': row.get('adjClose', row['close']),
                            'change': row.get('change', 0),
                            'change_percent': row.get('changePercent', 0)
                        },
                        at=TimestampNanos.from_datetime(row['date'])
                    )
                sender.flush()
            
            logger.info(f"Loaded {len(df)} records to QuestDB for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading data to QuestDB for {symbol}: {e}")
            return False
    
    def process_all_etfs(self):
        """Process all ETFs with comprehensive error handling"""
        print(f"\n{'='*60}")
        print("Starting Network-Resilient ETF Data Processing")
        print(f"{'='*60}")
        
        # Test connectivity first
        if not self.test_network_connectivity():
            print("\n⚠️  Network connectivity issues detected. Attempting to continue anyway...")
        
        results = {}
        
        for symbol in self.etf_symbols.keys():
            print(f"\n{'-'*40}")
            print(f"Processing {symbol} ({self.etf_symbols[symbol]})")
            print(f"{'-'*40}")
            
            try:
                # Get profile data
                print(f"Getting profile data for {symbol}...")
                profile = self.get_etf_profile(symbol)
                
                # Get historical data
                print(f"Getting historical data for {symbol}...")
                historical_data = self.get_historical_data(symbol)
                
                if historical_data:
                    # Calculate metrics
                    print(f"Calculating metrics for {symbol}...")
                    metrics = self.calculate_metrics(historical_data)
                    
                    # Save to files
                    print(f"Saving data files for {symbol}...")
                    file_success = self.save_to_files(symbol, historical_data)
                    
                    # Load to QuestDB
                    print(f"Loading data to QuestDB for {symbol}...")
                    db_success = self.load_to_questdb(symbol, historical_data)
                    
                    results[symbol] = {
                        'profile': profile,
                        'records_count': len(historical_data),
                        'metrics': metrics,
                        'file_saved': file_success,
                        'db_loaded': db_success,
                        'status': 'SUCCESS'
                    }
                    
                    print(f"✓ {symbol}: {len(historical_data)} records processed successfully")
                    if metrics:
                        print(f"  Volatility: {metrics['volatility']:.4f}")
                        print(f"  Total Return: {metrics['total_return']:.4f}")
                        print(f"  Files Saved: {file_success}")
                        print(f"  DB Loaded: {db_success}")
                
                else:
                    results[symbol] = {
                        'status': 'FAILED',
                        'error': 'No historical data available'
                    }
                    print(f"✗ {symbol}: No data available")
                
                # Small delay between requests
                time.sleep(1)
                
            except Exception as e:
                results[symbol] = {
                    'status': 'ERROR',
                    'error': str(e)
                }
                print(f"✗ {symbol}: Error - {e}")
        
        # Generate summary report
        self.generate_summary_report(results)
        
        return results
    
    def generate_summary_report(self, results):
        """Generate and save summary report"""
        print(f"\n{'='*60}")
        print("PROCESSING SUMMARY")
        print(f"{'='*60}")
        
        summary_data = []
        
        for symbol, result in results.items():
            if result['status'] == 'SUCCESS':
                profile = result.get('profile', {})
                metrics = result.get('metrics', {})
                
                summary_data.append({
                    'symbol': symbol,
                    'name': self.etf_symbols[symbol],
                    'records_processed': result['records_count'],
                    'volatility': metrics.get('volatility', 0) if metrics else 0,
                    'total_return': metrics.get('total_return', 0) if metrics else 0,
                    'sharpe_ratio': metrics.get('sharpe_ratio', 0) if metrics else 0,
                    'expense_ratio': 'N/A',  # Would need to parse from profile
                    'files_saved': result['file_saved'],
                    'db_loaded': result['db_loaded'],
                    'status': result['status']
                })
                
                print(f"✓ {symbol}: {result['records_count']} records")
            else:
                print(f"✗ {symbol}: {result.get('error', 'Unknown error')}")
                summary_data.append({
                    'symbol': symbol,
                    'name': self.etf_symbols[symbol],
                    'status': result['status'],
                    'error': result.get('error', 'Unknown error')
                })
        
        # Save summary report
        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            
            # Sort by volatility (descending) for successful records
            success_df = summary_df[summary_df['status'] == 'SUCCESS'].copy()
            if not success_df.empty and 'volatility' in success_df.columns:
                success_df = success_df.sort_values('volatility', ascending=False)
                
                print(f"\nETF RANKING BY VOLATILITY (Higher = More Volatile):")
                print("-" * 55)
                for _, row in success_df.iterrows():
                    print(f"{row['symbol']:4} | {row['volatility']:8.4f} | {row['total_return']:8.4f} | {row['name']}")
            
            # Save to files
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            summary_file = self.data_dir / f'etf_analysis_summary_{timestamp}.csv'
            summary_df.to_csv(summary_file, index=False)
            print(f"\nSummary report saved: {summary_file}")
        
        successful_count = sum(1 for r in results.values() if r['status'] == 'SUCCESS')
        print(f"\nProcessing completed: {successful_count}/{len(results)} ETFs successful")

def main():
    """Main execution function"""
    try:
        processor = ETFNetworkResilientProcessor()
        results = processor.process_all_etfs()
        
        print(f"\n{'='*60}")
        print("ETF DATA PROCESSING COMPLETED")
        print(f"{'='*60}")
        
        return results
        
    except Exception as e:
        print(f"Fatal error in ETF processing: {e}")
        return None

if __name__ == "__main__":
    main()
