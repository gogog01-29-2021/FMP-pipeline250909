#!/usr/bin/env python3
"""
Unified Data Crawler
Downloads OHLCV data from multiple sources and saves to CSV/Parquet
Supports: FMP (stocks/ETFs), Binance (crypto), commodities
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
import io
import time
import logging
from typing import Optional, List, Dict
from dotenv import load_dotenv

# Fix Windows encoding
if sys.platform == 'win32':
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except (ValueError, AttributeError):
        pass  # Already wrapped or not available

# Load environment
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/data_crawler_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)

class UnifiedDataCrawler:
    """Unified crawler for all data sources"""

    def __init__(self, override_years: Optional[int] = None):
        self.fmp_api_key = os.getenv('FMP_API_KEY')
        if not self.fmp_api_key:
            raise ValueError("FMP_API_KEY not found in .env")

        # API endpoints
        self.fmp_base_url = "https://financialmodelingprep.com/api/v3"
        self.binance_base_url = "https://api.binance.com/api/v3"

        # Years override
        self.override_years = override_years

        # Setup directories
        self.setup_directories()

        # Load symbol list
        self.symbols_df = self.load_symbol_list()

        logging.info(f"Unified Data Crawler initialized (override_years={override_years})")

    def setup_directories(self):
        """Create necessary directories"""
        dirs = [
            Path('data/csv/stocks'),
            Path('data/csv/etf'),
            Path('data/csv/crypto'),
            Path('data/csv/commodity'),
            Path('data/parquet/stocks'),
            Path('data/parquet/etf'),
            Path('data/parquet/crypto'),
            Path('data/parquet/commodity'),
            Path('data/metadata'),
            Path('logs')
        ]
        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)

    def load_symbol_list(self) -> pd.DataFrame:
        """Load symbol metadata"""
        symbol_file = Path('data/metadata/symbol_list.csv')
        if symbol_file.exists():
            df = pd.read_csv(symbol_file)
            logging.info(f"✅ Loaded {len(df)} symbols from metadata")
            return df
        else:
            logging.error("❌ symbol_list.csv not found!")
            return pd.DataFrame()

    # ==================== FMP API FUNCTIONS ====================

    def fetch_fmp_historical(self, symbol: str, timeframe: str, years: int) -> Optional[pd.DataFrame]:
        """
        Fetch historical data from FMP API

        Args:
            symbol: Stock/ETF symbol
            timeframe: '1min', '1hour', '1day'
            years: Number of years of history
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=years*365)

        # Determine endpoint based on timeframe
        if timeframe == '1day':
            endpoint = f"{self.fmp_base_url}/historical-price-full/{symbol}"
            params = {
                'from': start_date.strftime('%Y-%m-%d'),
                'to': end_date.strftime('%Y-%m-%d'),
                'apikey': self.fmp_api_key
            }
        elif timeframe == '1hour':
            endpoint = f"{self.fmp_base_url}/historical-chart/1hour/{symbol}"
            params = {
                'from': start_date.strftime('%Y-%m-%d'),
                'to': end_date.strftime('%Y-%m-%d'),
                'apikey': self.fmp_api_key
            }
        elif timeframe == '1min':
            # 1-minute data only available for recent periods
            # We'll fetch in chunks (last N days)
            days_back = min(years * 365, 365)  # Max 1 year for 1-min data
            start_date = end_date - timedelta(days=days_back)
            endpoint = f"{self.fmp_base_url}/historical-chart/1min/{symbol}"
            params = {
                'from': start_date.strftime('%Y-%m-%d'),
                'to': end_date.strftime('%Y-%m-%d'),
                'apikey': self.fmp_api_key
            }
        else:
            logging.error(f"❌ Unknown timeframe: {timeframe}")
            return None

        try:
            response = requests.get(endpoint, params=params, timeout=60)

            if response.status_code == 200:
                data = response.json()

                # Handle different response formats
                if isinstance(data, dict) and 'historical' in data:
                    df = pd.DataFrame(data['historical'])
                elif isinstance(data, list):
                    df = pd.DataFrame(data)
                else:
                    logging.error(f"❌ {symbol}: Unexpected response format")
                    return None

                if df.empty:
                    logging.warning(f"⚠️  {symbol}: No data returned")
                    return None

                # Standardize columns
                df['date'] = pd.to_datetime(df['date'])
                df['symbol'] = symbol
                df = df.sort_values('date')

                # Calculate derived fields
                df['vwap'] = (df['high'] + df['low'] + df['close']) / 3
                df['returns'] = df['close'].pct_change()
                df['log_returns'] = np.log(df['close'] / df['close'].shift(1))

                logging.info(f"✅ {symbol} ({timeframe}): Fetched {len(df)} records")
                return df

            else:
                logging.error(f"❌ {symbol}: HTTP {response.status_code}")
                return None

        except Exception as e:
            logging.error(f"❌ {symbol}: Error - {e}")
            return None

    # ==================== BINANCE API FUNCTIONS ====================

    def fetch_binance_klines(self, symbol: str, interval: str, years: int) -> Optional[pd.DataFrame]:
        """
        Fetch cryptocurrency data from Binance

        Args:
            symbol: Crypto pair (e.g., BTCUSDT)
            interval: '1m', '1h', '1d'
            years: Number of years of history
        """
        # Binance uses different interval notation
        interval_map = {
            '1min': '1m',
            '1hour': '1h',
            '1day': '1d'
        }
        binance_interval = interval_map.get(interval, interval)

        end_time = int(datetime.now().timestamp() * 1000)
        start_time = int((datetime.now() - timedelta(days=years*365)).timestamp() * 1000)

        all_data = []
        current_start = start_time

        # Binance limits to 1000 candles per request
        max_per_request = 1000

        while current_start < end_time:
            try:
                params = {
                    'symbol': symbol,
                    'interval': binance_interval,
                    'startTime': current_start,
                    'endTime': end_time,
                    'limit': max_per_request
                }

                response = requests.get(f"{self.binance_base_url}/klines", params=params, timeout=30)

                if response.status_code == 200:
                    data = response.json()

                    if not data:
                        break

                    all_data.extend(data)

                    # Update start time for next batch
                    current_start = data[-1][0] + 1  # Last candle close time + 1ms

                    logging.info(f"📥 {symbol}: Fetched {len(data)} candles (total: {len(all_data)})")

                    time.sleep(0.5)  # Rate limiting
                else:
                    logging.error(f"❌ {symbol}: HTTP {response.status_code}")
                    break

            except Exception as e:
                logging.error(f"❌ {symbol}: Error - {e}")
                break

        if not all_data:
            return None

        # Convert to DataFrame
        df = pd.DataFrame(all_data, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_volume', 'trades', 'taker_buy_base',
            'taker_buy_quote', 'ignore'
        ])

        # Convert types
        df['date'] = pd.to_datetime(df['open_time'], unit='ms')
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)

        # Add metadata
        df['symbol'] = symbol
        df['vwap'] = (df['high'] + df['low'] + df['close']) / 3
        df['returns'] = df['close'].pct_change()
        df['log_returns'] = np.log(df['close'] / df['close'].shift(1))

        # Select relevant columns
        df = df[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'vwap', 'returns', 'log_returns']]

        logging.info(f"✅ {symbol} ({interval}): Total {len(df)} records")
        return df

    # ==================== SAVE FUNCTIONS ====================

    def save_data(self, df: pd.DataFrame, symbol: str, asset_class: str,
                  timeframe: str, start_date: str, end_date: str) -> bool:
        """Save data to CSV and Parquet"""
        if df is None or df.empty:
            return False

        try:
            # Create filename
            filename = f"{symbol}_{timeframe}_{start_date}_{end_date}"

            csv_path = Path(f'data/csv/{asset_class}/{filename}.csv')
            parquet_path = Path(f'data/parquet/{asset_class}/{filename}.parquet')

            # Save CSV
            df.to_csv(csv_path, index=False)
            logging.info(f"💾 Saved CSV: {csv_path}")

            # Save Parquet
            table = pa.Table.from_pandas(df)
            pq.write_table(table, parquet_path, compression='snappy')
            logging.info(f"💾 Saved Parquet: {parquet_path}")

            return True

        except Exception as e:
            logging.error(f"❌ Save failed: {e}")
            return False

    # ==================== MAIN CRAWLING FUNCTION ====================

    def crawl_symbol(self, row: pd.Series) -> bool:
        """Crawl data for a single symbol"""
        symbol = row['symbol']
        asset_class = row['asset_class']
        timeframes = row['timeframes'].split(',')
        years = self.override_years if self.override_years else int(row['years'])

        logging.info(f"\n{'='*80}")
        logging.info(f"🔍 Crawling {symbol} ({asset_class})")
        logging.info(f"{'='*80}")

        success = True

        for timeframe in timeframes:
            timeframe = timeframe.strip()

            logging.info(f"\n📊 Fetching {symbol} - {timeframe} - {years} years")

            # Fetch data based on asset class
            if asset_class in ['stock', 'etf', 'commodity']:
                df = self.fetch_fmp_historical(symbol, timeframe, years)
            elif asset_class == 'crypto':
                df = self.fetch_binance_klines(symbol, timeframe, years)
            else:
                logging.error(f"❌ Unknown asset class: {asset_class}")
                continue

            if df is not None and not df.empty:
                start_date = df['date'].min().strftime('%Y%m%d')
                end_date = df['date'].max().strftime('%Y%m%d')

                # Add metadata columns
                df['asset_class'] = asset_class
                df['index_membership'] = row.get('index_membership', 'N/A')
                df['timeframe'] = timeframe
                df['data_source'] = 'Binance' if asset_class == 'crypto' else 'FMP'
                df['inserted_at'] = datetime.now()

                # Save to disk
                self.save_data(df, symbol, asset_class, timeframe, start_date, end_date)
            else:
                logging.warning(f"⚠️  No data for {symbol} - {timeframe}")
                success = False

            # Rate limiting
            time.sleep(1)

        return success

    def crawl_all(self, priority: Optional[int] = None, limit: Optional[int] = None):
        """Crawl all symbols (optionally filtered by priority)"""
        logging.info("\n" + "="*80)
        logging.info("🚀 STARTING UNIFIED DATA CRAWL")
        logging.info("="*80)

        # Filter symbols
        symbols = self.symbols_df.copy()

        if priority is not None:
            symbols = symbols[symbols['priority'] <= priority]
            logging.info(f"🎯 Filtering to priority <= {priority}: {len(symbols)} symbols")

        if limit is not None:
            symbols = symbols.head(limit)
            logging.info(f"🎯 Limiting to {limit} symbols")

        logging.info(f"\n📋 Total symbols to crawl: {len(symbols)}")

        # Crawl each symbol
        success_count = 0
        failed_symbols = []

        for idx, row in symbols.iterrows():
            try:
                if self.crawl_symbol(row):
                    success_count += 1
                else:
                    failed_symbols.append(row['symbol'])
            except Exception as e:
                logging.error(f"❌ Failed to crawl {row['symbol']}: {e}")
                failed_symbols.append(row['symbol'])

        # Summary
        logging.info("\n" + "="*80)
        logging.info("✅ CRAWL COMPLETE")
        logging.info("="*80)
        logging.info(f"✅ Successful: {success_count}/{len(symbols)}")
        logging.info(f"❌ Failed: {len(failed_symbols)}")

        if failed_symbols:
            logging.info(f"\n⚠️  Failed symbols: {', '.join(failed_symbols)}")

def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Unified Data Crawler')
    parser.add_argument('--priority', type=int, help='Only crawl symbols with priority <= N')
    parser.add_argument('--limit', type=int, help='Limit number of symbols to crawl')
    parser.add_argument('--symbol', type=str, help='Crawl specific symbol only')
    parser.add_argument('--years', type=int, help='Override years (e.g., 5, 10, 20)')

    args = parser.parse_args()

    crawler = UnifiedDataCrawler(override_years=args.years)

    if args.symbol:
        # Crawl specific symbol
        row = crawler.symbols_df[crawler.symbols_df['symbol'] == args.symbol]
        if not row.empty:
            crawler.crawl_symbol(row.iloc[0])
        else:
            logging.error(f"❌ Symbol not found: {args.symbol}")
    else:
        # Crawl all (with filters)
        crawler.crawl_all(priority=args.priority, limit=args.limit)

if __name__ == "__main__":
    main()
