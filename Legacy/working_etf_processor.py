"""
Working ETF Data Processor - Generates Sample Data for Complete Pipeline Demo
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from pathlib import Path
import time

print("=" * 60)
print("ETF DATA PROCESSOR - WORKING VERSION")
print("=" * 60)

# ETF symbols and data
etf_symbols = {
    'SPY': 'SPDR S&P 500 ETF Trust',
    'VXX': 'iPath Series B S&P 500 VIX Short-Term Futures ETN',
    'QQQ': 'Invesco QQQ Trust ETF',
    'IWM': 'iShares Russell 2000 ETF'
}

# Setup directories
data_dir = Path('./data')
parquet_dir = Path('./data/parquet')
csv_dir = Path('./data/csv')

data_dir.mkdir(exist_ok=True)
parquet_dir.mkdir(exist_ok=True)
csv_dir.mkdir(exist_ok=True)

print(f"Data directory: {data_dir}")
print(f"Processing ETFs: {list(etf_symbols.keys())}")

results = {}

for symbol in etf_symbols.keys():
    print(f"\n{'-'*40}")
    print(f"Processing {symbol} ({etf_symbols[symbol]})")
    print(f"{'-'*40}")
    
    try:
        # Generate 5 years of realistic sample data
        print(f"Generating 5 years of data for {symbol}...")
        
        # Different base prices and characteristics
        if symbol == 'SPY':
            base_price = 400.0
            volatility = 0.15
        elif symbol == 'QQQ':
            base_price = 350.0
            volatility = 0.20
        elif symbol == 'IWM':
            base_price = 200.0
            volatility = 0.25
        elif symbol == 'VXX':
            base_price = 25.0
            volatility = 0.80
        
        # Generate business days for 5 years
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=5 * 365)
        dates = pd.date_range(start=start_date, end=end_date, freq='B')
        
        # Generate realistic price series
        np.random.seed(42 + hash(symbol) % 100)
        returns = np.random.normal(0, volatility/np.sqrt(252), len(dates))
        price_series = base_price * np.exp(np.cumsum(returns))
        
        # Create OHLCV data
        data = []
        for i, date in enumerate(dates):
            price = price_series[i]
            daily_vol = volatility * price * 0.02
            
            open_price = price + np.random.uniform(-daily_vol/2, daily_vol/2)
            close_price = price + np.random.uniform(-daily_vol/2, daily_vol/2)
            high_price = max(open_price, close_price) + np.random.uniform(0, daily_vol/2)
            low_price = min(open_price, close_price) - np.random.uniform(0, daily_vol/2)
            
            volume = int(np.random.uniform(10_000_000, 100_000_000))
            
            data.append({
                'date': date.strftime('%Y-%m-%d'),
                'open': round(open_price, 2),
                'high': round(high_price, 2),
                'low': round(low_price, 2),
                'close': round(close_price, 2),
                'volume': volume,
                'symbol': symbol
            })
        
        df = pd.DataFrame(data)
        print(f"Generated {len(df)} records")
        
        # Calculate metrics
        print(f"Calculating metrics...")
        df['daily_return'] = df['close'].pct_change()
        volatility_calc = df['daily_return'].std() * np.sqrt(252)
        total_return = (df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0]
        
        print(f"  Volatility: {volatility_calc:.4f}")
        print(f"  Total Return: {total_return:.4f}")
        print(f"  Current Price: ${df['close'].iloc[-1]:.2f}")
        
        # Save to CSV
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_file = csv_dir / f"{symbol}_historical_data_{timestamp}.csv"
        df.to_csv(csv_file, index=False)
        print(f"✓ Saved CSV: {csv_file}")
        
        # Save to Parquet
        parquet_file = parquet_dir / f"{symbol}_historical_data_{timestamp}.parquet"
        df.to_parquet(parquet_file, index=False)
        print(f"✓ Saved Parquet: {parquet_file}")
        
        # Try to load to QuestDB
        try:
            print(f"Loading to QuestDB...")
            from questdb.ingress import Sender, TimestampNanos
            
            with Sender('localhost', 9009) as sender:
                for _, row in df.iterrows():
                    sender.row(
                        'etf_prices',
                        symbols={
                            'symbol': symbol,
                            'name': etf_symbols[symbol]
                        },
                        columns={
                            'open': row['open'],
                            'high': row['high'],
                            'low': row['low'],
                            'close': row['close'],
                            'volume': row['volume']
                        },
                        at=TimestampNanos.from_datetime(pd.to_datetime(row['date']))
                    )
                sender.flush()
            print(f"✓ Loaded {len(df)} records to QuestDB")
            db_success = True
        except Exception as e:
            print(f"⚠ QuestDB loading failed: {e}")
            db_success = False
        
        results[symbol] = {
            'records': len(df),
            'volatility': volatility_calc,
            'total_return': total_return,
            'current_price': df['close'].iloc[-1],
            'csv_saved': True,
            'parquet_saved': True,
            'db_loaded': db_success,
            'status': 'SUCCESS'
        }
        
        print(f"✓ {symbol} processing completed successfully")
        
    except Exception as e:
        print(f"✗ {symbol} failed: {e}")
        results[symbol] = {'status': 'FAILED', 'error': str(e)}

# Generate summary
print(f"\n{'='*60}")
print("PROCESSING SUMMARY")
print(f"{'='*60}")

summary_data = []
for symbol, result in results.items():
    if result['status'] == 'SUCCESS':
        summary_data.append({
            'symbol': symbol,
            'name': etf_symbols[symbol],
            'records_processed': result['records'],
            'volatility': result['volatility'],
            'total_return': result['total_return'],
            'current_price': result['current_price'],
            'csv_saved': result['csv_saved'],
            'parquet_saved': result['parquet_saved'],
            'db_loaded': result['db_loaded']
        })
        print(f"✓ {symbol}: {result['records']} records, Vol: {result['volatility']:.4f}")
    else:
        print(f"✗ {symbol}: {result.get('error', 'Unknown error')}")

# Sort by volatility and create ranking
if summary_data:
    summary_df = pd.DataFrame(summary_data)
    summary_df = summary_df.sort_values('volatility', ascending=False)
    
    print(f"\nETF RANKING BY VOLATILITY (Higher = More Volatile):")
    print("-" * 80)
    print(f"{'Rank':<6} {'Symbol':<6} {'Volatility':<12} {'Return':<10} {'Price':<10} {'Name'}")
    print("-" * 80)
    
    for i, (_, row) in enumerate(summary_df.iterrows()):
        print(f"{i+1:<6} {row['symbol']:<6} {row['volatility']:<12.4f} {row['total_return']:<10.4f} ${row['current_price']:<9.2f} {row['name']}")
    
    # Save summary
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    summary_file = data_dir / f'etf_analysis_summary_{timestamp}.csv'
    summary_df.to_csv(summary_file, index=False)
    print(f"\nSummary saved: {summary_file}")

successful = sum(1 for r in results.values() if r['status'] == 'SUCCESS')
print(f"\nProcessing completed: {successful}/{len(results)} ETFs successful")

print(f"\n{'='*60}")
print("DATA PIPELINE COMPLETED!")
print(f"Check files in:")
print(f"  CSV: {csv_dir}")
print(f"  Parquet: {parquet_dir}")
print(f"  QuestDB: http://localhost:9000")
print(f"{'='*60}")
