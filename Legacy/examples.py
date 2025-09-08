"""
Example usage of the ETF Data Pipeline

This script demonstrates how to use the ETF downloader and QuestDB integration
"""

import os
import sys
from datetime import datetime, timedelta

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from etf_downloader import ETFDownloader
from questdb_client import QuestDBClient
from etf_analyzer import ETFAnalyzer

def example_download_specific_etfs():
    """Example: Download data for specific ETFs"""
    print("Example 1: Downloading specific ETFs")
    print("-" * 40)
    
    downloader = ETFDownloader()
    
    # Download specific ETFs
    etfs_to_download = ['SPY', 'QQQ', 'IWM', 'VXX']
    
    for etf in etfs_to_download:
        print(f"Downloading {etf}...")
        data = downloader.download_etf_data(etf, years=2, save_format='both')
        if data is not None:
            print(f"✅ {etf}: {len(data)} records downloaded")
        else:
            print(f"❌ {etf}: Download failed")
    
    print("Example 1 completed!\n")

def example_questdb_operations():
    """Example: QuestDB operations"""
    print("Example 2: QuestDB Operations")
    print("-" * 40)
    
    client = QuestDBClient()
    
    # Create table
    print("Creating QuestDB table...")
    if client.create_ohlcv_table():
        print("✅ Table created successfully")
    else:
        print("❌ Table creation failed")
        return
    
    # Load data
    print("Loading data to QuestDB...")
    if client.load_all_parquet_files():
        print("✅ Data loaded successfully")
    else:
        print("❌ Data loading failed")
        return
    
    # Query sample data
    print("Querying sample data...")
    sample_data = client.query_ohlcv_data(symbol='SPY', limit=5)
    if sample_data is not None and not sample_data.empty:
        print("Sample SPY data:")
        print(sample_data[['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']].to_string())
    
    # Get table info
    client.get_table_info()
    
    print("Example 2 completed!\n")

def example_analysis():
    """Example: ETF Analysis"""
    print("Example 3: ETF Analysis")
    print("-" * 40)
    
    analyzer = ETFAnalyzer()
    
    # Load and analyze data
    df = analyzer.load_combined_data()
    if df is not None:
        print("Generating analysis report...")
        metrics = analyzer.generate_report(df)
        
        if metrics is not None:
            print(f"✅ Analysis completed for {len(metrics)} ETFs")
        else:
            print("❌ Analysis failed")
    else:
        print("❌ No data found for analysis. Please run the main pipeline first.")
    
    print("Example 3 completed!\n")

def example_filter_by_criteria():
    """Example: Filter ETFs by specific criteria"""
    print("Example 4: Filter ETFs by Criteria")
    print("-" * 40)
    
    downloader = ETFDownloader()
    
    # Filter ETFs by low expense ratio
    low_fee_etfs = {symbol: info for symbol, info in downloader.etfs.items() 
                    if info['expense_ratio'] <= 0.20}
    
    print("ETFs with expense ratio <= 0.20%:")
    for symbol, info in low_fee_etfs.items():
        print(f"  {symbol}: {info['name']} - {info['expense_ratio']:.2f}% ({info['index']})")
    
    # Filter by index
    sp500_etfs = {symbol: info for symbol, info in downloader.etfs.items() 
                  if info['index'] == 'S&P 500'}
    
    print(f"\nS&P 500 ETFs ({len(sp500_etfs)} found):")
    for symbol, info in sp500_etfs.items():
        print(f"  {symbol}: {info['expense_ratio']:.2f}% fee")
    
    print("Example 4 completed!\n")

def example_custom_date_range():
    """Example: Download data for custom date range"""
    print("Example 5: Custom Date Range Download")
    print("-" * 40)
    
    # Note: This is a conceptual example as FMP API uses years parameter
    # For exact date ranges, you would need to modify the downloader class
    
    downloader = ETFDownloader()
    
    print("Downloading recent data (1 year)...")
    recent_data = downloader.download_etf_data('SPY', years=1, save_format='parquet')
    
    if recent_data is not None:
        date_range = f"{recent_data['date'].min().date()} to {recent_data['date'].max().date()}"
        print(f"✅ Downloaded SPY data: {date_range} ({len(recent_data)} records)")
        
        # Show recent price action
        latest_prices = recent_data.tail(5)[['date', 'close', 'volume', 'daily_return']]
        print("Latest 5 days:")
        print(latest_prices.to_string(index=False))
    else:
        print("❌ Download failed")
    
    print("Example 5 completed!\n")

def main():
    """Run all examples"""
    print("ETF Data Pipeline - Usage Examples")
    print("=" * 50)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Check if .env file exists
    if not os.path.exists('.env'):
        print("⚠️  Warning: .env file not found!")
        print("Please copy .env.example to .env and configure your API keys.")
        print("Some examples may not work without proper configuration.")
        print()
    
    try:
        # Run examples
        example_download_specific_etfs()
        example_questdb_operations()
        example_analysis()
        example_filter_by_criteria()
        example_custom_date_range()
        
        print("🎉 All examples completed successfully!")
        
    except KeyboardInterrupt:
        print("\n⏹️  Examples interrupted by user")
    except Exception as e:
        print(f"\n❌ Error running examples: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
