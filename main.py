import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from premium_unified_processor import PremiumDataProcessor
import os
from datetime import datetime

def main():
    """
    Main execution script for ETF data pipeline
    """
    print("ETF Data Pipeline - FMP to QuestDB")
    print("="*50)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check if .env file exists
    if not os.path.exists('.env'):
        print("⚠️  Warning: .env file not found. Please copy .env.example to .env and configure your API keys.")
        return
    
    # Step 1: Download ETF data from FMP
    print("\n📥 Step 1: Downloading ETF data from Financial Modeling Prep...")
    downloader = ETFDownloader()
    
    # Show available ETFs
    downloader.get_etf_summary()
    
    # Download all ETF data (5 years, daily)
    combined_data = downloader.download_all_etfs(years=5, save_format='both')
    
    if combined_data is None:
        print("❌ Failed to download ETF data. Please check your FMP API key.")
        return
    
    # Step 2: Setup QuestDB and create OHLCV table
    print("\n📊 Step 2: Setting up QuestDB and creating OHLCV1d table...")
    questdb_client = QuestDBClient()
    
    if not questdb_client.create_ohlcv_table():
        print("❌ Failed to create QuestDB table. Please ensure QuestDB is running.")
        return
    
    # Step 3: Load data into QuestDB
    print("\n⬆️  Step 3: Loading data into QuestDB...")
    if not questdb_client.load_all_parquet_files():
        print("❌ Failed to load data into QuestDB.")
        return
    
    # Step 4: Verify data and show summary
    print("\n✅ Step 4: Verification and Summary...")
    questdb_client.get_table_info()
    
    # Sample query for each index type
    indices = ['S&P 500', 'VIX', 'NASDAQ 100', 'Russell 2000']
    for index in indices:
        print(f"\n📈 Sample data for {index}:")
        sample_data = questdb_client.query_ohlcv_data(limit=5)
        if sample_data is not None and not sample_data.empty:
            # Filter by index if possible
            index_data = sample_data[sample_data['index_tracked'] == index] if 'index_tracked' in sample_data.columns else sample_data.head(3)
            if not index_data.empty:
                print(index_data[['timestamp', 'symbol', 'close', 'volume', 'daily_return']].head(3).to_string(index=False))
    
    print(f"\n🎉 Pipeline completed successfully at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n📁 Files saved in:")
    print(f"   - Parquet files: ./data/parquet/")
    print(f"   - CSV files: ./data/csv/")
    print(f"   - QuestDB table: ohlcv1d")

if __name__ == "__main__":
    main()
