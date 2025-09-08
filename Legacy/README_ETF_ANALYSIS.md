# ETF Data Analysis Project

This project fetches 5 years of historical price data for major ETFs from Financial Modeling Prep (FMP), analyzes them by volatility and expense ratios, and stores the data in multiple formats (Parquet, CSV, and QuestDB).

## 🎯 Target ETFs

The project analyzes these major index ETFs:

- **SPY**: SPDR S&P 500 ETF Trust (S&P 500 Index)
- **QQQ**: Invesco QQQ Trust ETF (Nasdaq 100 Index)  
- **IWM**: iShares Russell 2000 ETF (Russell 2000 Index)
- **VXX**: iPath Series B S&P 500 VIX Short-Term Futures ETN (VIX Volatility Index)

## 📊 Analysis Criteria

ETFs are ranked by:
1. **Higher Volatility** (descending) - for better trading opportunities
2. **Lower Expense Ratio** (ascending) - for cost efficiency

## 🚀 Quick Start

### Option 1: Windows Batch File (Easiest)
```cmd
run_etf_analysis.bat
```

### Option 2: Python Script
```bash
python run_analysis.py
```

### Option 3: Manual Steps
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env and add your FMP_API_KEY

# 3. Start QuestDB
# Download from https://questdb.io/get-questdb/
# Run: questdb.exe

# 4. Setup QuestDB table
python setup_questdb.py

# 5. Run ETF analysis
python etf_fmp_processor.py
```

## 📋 Prerequisites

### 1. Python Environment
- Python 3.8+
- Required packages (see requirements.txt)

### 2. FMP API Key
- Get free API key from: https://financialmodelingprep.com/
- Add to `.env` file as `FMP_API_KEY=your_key_here`

### 3. QuestDB
- Download from: https://questdb.io/get-questdb/
- Extract and run `questdb.exe`
- Access web console: http://localhost:9000

## 📁 Output Files

### Data Formats
- **Parquet**: `data/parquet/{SYMBOL}_5y_ohlcv.parquet`
- **CSV**: `data/csv/{SYMBOL}_5y_ohlcv.csv`
- **QuestDB**: Table `ohlcv1d` (accessible via web console)

### Analysis Results
- **Summary**: `data/etf_analysis_results.csv`
- Contains volatility metrics, expense ratios, data points, and file status

## 🗄️ QuestDB Schema

Table: `ohlcv1d`
```sql
CREATE TABLE ohlcv1d (
    symbol SYMBOL,           -- ETF ticker symbol
    name STRING,             -- Full ETF name
    open DOUBLE,             -- Opening price
    high DOUBLE,             -- Daily high price
    low DOUBLE,              -- Daily low price
    close DOUBLE,            -- Closing price
    volume LONG,             -- Trading volume
    adj_close DOUBLE,        -- Adjusted closing price
    ts TIMESTAMP             -- Date (partitioned by day)
) TIMESTAMP(ts) PARTITION BY DAY WAL;
```

## 📊 Sample QuestDB Queries

```sql
-- View all data for SPY
SELECT * FROM ohlcv1d WHERE symbol = 'SPY' ORDER BY ts DESC LIMIT 10;

-- Get latest prices for all ETFs
SELECT symbol, MAX(ts) as latest_date, 
       close as latest_price, volume
FROM ohlcv1d 
GROUP BY symbol, close, volume 
ORDER BY symbol;

-- Calculate average volume by ETF
SELECT symbol, AVG(volume) as avg_volume 
FROM ohlcv1d 
GROUP BY symbol 
ORDER BY avg_volume DESC;

-- Get monthly high/low prices
SELECT symbol, 
       DATE_TRUNC('month', ts) as month,
       MAX(high) as monthly_high,
       MIN(low) as monthly_low
FROM ohlcv1d 
GROUP BY symbol, month 
ORDER BY symbol, month DESC;
```

## 🧪 Testing Setup

Run the test script to verify everything is configured correctly:

```bash
python test_setup.py
```

This will check:
- ✅ Python package imports
- ✅ Environment configuration
- ✅ QuestDB connection
- ✅ FMP API access

## 📈 Key Metrics Calculated

### Volatility
- **Annualized Volatility**: Based on 252 trading days rolling window
- **Formula**: `σ_annual = σ_daily × √252`
- **Higher is better** for trading opportunities

### Expense Ratio
- **Annual Management Fee**: Percentage of assets
- **Source**: FMP ETF profile data
- **Lower is better** for cost efficiency

## 🔧 Configuration

### Environment Variables (.env)
```bash
# Required
FMP_API_KEY=your_fmp_api_key_here

# Optional (defaults provided)
QUESTDB_HOST=localhost
QUESTDB_PORT=9000
DATA_DIR=./data
PARQUET_DIR=./data/parquet
CSV_DIR=./data/csv
```

### Customizing ETF List

To analyze different ETFs, edit `etf_fmp_processor.py`:

```python
self.etf_symbols = {
    'SPY': 'SPDR S&P 500 ETF Trust',
    'QQQ': 'Invesco QQQ Trust ETF',
    'IWM': 'iShares Russell 2000 ETF',
    'VXX': 'iPath Series B S&P 500 VIX Short-Term Futures ETN',
    # Add your ETFs here:
    # 'VTI': 'Vanguard Total Stock Market ETF',
    # 'BND': 'Vanguard Total Bond Market ETF'
}
```

## 🔍 Troubleshooting

### Common Issues

**1. FMP API Key Issues**
```
❌ Configuration error: Please set your FMP_API_KEY in the .env file
```
- Solution: Get API key from https://financialmodelingprep.com/
- Add to .env file: `FMP_API_KEY=your_actual_key`

**2. QuestDB Connection Failed**
```
❌ Could not connect to QuestDB at localhost:9000
```
- Solution: Download and start QuestDB from https://questdb.io/get-questdb/
- Run: `questdb.exe` (Windows) or `./questdb.sh start` (Linux/Mac)

**3. Missing Packages**
```
❌ Import "package_name" could not be resolved
```
- Solution: `pip install -r requirements.txt`

**4. No Data Returned**
```
❌ Failed to fetch data for SYMBOL
```
- Check FMP API key validity
- Verify symbol exists and is tradeable
- Check API rate limits (free tier: 250 calls/day)

## 📞 Support

### Resources
- **FMP API Docs**: https://financialmodelingprep.com/developer/docs
- **QuestDB Docs**: https://questdb.io/docs/
- **Pandas Docs**: https://pandas.pydata.org/docs/

### File Structure
```
etf_data_project/
├── etf_fmp_processor.py      # Main ETF data processor
├── setup_questdb.py         # QuestDB table setup
├── run_analysis.py          # Complete pipeline runner
├── test_setup.py            # System test script
├── run_etf_analysis.bat     # Windows batch runner
├── requirements.txt         # Python dependencies
├── .env                     # Environment configuration
├── .env.example            # Environment template
└── data/                   # Output data directory
    ├── parquet/           # Parquet files
    ├── csv/               # CSV files
    └── etf_analysis_results.csv  # Analysis summary
```

## 📄 License

This project is for educational and research purposes. Please respect data provider terms of service.

---

**Ready to analyze ETF data? Run `run_etf_analysis.bat` or `python run_analysis.py` to get started! 🚀**
