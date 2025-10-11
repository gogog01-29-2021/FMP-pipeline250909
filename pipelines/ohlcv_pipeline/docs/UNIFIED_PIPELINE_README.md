# Unified Data Pipeline - Complete Guide

## 📋 Overview

A comprehensive ETL pipeline for downloading, storing, and analyzing multi-asset financial market data:
- **S&P 500 stocks** (500 symbols)
- **Major ETFs** (SPY, QQQ, IWM, etc.)
- **Cryptocurrencies** (BTC, ETH, etc.)
- **Commodities** (Gold, Oil, etc.)

**Data Coverage:**
- 20 years of historical data
- Multiple timeframes: 1-minute, 1-hour, 1-day
- ~30GB total storage (CSV + Parquet + QuestDB)

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                              │
├─────────────────────────────────────────────────────────────┤
│  FMP API          │  Binance API      │  Commodity APIs     │
│  (Stocks/ETFs)    │  (Crypto)         │  (Futures)          │
└─────────┬─────────┴─────────┬─────────┴──────────┬──────────┘
          │                   │                     │
          ▼                   ▼                     ▼
┌─────────────────────────────────────────────────────────────┐
│              UNIFIED DATA CRAWLER                            │
│          (unified_data_crawler.py)                           │
│  - Fetches OHLCV data from APIs                             │
│  - Calculates returns, VWAP                                 │
│  - Saves to CSV + Parquet                                   │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                  LOCAL STORAGE                               │
│  data/csv/        │  data/parquet/                          │
│  ├─ stocks/       │  ├─ stocks/                             │
│  ├─ etf/          │  ├─ etf/                                │
│  ├─ crypto/       │  ├─ crypto/                             │
│  └─ commodity/    │  └─ commodity/                          │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│               ETL LOADER                                     │
│          (load_to_questdb.py)                               │
│  - Reads CSV/Parquet files                                  │
│  - Loads into QuestDB                                       │
│  - Handles deduplication                                    │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                  QUESTDB DATABASE                            │
│  Table: ohlcv_unified (partitioned by DAY)                  │
│  - All symbols in one table                                 │
│  - Multi-timeframe support                                  │
│  - Optimized for time-series queries                        │
└─────────────────────────────────────────────────────────────┘
```

---

## 📂 Project Structure

```
etf_data_project/
├── src/
│   ├── create_unified_schema.py      # QuestDB schema creator
│   ├── unified_data_crawler.py       # Data fetcher
│   ├── load_to_questdb.py            # ETL loader
│   └── ...
├── data/
│   ├── csv/
│   │   ├── stocks/
│   │   ├── etf/
│   │   ├── crypto/
│   │   └── commodity/
│   ├── parquet/
│   │   └── [same structure]
│   └── metadata/
│       └── symbol_list.csv           # Symbol configuration
├── logs/
│   └── [pipeline logs]
├── run_unified_pipeline.py           # Main orchestrator
└── UNIFIED_PIPELINE_README.md        # This file
```

---

## 🚀 Quick Start

### Prerequisites

1. **Python 3.8+** installed
2. **QuestDB** running (see setup below)
3. **FMP API Key** (get from financialmodelingprep.com)

### Setup Steps

```bash
# 1. Start QuestDB
cd questdb-9.0.3-rt-windows-x86-64
.\bin\questdb.exe start

# 2. Set up environment
echo FMP_API_KEY=your_api_key_here > .env

# 3. Install dependencies
pip install pandas numpy requests psycopg2 pyarrow python-dotenv

# 4. Run quick test (5 symbols)
python run_unified_pipeline.py --mode test
```

---

## 💻 Usage

### Mode 1: Quick Test (Recommended First Run)

Tests the pipeline with 5 priority-1 symbols:

```bash
python run_unified_pipeline.py --mode test
```

**What it does:**
- ✅ Creates QuestDB schema
- ✅ Crawls 5 high-priority symbols
- ✅ Loads data into QuestDB
- ✅ Verifies results

**Expected time:** 5-10 minutes
**Expected data:** ~50MB

### Mode 2: Priority 1 Only (Important Symbols)

Crawls all priority-1 symbols (AAPL, MSFT, SPY, QQQ, etc.):

```bash
python run_unified_pipeline.py --mode priority1
```

**Expected time:** 30-60 minutes
**Expected data:** ~2GB

### Mode 3: Full Production (All Symbols)

Crawls ALL 65+ symbols with all timeframes:

```bash
python run_unified_pipeline.py --mode full
```

**Expected time:** 4-8 hours
**Expected data:** ~30GB

**⚠️ WARNING:** This will use significant API quota!

### Mode 4: Reload Existing Data

Re-loads existing CSV files into QuestDB (useful after schema changes):

```bash
python run_unified_pipeline.py --mode reload
```

---

## 🔧 Advanced Usage

### Custom Priority

Crawl only symbols with priority ≤ 2:

```bash
python run_unified_pipeline.py --priority 2
```

### Limit Symbols

Crawl only first 10 symbols:

```bash
python run_unified_pipeline.py --limit 10
```

### Skip Steps

Skip schema creation (if already exists):

```bash
python run_unified_pipeline.py --skip-schema
```

Skip data crawling (only load existing files):

```bash
python run_unified_pipeline.py --skip-crawl
```

### Individual Scripts

Run components separately:

```bash
# 1. Create schema only
python src/create_unified_schema.py

# 2. Crawl specific symbol
python src/unified_data_crawler.py --symbol AAPL

# 3. Crawl with priority filter
python src/unified_data_crawler.py --priority 1

# 4. Load CSV files to QuestDB
python src/load_to_questdb.py --type csv --verify

# 5. Load Parquet files
python src/load_to_questdb.py --type parquet
```

---

## 📊 Database Schema

### Main Table: `ohlcv_unified`

```sql
CREATE TABLE ohlcv_unified (
    symbol STRING,              -- Stock/ETF/Crypto symbol
    asset_class STRING,         -- 'stock', 'etf', 'crypto', 'commodity'
    index_membership STRING,    -- 'SP500', 'NASDAQ100', etc.
    timestamp TIMESTAMP,        -- Price timestamp
    timeframe STRING,           -- '1min', '1hour', '1day'
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume LONG,
    adj_close DOUBLE,          -- Adjusted close (stocks only)
    vwap DOUBLE,               -- Volume-weighted average
    returns DOUBLE,            -- Simple returns
    log_returns DOUBLE,        -- Log returns
    data_source STRING,        -- 'FMP', 'Binance'
    inserted_at TIMESTAMP      -- When inserted
) timestamp(timestamp) PARTITION BY DAY;
```

### Supporting Tables

- `symbol_metadata` - Symbol information
- `index_constituents` - Index membership tracking
- `data_quality` - Data quality metrics
- `pairwise_correlation` - Pre-computed correlations
- `rolling_statistics` - Pre-computed rolling stats

---

## 📈 Sample Queries

### Basic Queries

```sql
-- Total records
SELECT COUNT(*) FROM ohlcv_unified;

-- Records by symbol
SELECT symbol, COUNT(*) as count
FROM ohlcv_unified
GROUP BY symbol
ORDER BY count DESC;

-- Latest prices
SELECT symbol, timestamp, close
FROM ohlcv_unified
WHERE timestamp = (SELECT MAX(timestamp) FROM ohlcv_unified)
ORDER BY symbol;

-- AAPL daily data (last 30 days)
SELECT timestamp, open, high, low, close, volume
FROM ohlcv_unified
WHERE symbol = 'AAPL'
  AND timeframe = '1day'
  AND timestamp >= dateadd('d', -30, now())
ORDER BY timestamp DESC;
```

### Advanced Analytics

```sql
-- Calculate 30-day volatility
SELECT symbol,
       STDDEV(returns) * SQRT(252) * 100 as annual_volatility
FROM ohlcv_unified
WHERE timeframe = '1day'
  AND timestamp >= dateadd('d', -30, now())
GROUP BY symbol
ORDER BY annual_volatility DESC;

-- Moving average crossover
SELECT symbol, timestamp, close,
       AVG(close) OVER (ORDER BY timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as sma_20,
       AVG(close) OVER (ORDER BY timestamp ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) as sma_50
FROM ohlcv_unified
WHERE symbol = 'AAPL' AND timeframe = '1day'
ORDER BY timestamp DESC
LIMIT 100;
```

---

## 🐍 Python Integration

### Using pandas + SQLAlchemy

```python
import pandas as pd
from sqlalchemy import create_engine

# Connect to QuestDB
engine = create_engine('postgresql://admin:quest@localhost:8812/qdb')

# Load data
df = pd.read_sql("""
    SELECT * FROM ohlcv_unified
    WHERE symbol = 'AAPL' AND timeframe = '1day'
    ORDER BY timestamp DESC
    LIMIT 1000
""", engine)

# Calculate correlation matrix
symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN']
data = {}

for symbol in symbols:
    query = f"""
        SELECT timestamp, close
        FROM ohlcv_unified
        WHERE symbol = '{symbol}' AND timeframe = '1day'
        ORDER BY timestamp
    """
    df_temp = pd.read_sql(query, engine)
    data[symbol] = df_temp.set_index('timestamp')['close']

df_combined = pd.DataFrame(data)
correlation_matrix = df_combined.corr()
print(correlation_matrix)
```

### Using psycopg2 (Direct)

```python
import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host='localhost',
    port=8812,
    database='qdb',
    user='admin',
    password='quest'
)

# Query
df = pd.read_sql("""
    SELECT symbol, AVG(close) as avg_price
    FROM ohlcv_unified
    WHERE timeframe = '1day'
    GROUP BY symbol
""", conn)

conn.close()
```

---

## 🔍 Data Verification

After running the pipeline, verify data:

```bash
# Run verification
python src/load_to_questdb.py --verify
```

**Output:**
```
📊 Total Records: 2,520,000
📊 Records by Asset Class:
  stock: 2,000,000
  etf: 400,000
  crypto: 100,000
  commodity: 20,000
📊 Records by Timeframe:
  1day: 2,520,000
📅 Date Range: 2005-01-01 to 2025-01-01
```

---

## 🛠️ Troubleshooting

### QuestDB Connection Failed

**Problem:** Cannot connect to QuestDB

**Solution:**
```bash
# 1. Start QuestDB
cd questdb-9.0.3-rt-windows-x86-64
.\bin\questdb.exe start

# 2. Verify it's running
# Open browser: http://localhost:9000

# 3. Check port 8812 is open
netstat -an | findstr 8812
```

### API Rate Limit

**Problem:** FMP API returns 429 (Too Many Requests)

**Solution:**
- Reduce crawl limit: `--limit 10`
- Add delays in `unified_data_crawler.py` (increase `time.sleep()`)
- Upgrade FMP API plan

### Out of Disk Space

**Problem:** Not enough space for 30GB data

**Solution:**
- Reduce symbols: `--priority 1 --limit 20`
- Skip 1-minute data (edit `symbol_list.csv`, remove `1min` from timeframes)
- Use Parquet only (smaller size)

---

## 📊 Symbol Configuration

Edit `data/metadata/symbol_list.csv` to customize:

```csv
symbol,name,asset_class,index_membership,sector,industry,timeframes,years,priority
AAPL,Apple Inc.,stock,SP500,Technology,Consumer Electronics,"1min,1hour,1day",20,1
```

**Columns:**
- `symbol` - Ticker symbol
- `asset_class` - stock/etf/crypto/commodity
- `index_membership` - SP500/NASDAQ100/DOW30/etc
- `timeframes` - Comma-separated: 1min,1hour,1day
- `years` - Years of history to fetch
- `priority` - 1 (critical), 2 (important), 3 (optional)

---

## 📅 Scheduling (Optional)

### Windows Task Scheduler

Create daily update task:

```bash
# Run at 2 AM daily
schtasks /create /tn "MarketDataUpdate" /tr "python C:\etf_data_project\run_unified_pipeline.py --mode priority1 --skip-schema" /sc daily /st 02:00
```

### Linux Cron

```bash
# Add to crontab
0 2 * * * cd /path/to/etf_data_project && python run_unified_pipeline.py --mode priority1 --skip-schema
```

---

## 📚 Next Steps

1. **Explore data in QuestDB**: http://localhost:9000
2. **Run analytics**: Use Python notebooks with pandas
3. **Build dashboards**: Connect to Grafana/Tableau
4. **Add more symbols**: Edit `symbol_list.csv`
5. **Pre-compute analytics**: Run correlation/regression scripts

---

## 🤝 Support

For issues or questions:
1. Check logs in `logs/` directory
2. Verify QuestDB is running
3. Check API key in `.env` file

---

## 📄 License

This project is for educational and research purposes.

**Data Sources:**
- FMP API: https://financialmodelingprep.com
- Binance API: https://binance.com

**Database:**
- QuestDB: https://questdb.io
