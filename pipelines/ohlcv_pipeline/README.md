# OHLCV Data Pipeline

Complete pipeline for downloading, storing, and analyzing financial market OHLCV data.

## 📁 Folder Structure

```
pipelines/ohlcv_pipeline/
├── README.md                          # This file
├── run_unified_pipeline.py            # Main orchestrator
│
├── src/                               # Core pipeline modules
│   ├── create_unified_schema.py       # QuestDB schema creator
│   ├── unified_data_crawler.py        # Data downloader (FMP, Binance)
│   └── load_to_questdb.py             # CSV/Parquet → QuestDB loader
│
├── analytics/                         # Analytics scripts
│   ├── load_and_analyze.py            # Load data + run SQL analytics
│   ├── analytics_comprehensive.py     # Python-based analytics (CSV)
│   ├── create_analytics_tables.py     # Create result tables
│   └── populate_analytics_tables.py   # Populate analytics tables
│
├── scripts/                           # Utility scripts
│   └── test_pipeline_simple.py        # Simple test script
│
└── docs/                              # Documentation
    ├── QUICK_START_GUIDE.md           # Quick start guide
    └── UNIFIED_PIPELINE_README.md     # Detailed documentation
```

## 🚀 Quick Start

### 1. Prerequisites

```bash
# Install dependencies
pip install pandas numpy psycopg2 requests python-dotenv pyarrow

# Start QuestDB (run as Administrator)
cd ../../questdb-9.0.3-rt-windows-x86-64/bin
.\questdb.exe install
.\questdb.exe start
```

### 2. Run the Pipeline

**Option A: Full pipeline (recommended)**
```bash
# Test with 3 symbols, 1 year of data
python run_unified_pipeline.py --mode test --years 1 --limit 3

# Production: All priority-1 symbols, 5 years
python run_unified_pipeline.py --priority 1 --years 5
```

**Option B: Step-by-step**
```bash
# Step 1: Create schema
python src/create_unified_schema.py

# Step 2: Download data
python src/unified_data_crawler.py --limit 5 --years 1

# Step 3: Load to QuestDB
python src/load_to_questdb.py --type csv --verify
```

### 3. Run Analytics

**Option A: Load data + run analytics (SQL-based)**
```bash
python analytics/load_and_analyze.py
```

**Option B: Python-based analytics (works without QuestDB)**
```bash
python analytics/analytics_comprehensive.py
```

**Option C: Create persistent analytics tables**
```bash
# Create result tables
python analytics/create_analytics_tables.py

# Populate with data
python analytics/populate_analytics_tables.py
```

## 📊 Data Sources

- **FMP API**: Stocks, ETFs (1min, 1hour, 1day)
- **Binance API**: Cryptocurrencies (1m, 1h, 1d)

Configure symbols in: `../../data/metadata/symbol_list.csv`

## 🗄️ Database Schema & Table Relationships

### **Complete Table Architecture**

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA LAYER                                   │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                    ohlcv_unified                             │   │
│  │  PRIMARY DATA TABLE - All raw OHLCV data                    │   │
│  │                                                               │   │
│  │  • symbol          STRING      (SPY, QQQ, AAPL...)          │   │
│  │  • timestamp       TIMESTAMP   (Designated timestamp)        │   │
│  │  • timeframe       STRING      ('1min', '1hour', '1day')    │   │
│  │  • open/high/low   DOUBLE                                    │   │
│  │  • close/volume    DOUBLE/LONG                               │   │
│  │  • returns         DOUBLE      (Pre-calculated)              │   │
│  │  • vwap            DOUBLE                                    │   │
│  │                                                               │   │
│  │  Access: WHERE timeframe = '1min' | '1hour' | '1day'        │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              │                                        │
│                              │ (Source for analytics)                │
│                              ▼                                        │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                      ANALYTICS LAYER                                 │
│            (Generated from ohlcv_unified WHERE timeframe='1day')     │
│                                                                       │
│  ┌──────────────────────────┐  ┌──────────────────────────────┐    │
│  │    beta_results          │  │   correlation_matrix         │    │
│  │  Beta vs SPY benchmark   │  │   Pairwise correlations      │    │
│  │                          │  │                              │    │
│  │  • symbol        STRING  │  │  • symbol1       STRING      │    │
│  │  • beta          DOUBLE  │  │  • symbol2       STRING      │    │
│  │  • r_squared     DOUBLE  │  │  • correlation   DOUBLE      │    │
│  │  • correlation   DOUBLE  │  │  • observations  INT         │    │
│  │  • observations  INT     │  │  • calculated_at TIMESTAMP   │    │
│  │  • calculated_at TS      │  │                              │    │
│  │                          │  │  Join: symbol1 = symbol      │    │
│  │  Join: symbol = symbol   │  │        symbol2 = symbol      │    │
│  └──────────────────────────┘  └──────────────────────────────┘    │
│                                                                       │
│  ┌──────────────────────────┐  ┌──────────────────────────────┐    │
│  │  volatility_metrics      │  │   significant_events         │    │
│  │  Risk & performance      │  │   Major price movements      │    │
│  │                          │  │                              │    │
│  │  • symbol        STRING  │  │  • symbol        STRING      │    │
│  │  • annual_vol_pct DOUBLE │  │  • event_date    TIMESTAMP   │    │
│  │  • annual_ret_pct DOUBLE │  │  • return_pct    DOUBLE      │    │
│  │  • sharpe_ratio  DOUBLE  │  │  • close_price   DOUBLE      │    │
│  │  • observations  INT     │  │  • event_type    STRING      │    │
│  │  • calculated_at TS      │  │  • detected_at   TIMESTAMP   │    │
│  │                          │  │                              │    │
│  │  Join: symbol = symbol   │  │  Join: symbol = symbol       │    │
│  └──────────────────────────┘  │        event_date = date     │    │
│                                 └──────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
```

---

### **Table 1: `ohlcv_unified` (Primary Data Table)**

**Purpose:** Stores all raw OHLCV data across all timeframes in a single unified table.

**Schema:**
```sql
CREATE TABLE ohlcv_unified (
    symbol STRING,                 -- Stock/ETF symbol (SPY, QQQ, AAPL...)
    asset_class STRING,            -- 'stock', 'etf', 'crypto', 'commodity'
    index_membership STRING,       -- 'SP500', 'NASDAQ100', 'N/A'
    timestamp TIMESTAMP,           -- Designated timestamp (MAIN)
    timeframe STRING,              -- '1min', '1hour', '1day'
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume LONG,
    adj_close DOUBLE,
    vwap DOUBLE,
    returns DOUBLE,                -- Pre-calculated daily returns
    log_returns DOUBLE,
    data_source STRING,            -- 'FMP', 'Binance'
    inserted_at TIMESTAMP
) timestamp(timestamp) PARTITION BY DAY;
```

**How to Access:**
```sql
-- Get 1-minute data
SELECT * FROM ohlcv_unified WHERE timeframe = '1min' AND symbol = 'SPY';

-- Get hourly data
SELECT * FROM ohlcv_unified WHERE timeframe = '1hour';

-- Get daily data
SELECT * FROM ohlcv_unified WHERE timeframe = '1day';

-- Count records by timeframe
SELECT timeframe, COUNT(*) FROM ohlcv_unified GROUP BY timeframe;
```

**Current Data (as of 2025-10-12):**
- SPY: 500 daily, 896 hourly, 1560 minute records
- QQQ: 500 daily, 896 hourly, 1560 minute records
- IWM: 500 daily, 896 hourly records
- **Total: 6,280 records**

---

### **Table 2: `beta_results` (Analytics)**

**Purpose:** Beta calculations measuring systematic risk vs SPY benchmark.

**Schema:**
```sql
CREATE TABLE beta_results (
    symbol STRING,
    beta DOUBLE,                   -- β = Cov(stock, market) / Var(market)
    r_squared DOUBLE,              -- R² (variance explained)
    correlation DOUBLE,            -- Correlation coefficient
    observations INT,              -- Number of data points used
    calculated_at TIMESTAMP        -- When calculation was performed
) timestamp(calculated_at) PARTITION BY MONTH;
```

**How to Access:**
```sql
-- Get all beta results
SELECT * FROM beta_results ORDER BY beta DESC;

-- High beta stocks (volatile)
SELECT * FROM beta_results WHERE beta > 1.2;

-- Well-explained variance
SELECT * FROM beta_results WHERE r_squared > 0.9;
```

**Join with OHLCV:**
```sql
-- Combine price data with beta
SELECT
    o.symbol,
    o.timestamp,
    o.close,
    b.beta,
    b.r_squared
FROM ohlcv_unified o
JOIN beta_results b ON o.symbol = b.symbol
WHERE o.timeframe = '1day'
ORDER BY o.timestamp DESC;
```

**Current Results:**
- QQQ: Beta = 1.169, R² = 0.941
- IWM: Beta = 1.034, R² = 0.762

---

### **Table 3: `correlation_matrix` (Analytics)**

**Purpose:** Pairwise correlation coefficients between all symbols.

**Schema:**
```sql
CREATE TABLE correlation_matrix (
    symbol1 STRING,
    symbol2 STRING,
    correlation DOUBLE,            -- Pearson correlation (-1 to 1)
    observations INT,
    calculated_at TIMESTAMP
) timestamp(calculated_at) PARTITION BY MONTH;
```

**How to Access:**
```sql
-- All correlations
SELECT * FROM correlation_matrix ORDER BY abs(correlation) DESC;

-- High positive correlation
SELECT * FROM correlation_matrix WHERE correlation > 0.8;

-- Find correlations for specific symbol
SELECT * FROM correlation_matrix
WHERE symbol1 = 'SPY' OR symbol2 = 'SPY'
ORDER BY abs(correlation) DESC;
```

**Join with OHLCV:**
```sql
-- Price data for correlated pairs
SELECT
    o1.timestamp,
    o1.symbol as sym1,
    o1.close as close1,
    o2.symbol as sym2,
    o2.close as close2,
    c.correlation
FROM ohlcv_unified o1
JOIN ohlcv_unified o2 ON o1.timestamp = o2.timestamp
JOIN correlation_matrix c ON o1.symbol = c.symbol1 AND o2.symbol = c.symbol2
WHERE o1.timeframe = '1day' AND o2.timeframe = '1day'
    AND c.correlation > 0.9;
```

**Current Results:**
- QQQ-SPY: 0.970 (extremely high)
- IWM-SPY: 0.873 (high)
- IWM-QQQ: 0.817 (high)

---

### **Table 4: `volatility_metrics` (Analytics)**

**Purpose:** Risk and performance metrics (volatility, returns, Sharpe ratio).

**Schema:**
```sql
CREATE TABLE volatility_metrics (
    symbol STRING,
    annual_volatility_pct DOUBLE,  -- σ * √252 * 100
    annual_return_pct DOUBLE,      -- μ * 252 * 100
    sharpe_ratio DOUBLE,           -- (μ / σ) * √252
    observations INT,
    calculated_at TIMESTAMP
) timestamp(calculated_at) PARTITION BY MONTH;
```

**How to Access:**
```sql
-- All metrics
SELECT * FROM volatility_metrics ORDER BY sharpe_ratio DESC;

-- High Sharpe ratio (best risk-adjusted returns)
SELECT * FROM volatility_metrics WHERE sharpe_ratio > 1.0;

-- Low volatility stocks
SELECT * FROM volatility_metrics WHERE annual_volatility_pct < 20;
```

**Join with OHLCV:**
```sql
-- Recent prices with risk metrics
SELECT
    o.symbol,
    o.timestamp,
    o.close,
    v.annual_volatility_pct,
    v.annual_return_pct,
    v.sharpe_ratio
FROM ohlcv_unified o
JOIN volatility_metrics v ON o.symbol = v.symbol
WHERE o.timeframe = '1day'
    AND o.timestamp > '2025-01-01'
ORDER BY v.sharpe_ratio DESC;
```

**Current Results:**
- QQQ: 23.64% vol, 20.78% return, Sharpe 0.88
- SPY: 19.61% vol, 13.97% return, Sharpe 0.71
- IWM: 23.24% vol, 9.98% return, Sharpe 0.43

---

### **Table 5: `significant_events` (Analytics)**

**Purpose:** Major price movements (>3% daily returns).

**Schema:**
```sql
CREATE TABLE significant_events (
    symbol STRING,
    event_date TIMESTAMP,          -- Date of the event
    return_pct DOUBLE,             -- Return % (e.g., 12.00 = +12%)
    close_price DOUBLE,
    event_type STRING,             -- 'SPIKE_UP' or 'SPIKE_DOWN'
    detected_at TIMESTAMP
) timestamp(detected_at) PARTITION BY MONTH;
```

**How to Access:**
```sql
-- All events
SELECT * FROM significant_events ORDER BY abs(return_pct) DESC;

-- Positive spikes only
SELECT * FROM significant_events WHERE event_type = 'SPIKE_UP';

-- Events for specific symbol
SELECT * FROM significant_events
WHERE symbol = 'QQQ'
ORDER BY event_date DESC;
```

**Join with OHLCV:**
```sql
-- Price context around events (±5 days)
SELECT
    e.symbol,
    e.event_date,
    e.return_pct,
    o.timestamp,
    o.close,
    o.returns
FROM significant_events e
JOIN ohlcv_unified o ON e.symbol = o.symbol
WHERE o.timeframe = '1day'
    AND o.timestamp BETWEEN e.event_date - INTERVAL '5' DAY
                       AND e.event_date + INTERVAL '5' DAY
ORDER BY e.event_date, o.timestamp;
```

**Current Results:**
- QQQ: +12.00% on 2025-04-09 (largest gain)
- IWM: -6.42% on 2025-04-03 (largest loss)

---

### **Master Query: Join All Tables**

Combine all analytics with price data:

```sql
SELECT
    o.symbol,
    o.timestamp,
    o.close,
    o.returns,
    b.beta,
    b.r_squared,
    v.annual_volatility_pct,
    v.sharpe_ratio,
    c.correlation as corr_with_spy
FROM ohlcv_unified o
LEFT JOIN beta_results b ON o.symbol = b.symbol
LEFT JOIN volatility_metrics v ON o.symbol = v.symbol
LEFT JOIN correlation_matrix c ON o.symbol = c.symbol1 AND c.symbol2 = 'SPY'
WHERE o.timeframe = '1day'
    AND o.timestamp > '2025-01-01'
ORDER BY o.timestamp DESC, o.symbol
LIMIT 100;
```

---

### **Table Relationships Diagram**

```
ohlcv_unified (6,280 records)
    ├── WHERE timeframe = '1min'  → Minute data
    ├── WHERE timeframe = '1hour' → Hourly data
    └── WHERE timeframe = '1day'  → Daily data (used for analytics)
              │
              ├─→ beta_results (2 records)
              │      JOIN ON symbol = symbol
              │
              ├─→ correlation_matrix (3 pairs)
              │      JOIN ON symbol1/symbol2 = symbol
              │
              ├─→ volatility_metrics (3 records)
              │      JOIN ON symbol = symbol
              │
              └─→ significant_events (22 events)
                     JOIN ON symbol = symbol
                     JOIN ON event_date = timestamp::date
```

## 📈 Analytics Performed

1. **Beta Calculation** - Measure systematic risk vs SPY
   - Formula: `β = Cov(stock, market) / Var(market)`
   - R² and correlation included

2. **Regression Analysis** - CAPM model
   - Formula: `R_stock = α + β * R_market`
   - Identifies excess returns (alpha)

3. **Correlation Matrix** - Pairwise correlations
   - Identifies highly correlated assets

4. **Volatility Analysis** - Risk metrics
   - Annual volatility: `σ * √252`
   - Sharpe ratio: `(μ / σ) * √252`

5. **Event Studies** - Significant price movements
   - Detects returns > 3% (SPIKE_UP/SPIKE_DOWN)

## 🔍 Example Queries

```sql
-- Get 1min data for SPY
SELECT * FROM ohlcv_unified
WHERE symbol = 'SPY' AND timeframe = '1min'
ORDER BY timestamp DESC LIMIT 100;

-- Query beta results
SELECT * FROM beta_results ORDER BY beta DESC;

-- Find high volatility stocks
SELECT * FROM volatility_metrics
WHERE annual_volatility_pct > 25
ORDER BY sharpe_ratio DESC;

-- Recent significant events
SELECT * FROM significant_events
WHERE event_type = 'SPIKE_UP'
ORDER BY return_pct DESC LIMIT 10;

-- Join OHLCV with analytics
SELECT
    o.symbol,
    o.timestamp,
    o.close,
    b.beta,
    v.sharpe_ratio
FROM ohlcv_unified o
JOIN beta_results b ON o.symbol = b.symbol
JOIN volatility_metrics v ON o.symbol = v.symbol
WHERE o.timeframe = '1day'
ORDER BY o.timestamp DESC
LIMIT 100;
```

## 📝 Configuration

### Years of Data

Override years via command line:

```bash
# 1 year (fast test)
python run_unified_pipeline.py --years 1 --limit 5

# 5 years (recommended)
python run_unified_pipeline.py --years 5 --priority 1

# 10 years (large dataset)
python run_unified_pipeline.py --years 10 --mode full

# 20 years (maximum history)
python run_unified_pipeline.py --years 20 --mode full
```

### Symbol List

Edit `../../data/metadata/symbol_list.csv`:

```csv
symbol,name,asset_class,index_membership,sector,industry,timeframes,years,priority
AAPL,Apple Inc.,stock,SP500,Technology,Consumer Electronics,"1hour,1day",10,1
SPY,S&P 500 ETF,etf,SP500,N/A,N/A,"1min,1hour,1day",5,1
BTCUSDT,Bitcoin,crypto,N/A,Crypto,Cryptocurrency,"1hour,1day",3,2
```

## 🌐 Web Console

Access QuestDB web console: **http://localhost:9000**

## 📦 Output Files

Data is saved in multiple formats:

```
../../data/
├── csv/              # CSV files
│   ├── etf/
│   ├── stocks/
│   └── crypto/
│
├── parquet/          # Parquet files (compressed)
│   ├── etf/
│   ├── stocks/
│   └── crypto/
│
└── metadata/
    └── symbol_list.csv
```

## 🔧 Troubleshooting

### QuestDB not running
```bash
# Check if running
tasklist | findstr java

# Start QuestDB
cd ../../questdb-9.0.3-rt-windows-x86-64/bin
.\questdb.exe start
```

### API rate limits
```bash
# Reduce number of symbols
python run_unified_pipeline.py --limit 5

# Or increase delay in unified_data_crawler.py (line 148)
time.sleep(2)  # Increase from 1 to 2 seconds
```

### Unicode/encoding errors
- Use Windows Terminal or PowerShell instead of cmd.exe
- Scripts automatically handle UTF-8 encoding

## 📚 Additional Documentation

- **Quick Start Guide**: `docs/QUICK_START_GUIDE.md`
- **Full Documentation**: `docs/UNIFIED_PIPELINE_README.md`

## 🔄 Daily Updates

Schedule daily data updates:

```bash
# Windows Task Scheduler
schtasks /create /tn "MarketData" /tr "python C:\etf_data_project\pipelines\ohlcv_pipeline\run_unified_pipeline.py --priority 1 --years 1 --skip-schema" /sc daily /st 02:00
```

## 📊 Data Volume Estimates

| Years | Records/Symbol | Total Size (500 symbols) |
|-------|----------------|--------------------------|
| 1     | ~252 days      | ~125 MB                  |
| 5     | ~1,260 days    | ~630 MB                  |
| 10    | ~2,520 days    | ~1.26 GB                 |
| 20    | ~5,040 days    | ~2.52 GB                 |

*Note: Add 1hour data multiplies size by ~8x, 1min data by ~390x*

## 🎯 Use Cases

1. **Backtesting** - Historical OHLCV data for strategy testing
2. **Risk Analysis** - Beta, volatility, correlation metrics
3. **Portfolio Optimization** - Sharpe ratios, diversification
4. **Event Studies** - Analyze market reactions
5. **Machine Learning** - Feature engineering for predictive models

## 📄 License

Internal use only. API keys required for data sources.

---

**Last Updated**: 2025-10-12
**Version**: 1.0
