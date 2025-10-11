# Quick Start Guide - Unified Data Pipeline

## Summary

I've created a complete unified data pipeline with the following features:

✅ **Single table design** (`ohlcv_unified`) - All assets in one table
✅ **Configurable years** - Choose 1, 5, 10, or 20 years of data
✅ **Multi-source support** - FMP API (stocks/ETFs), Binance (crypto)
✅ **Multiple timeframes** - 1min, 1hour, 1day
✅ **Saves in 3 formats** - CSV, Parquet, QuestDB

---

## Files Created

| File | Purpose |
|------|---------|
| `src/create_unified_schema.py` | Creates QuestDB tables |
| `src/unified_data_crawler.py` | Downloads data from APIs |
| `src/load_to_questdb.py` | Loads CSV/Parquet into QuestDB |
| `run_unified_pipeline.py` | Main orchestrator |
| `data/metadata/symbol_list.csv` | Symbol configuration (67 symbols) |
| `UNIFIED_PIPELINE_README.md` | Complete documentation |

---

## How to Run (Choose your years!)

### Option 1: Quick Test (1 year, 3 symbols) - RECOMMENDED FIRST

```bash
# Start QuestDB first
cd questdb-9.0.3-rt-windows-x86-64\bin
questdb.exe start

# In a new terminal, run:
cd C:\etf_data_project

# Test with 1 year of data
python run_unified_pipeline.py --mode test --years 1 --limit 3
```

**Expected result:**
- 3 symbols (SPY, QQQ, AAPL)
- 1 year of data
- ~50MB total
- Takes 3-5 minutes

### Option 2: Custom Years

You can choose any number of years:

```bash
# 5 years of data for priority-1 symbols
python run_unified_pipeline.py --priority 1 --years 5

# 10 years for specific symbols
python src/unified_data_crawler.py --symbol AAPL --years 10

# 20 years (full historical)
python run_unified_pipeline.py --mode full --years 20
```

### Option 3: Individual Components

Run each step separately:

```bash
# Step 1: Create schema
python src/create_unified_schema.py

# Step 2: Crawl data (with custom years)
python src/unified_data_crawler.py --priority 1 --years 5 --limit 10

# Step 3: Load to QuestDB
python src/load_to_questdb.py --type csv --verify
```

---

## Data Size by Years

| Years | Records (per symbol) | Size Estimate (500 symbols) |
|-------|----------------------|-----------------------------|
| 1 year | ~252 days | ~125 MB |
| 5 years | ~1,260 days | ~630 MB |
| 10 years | ~2,520 days | ~1.26 GB |
| 20 years | ~5,040 days | ~2.52 GB |

**Note:** Add 1hour data multiplies size by ~8x, 1min data by ~390x

---

## Verify It Worked

### 1. Check Files Created

```bash
dir data\csv\stocks
dir data\csv\etf
dir data\parquet\stocks
```

You should see files like:
- `AAPL_1day_20240101_20251011.csv`
- `SPY_1hour_20240101_20251011.csv`

### 2. Check QuestDB

Open browser: http://localhost:9000

Run these queries:

```sql
-- Total records
SELECT COUNT(*) FROM ohlcv_unified;

-- Records by symbol
SELECT symbol, COUNT(*) as records
FROM ohlcv_unified
GROUP BY symbol
ORDER BY records DESC;

-- Date range
SELECT symbol, MIN(timestamp) as start_date, MAX(timestamp) as end_date
FROM ohlcv_unified
GROUP BY symbol;

-- Latest prices
SELECT symbol, timestamp, close
FROM ohlcv_unified
WHERE timestamp IN (
  SELECT MAX(timestamp) FROM ohlcv_unified GROUP BY symbol
)
ORDER BY symbol;
```

### 3. Check Logs

```bash
dir logs
type logs\data_crawler_*.log
```

---

## Examples with Different Years

### Example 1: 1 Year Test (Fast)

```bash
python src/unified_data_crawler.py --limit 5 --years 1
python src/load_to_questdb.py --type csv
```

**Result:** 5 symbols × 252 days = 1,260 records

### Example 2: 5 Years (Medium)

```bash
python src/unified_data_crawler.py --priority 1 --years 5
python src/load_to_questdb.py --type csv
```

**Result:** ~20 symbols × 1,260 days = 25,200 records

### Example 3: 10 Years (Large)

```bash
python src/unified_data_crawler.py --priority 1 --years 10
python src/load_to_questdb.py --type csv
```

**Result:** ~20 symbols × 2,520 days = 50,400 records

### Example 4: 20 Years Full (Maximum)

```bash
python run_unified_pipeline.py --mode full --years 20
```

**Result:** All symbols × 5,040 days = ~300,000+ records

---

## Troubleshooting

### QuestDB Not Running

```bash
# Check if running
tasklist | findstr questdb

# If not, start it
cd questdb-9.0.3-rt-windows-x86-64\bin
questdb.exe start

# Wait 10 seconds, then verify
# Open: http://localhost:9000
```

### Encoding Errors

The scripts have emoji/unicode characters that may cause issues in some terminals.

**Solution:** Use Windows Terminal or PowerShell, or run individual scripts:

```bash
# Instead of run_unified_pipeline.py, run steps manually:
python src/create_unified_schema.py
python src/unified_data_crawler.py --limit 3 --years 1
python src/load_to_questdb.py --type csv
```

### API Rate Limits

If you hit FMP API limits:

1. Reduce number of symbols: `--limit 5`
2. Add delay between requests (edit `unified_data_crawler.py`, increase `time.sleep()`)
3. Run in batches:

```bash
# Batch 1
python src/unified_data_crawler.py --limit 10 --years 1

# Wait 1 hour, then Batch 2
python src/unified_data_crawler.py --limit 10 --years 1
```

---

## Configuration

### Changing Years in symbol_list.csv

Edit `data/metadata/symbol_list.csv`:

```csv
symbol,name,asset_class,index_membership,sector,industry,timeframes,years,priority
AAPL,Apple Inc.,stock,SP500,Technology,Consumer Electronics,"1hour,1day",10,1
```

Change the `years` column (last number before priority).

### Override Years via Command Line

```bash
# This will override the years in symbol_list.csv
python src/unified_data_crawler.py --years 5
```

---

## What's Next?

1. **Run with your preferred years:**
   ```bash
   python run_unified_pipeline.py --limit 5 --years 5
   ```

2. **Query the data:**
   - Open http://localhost:9000
   - Run SQL queries

3. **Use in Python:**
   ```python
   import pandas as pd
   from sqlalchemy import create_engine

   engine = create_engine('postgresql://admin:quest@localhost:8812/qdb')
   df = pd.read_sql("SELECT * FROM ohlcv_unified WHERE symbol='AAPL'", engine)
   ```

4. **Add more symbols:**
   - Edit `data/metadata/symbol_list.csv`
   - Add new rows with your desired symbols

5. **Schedule daily updates:**
   ```bash
   # Run every day at 2 AM
   schtasks /create /tn "MarketData" /tr "python C:\etf_data_project\run_unified_pipeline.py --priority 1 --years 1 --skip-schema" /sc daily /st 02:00
   ```

---

## Summary: Choose Your Years!

| Use Case | Command | Years | Time | Data Size |
|----------|---------|-------|------|-----------|
| **Quick Test** | `--years 1 --limit 3` | 1 | 3 min | 10 MB |
| **Dev/Test** | `--years 5 --limit 10` | 5 | 15 min | 250 MB |
| **Production (Recent)** | `--years 5 --priority 1` | 5 | 30 min | 1 GB |
| **Full Historical** | `--years 20 --mode full` | 20 | 4 hrs | 10 GB+ |

**Recommendation:** Start with `--years 1` to test everything works, then scale up!

---

For full documentation, see: `UNIFIED_PIPELINE_README.md`
