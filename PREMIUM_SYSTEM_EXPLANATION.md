# Premium System Explanation

## 1. ✅ **What's POSSIBLE with Premium Account**

### Daily Data (ohlcv1d table)
| Feature | Status | Details |
|---------|--------|---------|
| SPY, QQQ, IWM, VXX | ✅ Possible | All index ETFs working |
| 1 month historical | ✅ Possible | Last 30 days |
| 5 years historical | ✅ Possible | Full history |
| Volatility calculation | ✅ Possible | Monthly & annual |
| Expense ratio sorting | ✅ Possible | Built-in |
| CSV + Parquet storage | ✅ Possible | Both formats |
| QuestDB storage | ✅ Possible | Automated |
| Nightly updates | ✅ Possible | 2 AM scheduled |

### 1-Minute Data (ohlcv1min table)
| Feature | Status | Details |
|---------|--------|---------|
| S&P 500 stocks | ✅ Possible | Top 500 companies |
| SPY ETF 1-min | ✅ Possible | Intraday data |
| Last 30 days | ✅ Possible | Rolling window |
| Real-time updates | ❌ Not Possible | Need enterprise |
| Unlimited history | ❌ Not Possible | API limitation |

## 2. 📊 **How Tables Work (No Collision)**

### Table Design:

```sql
-- DAILY TABLE (ohlcv1d)
-- Stores: ALL historical daily data
-- Updates: Only adds NEW dates (no duplicates)
CREATE TABLE ohlcv1d (
    symbol STRING,      -- e.g., 'SPY'
    date TIMESTAMP,     -- e.g., '2024-01-15'
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume LONG,
    timestamp TIMESTAMP -- same as date
) timestamp(timestamp) PARTITION BY MONTH;

-- 1-MINUTE TABLE (ohlcv1min)
-- Stores: Last 30 days only
-- Updates: Deletes old data, adds new
CREATE TABLE ohlcv1min (
    symbol STRING,      -- e.g., 'AAPL'
    date TIMESTAMP,     -- e.g., '2024-01-15 09:30:00'
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume LONG,
    timestamp TIMESTAMP -- same as date
) timestamp(timestamp) PARTITION BY DAY;
```

### How Updates Work:

**Daily Updates (ohlcv1d):**
```
Night 1: SPY has 1000 records
Night 2: Fetches last 30 days, finds 1 new day
Result: SPY now has 1001 records (only NEW date added)
```

**1-Minute Updates (ohlcv1min):**
```
Night 1: AAPL has 7,800 records (30 days × 390 min/day)
Night 2: Deletes data older than 30 days, adds today's
Result: Still ~7,800 records (rolling 30-day window)
```

## 3. 🚀 **One Modular File Solution**

The `premium_unified_processor.py` contains everything:

```python
class PremiumDataProcessor:
    # All features in one class
    
    def process_indices():
        # Handles: SPY, QQQ, IWM, VXX
        # Sorts by: Volatility ↑, Expense Ratio ↓
        # Saves to: CSV, Parquet, QuestDB
    
    def process_sp500_1min():
        # Handles: S&P 500 stocks
        # Fetches: 1-minute data
        # Saves to: CSV, Parquet, QuestDB
    
    def run_nightly_update():
        # Runs everything at 2 AM
        # Updates both tables
        # Generates reports
```

## 4. 📁 **Data Organization**

```
data/
├── daily/          # Daily OHLCV data
│   ├── csv/        # SPY_daily_20240115.csv
│   └── parquet/    # SPY_daily_20240115.parquet
├── 1min/           # 1-minute data
│   ├── csv/        # AAPL_1min_20240115.csv
│   └── parquet/    # AAPL_1min_20240115.parquet
└── reports/        # Daily analysis reports
```

## 5. ⚡ **How to Use**

### Test Premium Features:
```bash
# Test with limited data
python premium_unified_processor.py --test
```

### Run Full Update:
```bash
# Process all indices + S&P 500 stocks
python premium_unified_processor.py
```

### Schedule Nightly (2 AM):
```bash
# Create Windows scheduled task
python premium_unified_processor.py --schedule
```

## 6. 📊 **What You'll Get**

### Every Night at 2 AM:

1. **Index ETFs Updated:**
   - SPY, QQQ, IWM, VXX
   - Last 30 days daily data
   - Sorted by volatility/expense

2. **S&P 500 Stocks Updated:**
   - Top 10-25 stocks (configurable)
   - 1-minute intraday data
   - Last trading day

3. **Storage:**
   - CSV files for Excel
   - Parquet files for big data
   - QuestDB for SQL queries

4. **Reports:**
   - Daily summary
   - Volatility rankings
   - Market metrics

## 7. ⚠️ **Important Notes**

### API Limits:
- **Rate limit**: 300 requests/minute
- **Daily limit**: Depends on plan
- **Solution**: Built-in delays and limits

### Storage Management:
- **Daily data**: Keeps all history (grows slowly)
- **1-min data**: Keeps 30 days only (auto-cleanup)
- **Disk space**: ~500MB for full dataset

### Performance:
- **Nightly update**: ~5-10 minutes
- **Initial load**: ~15-20 minutes
- **Query speed**: Milliseconds in QuestDB

## 8. 🎯 **Summary**

With your premium account, you can:
- ✅ Get all index ETFs (SPY, QQQ, IWM, VXX)
- ✅ Get S&P 500 stocks 1-minute data
- ✅ Sort by volatility and expense ratio
- ✅ Save to CSV, Parquet, and QuestDB
- ✅ Automate nightly updates
- ✅ No table collisions (smart updates)

Everything is in ONE modular file that handles it all!