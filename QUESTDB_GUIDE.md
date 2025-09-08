# QuestDB ETF Data Guide

## 1. Accessing QuestDB

### Web Console
Open your browser and go to: **http://localhost:9000**

### Quick Start
1. Run `start_questdb.bat` (as Administrator if needed)
2. Wait 10-15 seconds for QuestDB to start
3. Open browser to http://localhost:9000
4. Run SQL queries in the console

## 2. Useful SQL Queries

### View All Data
```sql
-- Show first 100 records
SELECT * FROM ohlcv1d LIMIT 100;

-- Count records by symbol
SELECT symbol, COUNT(*) as records 
FROM ohlcv1d 
GROUP BY symbol;
```

### Latest Prices
```sql
-- Get latest price for each symbol
SELECT symbol, date, close as latest_price
FROM ohlcv1d
WHERE date = (SELECT MAX(date) FROM ohlcv1d)
ORDER BY symbol;
```

### Volatility Analysis
```sql
-- Calculate 30-day volatility
SELECT 
    symbol,
    STDDEV(close) as price_std,
    AVG(volume) as avg_volume,
    MAX(close) - MIN(close) as price_range
FROM ohlcv1d
WHERE date >= dateadd('d', -30, now())
GROUP BY symbol
ORDER BY price_std DESC;
```

### ETF Performance
```sql
-- SPY performance over time
SELECT 
    date,
    open,
    high,
    low,
    close,
    volume
FROM ohlcv1d
WHERE symbol = 'SPY'
ORDER BY date DESC
LIMIT 30;
```

### Daily Changes
```sql
-- Calculate daily returns
SELECT 
    symbol,
    date,
    close,
    (close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) / LAG(close) OVER (PARTITION BY symbol ORDER BY date) * 100 as daily_return
FROM ohlcv1d
WHERE symbol = 'SPY'
ORDER BY date DESC
LIMIT 30;
```

### Volume Analysis
```sql
-- High volume days
SELECT symbol, date, volume, close
FROM ohlcv1d
WHERE volume > (
    SELECT AVG(volume) * 2 
    FROM ohlcv1d 
    WHERE symbol = ohlcv1d.symbol
)
ORDER BY volume DESC
LIMIT 20;
```

## 3. Automated Scheduling

### Set Up Nightly Updates
1. Run `schedule_nightly_update.bat` as Administrator
2. This creates a Windows Task that runs at 2:00 AM daily

### Manual Update
```bash
python nightly_etf_update.py
```

### Check Schedule Status
```bash
schtasks /query /tn "ETF_Nightly_Update"
```

### Run Scheduled Task Manually
```bash
schtasks /run /tn "ETF_Nightly_Update"
```

## 4. Data Management

### Load All CSV Files to QuestDB
```bash
python questdb_manager.py
```

### Update Single Symbol
```python
from nightly_etf_update import NightlyETFUpdater

updater = NightlyETFUpdater()
df = updater.fetch_latest_data('SPY', days=5)
updater.update_questdb(df, 'SPY')
```

## 5. Available Data

### ETF
- **SPY**: S&P 500 ETF (0.0945% expense ratio)

### Stocks
- **Tech Giants**: AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA
- **Finance**: JPM, V
- **Healthcare**: JNJ, UNH

### Data Range
- 5 years of daily OHLCV data (2020-2025)
- Updates nightly at 2:00 AM

## 6. Monitoring

### Check Logs
```bash
# View today's log
type logs\etf_update_20250907.log

# View scheduler log
type logs\scheduler.log
```

### Database Size
```sql
-- Check table size
SELECT COUNT(*) as total_records,
       COUNT(DISTINCT symbol) as symbols,
       MIN(date) as earliest,
       MAX(date) as latest
FROM ohlcv1d;
```

## 7. Troubleshooting

### QuestDB Won't Start
1. Check if port 9000 is in use: `netstat -an | findstr :9000`
2. Kill existing process: `taskkill /F /IM questdb.exe`
3. Run as Administrator: `start_questdb.bat`

### Data Not Loading
1. Check QuestDB is running: http://localhost:9000
2. Verify CSV files exist: `dir data\csv\*.csv`
3. Run manual load: `python questdb_manager.py`

### API Errors
1. Check API key in `.env` file
2. Test connection: `python fmp_final_processor.py`
3. Check logs in `logs` folder

## 8. Quick Commands

```bash
# Start QuestDB
start_questdb.bat

# Load all data
python questdb_manager.py

# Run nightly update
python nightly_etf_update.py

# Schedule automation
schedule_nightly_update.bat

# Process new data from FMP
python fmp_final_processor.py
```

## 9. ETF Analysis Priority

Based on volatility (high) and expense ratio (low):

1. **SPY** - Best available ETF
   - Volatility: 17.45%
   - Expense: 0.0945%
   - Highly liquid

For higher volatility exposure, consider stocks:
1. **TSLA**: 61.72% volatility
2. **NVDA**: 52.38% volatility
3. **META**: 43.70% volatility