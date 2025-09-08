# ETF Data Update Schedule Status

## ✅ Current Status: ACTIVE

### 🕐 Schedule Details
- **Task Name**: ETF_Nightly_Update
- **Schedule**: Daily at 2:00 AM
- **Status**: Ready and scheduled
- **Created**: September 7, 2025

### 📊 Last Update Results (Sept 7, 2025)
Successfully updated 12 symbols with latest data:

| Symbol | Records | Latest Price | 30-Day Volatility | Avg Volume |
|--------|---------|--------------|-------------------|------------|
| SPY    | 1,254   | $647.24      | 10.96%           | 69.4M      |
| AAPL   | 1,254   | $239.69      | 28.10%           | 56.2M      |
| MSFT   | 1,254   | $495.00      | 19.84%           | 22.2M      |
| GOOGL  | 1,254   | $235.00      | 32.83%           | 35.7M      |
| AMZN   | 1,254   | $232.33      | 35.99%           | 42.3M      |
| NVDA   | 1,254   | $167.02      | 25.78%           | 172.8M     |
| META   | 1,254   | $752.45      | 41.87%           | 12.0M      |
| TSLA   | 1,254   | $350.84      | 35.97%           | 81.3M      |
| JPM    | 1,254   | $294.38      | 18.17%           | 7.4M       |
| JNJ    | 1,254   | $178.43      | 13.47%           | 8.3M       |
| V      | 1,254   | $343.22      | 16.62%           | 6.0M       |
| UNH    | 1,254   | $315.39      | 55.33%           | 19.5M      |

### 🔄 What Happens Every Night at 2:00 AM

1. **QuestDB Check**: Ensures QuestDB is running
2. **Data Fetch**: Downloads latest 5 days from FMP API
3. **Update Check**: Compares with existing data
4. **Insert New**: Adds only new records to avoid duplicates
5. **Report Generation**: Creates daily summary CSV
6. **Logging**: Records all activities in logs folder

### 📁 Output Locations

- **Daily Reports**: `data/daily_report_YYYYMMDD.csv`
- **Logs**: `logs/etf_update_YYYYMMDD.log`
- **QuestDB Data**: Table `ohlcv1d` in QuestDB
- **Parquet Files**: `data/parquet/`
- **CSV Files**: `data/csv/`

### 🛠️ Manual Controls

```bash
# Run update manually (test)
python nightly_etf_update.py

# Check task status
powershell "Get-ScheduledTask -TaskName ETF_Nightly_Update"

# Run scheduled task now
powershell "Start-ScheduledTask -TaskName ETF_Nightly_Update"

# Disable temporarily
powershell "Disable-ScheduledTask -TaskName ETF_Nightly_Update"

# Enable again
powershell "Enable-ScheduledTask -TaskName ETF_Nightly_Update"

# Delete task
powershell "Unregister-ScheduledTask -TaskName ETF_Nightly_Update"
```

### 📊 QuestDB Access

Open browser: http://localhost:9000

Quick queries:
```sql
-- Today's updates
SELECT * FROM ohlcv1d 
WHERE date >= dateadd('d', -1, now())
ORDER BY symbol, date DESC;

-- Check record counts
SELECT symbol, COUNT(*) as records, MAX(date) as last_update
FROM ohlcv1d
GROUP BY symbol;
```

### ⏰ Next Scheduled Run

**Tomorrow at 2:00 AM** (and every day after)

The system will automatically:
- Fetch latest market data
- Update QuestDB
- Generate reports
- Log all activities

### 🔍 Monitoring

Check if updates are working:
1. Look for new files in `data/` folder each morning
2. Check logs in `logs/` folder
3. Query QuestDB for latest dates
4. Review daily reports for anomalies

### 📈 Top Volatility Picks (Updated Daily)

Based on latest 30-day volatility:
1. **UNH**: 55.33% - Highest volatility
2. **META**: 41.87% - Tech giant volatility
3. **TSLA**: 35.97% - EV leader volatility
4. **AMZN**: 35.99% - E-commerce volatility
5. **SPY**: 10.96% - S&P 500 ETF (lowest expense ratio)