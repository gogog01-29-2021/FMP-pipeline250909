# ETF & Stock Market Data Pipeline

A comprehensive financial data pipeline that fetches, processes, and stores ETF and S&P 500 stock data using the Financial Modeling Prep (FMP) API, with storage in multiple formats including CSV, Parquet, and QuestDB time-series database.

## Features

- **Multi-format Data Storage**: CSV, Parquet, and QuestDB
- **Automated Daily Updates**: Windows Task Scheduler integration for 2 AM updates
- **Premium FMP API Integration**: Access to real-time and historical market data
- **ETF Analysis**: Automated volatility and expense ratio analysis
- **1-Minute Intraday Data**: High-frequency data for S&P 500 stocks
- **5-Year Historical Data**: Comprehensive historical analysis capability

## Data Coverage

### ETFs Tracked
| Symbol | Name | Volatility | Expense Ratio |
|--------|------|------------|---------------|
| SPY | SPDR S&P 500 ETF | 1.17% | 0.0945% |
| QQQ | Invesco QQQ Trust | 1.60% | 0.20% |
| IWM | iShares Russell 2000 | 2.16% | 0.19% |
| VXX | iPath S&P 500 VIX | 41.71% | N/A |

### S&P 500 Stocks
- Daily OHLCV data: 11 major stocks (AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA, JPM, JNJ, UNH, V)
- 1-Minute intraday data: Configurable for all S&P 500 constituents
- Historical depth: 5 years of daily data, 30-day rolling window for 1-minute data

## Quick Start

### Prerequisites
- Python 3.8+
- Windows 10/11 (for Task Scheduler)
- FMP API Key (Premium plan for 1-minute data)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/etf_data_project.git
cd etf_data_project
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure API key:
```bash
copy .env.example .env
# Edit .env and add your FMP_API_KEY
```

4. Install QuestDB:
```bash
powershell -ExecutionPolicy Bypass -File install_questdb.ps1
```

## Usage

### Manual Data Update
```bash
# Fetch all ETF and stock data
python premium_unified_processor.py

# Check data in QuestDB
python check_questdb_data.py
```

### Automated Scheduling
The system includes Windows Task Scheduler integration for automatic daily updates at 2:00 AM:

```bash
# Create scheduled task
powershell -ExecutionPolicy Bypass -File create_schedule.ps1

# Or manually with batch file
schedule_nightly_update.bat
```

### SQL Query Examples
```sql
-- Get latest ETF prices
SELECT * FROM ohlcv1d 
WHERE symbol IN ('SPY', 'QQQ', 'IWM', 'VXX') 
ORDER BY date DESC LIMIT 20;

-- Get 1-minute data
SELECT * FROM ohlcv1min 
WHERE symbol = 'AAPL' 
AND datetime >= '2025-09-05 09:30:00'
AND datetime <= '2025-09-05 10:00:00';
```

## Data Storage Locations

All data is saved in THREE formats for maximum flexibility:

### 1. CSV Files
- **Daily**: `C:\etf_data_project\data\daily\csv\`
- **1-Minute**: `C:\etf_data_project\data\1min\csv\`

### 2. Parquet Files
- **Daily**: `C:\etf_data_project\data\daily\parquet\`
- **1-Minute**: `C:\etf_data_project\data\1min\parquet\`

### 3. QuestDB Database
- **Access**: PostgreSQL protocol on `localhost:8812`
- **Web Console**: http://localhost:9000
- **Tables**:
  - `ohlcv1d`: Daily OHLCV data (15,000+ records)
  - `ohlcv1min`: 1-minute intraday data (2,000+ records/day)

## Project Structure

```
etf_data_project/
├── data/
│   ├── daily/
│   │   ├── csv/                    # Daily OHLCV CSV files
│   │   └── parquet/                # Daily OHLCV Parquet files
│   └── 1min/
│       ├── csv/                    # 1-minute CSV files
│       └── parquet/                # 1-minute Parquet files
├── Legacy/                         # Archive of old scripts
├── premium_unified_processor.py    # Main data processor
├── questdb_manager.py              # QuestDB operations
├── check_questdb_data.py          # Data verification
├── questdb_cleanup_fix.py         # Database maintenance
├── schedule_nightly_update.bat    # Windows scheduler
├── .env                           # API configuration
└── README.md                      # This file
```

## Configuration Files

### .env File
```
FMP_API_KEY=your_api_key_here
```

### requirements.txt
```
pandas>=2.0.0
numpy>=1.24.0
requests>=2.31.0
python-dotenv>=1.0.0
pyarrow>=12.0.0
psycopg2-binary>=2.9.6
schedule>=1.2.0
```

## Automation Features

### Daily Update Schedule
- **Time**: 2:00 AM daily
- **Actions**:
  1. Fetch latest daily OHLCV for all ETFs
  2. Update S&P 500 stock data
  3. Fetch 1-minute data for configured stocks
  4. Store in all three formats (CSV, Parquet, QuestDB)
  5. Clean up old 1-minute data (30-day retention)

### Monitoring
- Logs stored in `logs/` directory
- Daily processing reports
- Error notifications in log files

## API Endpoints Used

- **Daily Data**: `/api/v3/historical-price-full/{symbol}`
- **1-Minute Data**: `/api/v3/historical-chart/1min/{symbol}`
- **Company Profiles**: `/api/v3/profile/{symbol}`

## Performance Metrics

- Daily data fetch: ~2 seconds per symbol
- 1-minute data fetch: ~5 seconds per symbol per day
- QuestDB insertion: 10,000+ records/second
- Storage efficiency: Parquet files are ~70% smaller than CSV

## Troubleshooting

### Common Issues

1. **QuestDB Connection Error**:
```bash
# Start QuestDB
start_questdb.bat
```

2. **API Rate Limits**:
- Premium plan: 300 requests/minute
- The processor includes automatic rate limiting

3. **Data Not Updating**:
```bash
# Check Task Scheduler status
schtasks /query /tn "ETF_Nightly_Update"
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first.

## License

MIT License - see LICENSE file for details

## Support

For issues or questions:
- Open an issue on GitHub
- Check logs in `logs/` directory
- Verify API key in `.env` file

## Acknowledgments

- Financial Modeling Prep for market data API
- QuestDB for time-series database
- pandas and pyarrow for data processing