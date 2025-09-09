# ETF & Stock Market Data Pipeline

A comprehensive financial data pipeline that fetches, processes, and stores ETF and S&P 500 stock data using the Financial Modeling Prep (FMP) API, with storage in multiple formats including CSV, Parquet, and QuestDB time-series database.

## 🚀 Quick Reference

```bash
# Project Location
C:\etf_data_project

# Activate Conda Environment (ALWAYS DO THIS FIRST!)
conda activate etf_pipeline_working2509

# Run Data Update
python premium_unified_processor.py

# Check Scheduled Task
schtasks /query /tn "ETF_Nightly_Update"
```

⚠️ **IMPORTANT**: Windows Task Scheduler only runs when computer is ON. See [Automated Scheduling](#automated-scheduling) section for solutions.

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
- Python 3.8+ (Anaconda recommended)
- Windows 10/11 (for Task Scheduler)
- FMP API Key (Premium plan for 1-minute data)
- ~2GB disk space for data storage

### Complete Setup for New Computer

#### Step 1: Install Anaconda
1. Download Anaconda from: https://www.anaconda.com/products/individual
2. Install with default settings
3. Open Anaconda Prompt (not regular command prompt)

#### Step 2: Create Conda Environment (etf_pipeline_working2509)
```bash
# Create new conda environment with Python 3.9
conda create -n etf_pipeline_working2509 python=3.9 -y

# Activate the environment
conda activate etf_pipeline_working2509

# Verify environment is active (should show etf_pipeline_working2509)
conda info --envs
```

#### Step 3: Clone/Copy Project
```bash
# Option A: Clone from git (if available)
git clone https://github.com/yourusername/etf_data_project.git
cd etf_data_project

# Option B: Copy existing folder
# Copy your etf_data_project folder to C:\etf_data_project
```

#### Step 4: Install Dependencies
```bash
# Make sure you're in the project directory
cd C:\etf_data_project

# Activate conda environment if not already active
conda activate etf_pipeline_working2509

# Install packages using conda where possible (more stable)
conda install pandas numpy requests python-dotenv psycopg2 -y

# Install remaining packages with pip
pip install pyarrow schedule financialmodelingprep

# Verify installations
python -c "import pandas, numpy, requests, dotenv, psycopg2, pyarrow, schedule; print('All packages installed successfully!')"
```

#### Step 5: Configure API Key
```bash
# Create .env file from example
copy .env.example .env

# Edit .env file and add your FMP API key
notepad .env
# Add: FMP_API_KEY=your_actual_api_key_here
```

#### Step 6: Install and Configure QuestDB
```bash
# Install QuestDB (one-time setup)
powershell -ExecutionPolicy Bypass -File scripts\install_questdb.ps1

# Start QuestDB
scripts\start_questdb.bat

# Verify QuestDB is running
# Open browser: http://localhost:9000
```

#### Step 7: Initial Data Load
```bash
# Test the setup with a single update
python premium_unified_processor.py

# Verify data was loaded
python check_questdb_data.py
```

#### Step 8: Setup Automated Scheduling (Optional)
```bash
# Create Windows Task Scheduler task
powershell -ExecutionPolicy Bypass -File scripts\setup_auto_schedule.ps1
```

## Usage

### Important: Always Activate Conda Environment First
```bash
# EVERY TIME you open a new terminal/Anaconda Prompt:
conda activate etf_pipeline_working2509

# Navigate to project directory
cd C:\etf_data_project
```

### Manual Data Update
```bash
# Make sure conda environment is activated first!
conda activate etf_pipeline_working2509

# Fetch all ETF and stock data
python premium_unified_processor.py

# Check data in QuestDB
python check_questdb_data.py
```

### Automated Scheduling

⚠️ **IMPORTANT: Computer Must Be On** ⚠️
Windows Task Scheduler only runs when your computer is powered on. If your computer is off at 2:00 AM, the task will NOT run. Options:
1. **Keep computer on** - Leave your computer running overnight
2. **Wake on schedule** - Configure Windows to wake from sleep (see below)
3. **Run on startup** - Configure task to run when computer starts if missed
4. **Use cloud server** - Deploy to a cloud VPS for 24/7 operation

#### Setting Up Automated Daily Updates

The system includes Windows Task Scheduler integration for automatic daily updates at 2:00 AM:

##### Method 1: PowerShell Script (Recommended)
```bash
# Run the setup script to create the scheduled task
powershell -ExecutionPolicy Bypass -File scripts\setup_auto_schedule.ps1
```

This script will:
- Create a Windows Task called "ETF_Nightly_Update"
- Schedule it to run daily at 2:00 AM
- Configure it to run with highest privileges
- Set up automatic retry on failure
- Log output to `logs\scheduled_update.log`

##### Method 2: Manual Task Scheduler Setup
1. Open Task Scheduler (`taskschd.msc`)
2. Click "Create Basic Task"
3. Name: "ETF_Nightly_Update"
4. Trigger: Daily at 2:00 AM
5. Action: Start a program
6. Program: `C:\etf_data_project\run_network_resilient.bat`
7. Start in: `C:\etf_data_project`

##### Method 3: Batch File (Legacy)
```bash
# Alternative method using batch file
schedule_nightly_update.bat
```

#### Managing the Scheduled Task

```bash
# Check if task is running
schtasks /query /tn "ETF_Nightly_Update"

# Run the task immediately (for testing)
schtasks /run /tn "ETF_Nightly_Update"

# Disable the task temporarily
schtasks /change /tn "ETF_Nightly_Update" /disable

# Enable the task
schtasks /change /tn "ETF_Nightly_Update" /enable

# Delete the task
schtasks /delete /tn "ETF_Nightly_Update" /f
```

#### Verifying Task Execution

1. **Check Task History**:
   - Open Task Scheduler
   - Find "ETF_Nightly_Update" in the Task Scheduler Library
   - Check the "History" tab for execution details

2. **Check Log Files**:
   ```bash
   # View the latest log
   type logs\scheduled_update.log
   
   # Check for today's data updates
   dir data\daily\csv\*.csv | findstr /i today
   ```

3. **Verify in QuestDB**:
   ```sql
   -- Check latest data timestamps
   SELECT symbol, MAX(date) as last_update 
   FROM ohlcv1d 
   GROUP BY symbol 
   ORDER BY last_update DESC;
   ```

#### Customizing the Schedule

To change the schedule time or frequency, edit `scripts\setup_auto_schedule.ps1` and modify:
- `$trigger.At` for the time (default: "2:00AM")
- `$trigger.Daily` for frequency (can change to Weekly, Monthly, etc.)

Then re-run the setup script to apply changes.

#### Configuring Task to Run When Computer is Off

##### Option 1: Run Missed Task at Startup
```bash
# Configure task to run at startup if missed
schtasks /change /tn "ETF_Nightly_Update" /RI 1
```

##### Option 2: Wake Computer from Sleep
1. Open Task Scheduler
2. Find "ETF_Nightly_Update"
3. Right-click → Properties
4. Go to "Conditions" tab
5. Check "Wake the computer to run this task"
6. Ensure "Start only if computer is on AC power" is unchecked for laptops

##### Option 3: Configure Power Settings
```bash
# Allow wake timers in Windows
powercfg /waketimers enable

# Check current power plan
powercfg /query SCHEME_CURRENT SUB_SLEEP STANDBYIDLE
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
scripts\start_questdb.bat

# Check if QuestDB is running
netstat -an | findstr :8812
```

2. **API Rate Limits**:
- Premium plan: 300 requests/minute
- The processor includes automatic rate limiting

3. **Data Not Updating**:
```bash
# Check Task Scheduler status
schtasks /query /tn "ETF_Nightly_Update"

# Check last run time
schtasks /query /tn "ETF_Nightly_Update" /v | findstr "Last Run"
```

4. **Conda Environment Issues**:
```bash
# List all environments
conda env list

# If environment missing, recreate from yml file
conda env create -f environment.yml

# Activate environment
conda activate etf_pipeline_working2509
```

5. **Module Import Errors**:
```bash
# Reinstall all dependencies
conda activate etf_pipeline_working2509
pip install -r requirements.txt --force-reinstall
```

6. **Task Scheduler Not Running (Computer Was Off)**:
```bash
# Run the task manually to catch up
schtasks /run /tn "ETF_Nightly_Update"

# Or run the script directly
conda activate etf_pipeline_working2509
python premium_unified_processor.py
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