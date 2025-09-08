# Data Location Guide - ETF Data Project

## Project Location
**Main Folder**: `C:\etf_data_project`

## Data Storage Locations

### 1. CSV Files 📄
```
C:\etf_data_project\data\
├── daily\
│   └── csv\
│       ├── SPY_daily_20250907.csv
│       ├── QQQ_daily_20250907.csv
│       ├── IWM_daily_20250907.csv
│       └── VXX_daily_20250907.csv
└── 1min\
    └── csv\
        ├── AAPL_1min_20250908.csv
        ├── MSFT_1min_20250908.csv
        └── (other stocks...)
```

### 2. Parquet Files 📦
```
C:\etf_data_project\data\
├── daily\
│   └── parquet\
│       ├── SPY_daily_20250907.parquet
│       ├── QQQ_daily_20250907.parquet
│       ├── IWM_daily_20250907.parquet
│       └── VXX_daily_20250907.parquet
└── 1min\
    └── parquet\
        ├── AAPL_1min_20250908.parquet
        ├── MSFT_1min_20250908.parquet
        └── (other stocks...)
```

### 3. QuestDB Storage 🗄️
- **Database Location**: QuestDB internal storage (managed by QuestDB)
- **Access Method**: 
  - PostgreSQL protocol: `localhost:8812`
  - Web Console: http://localhost:9000
- **Tables**:
  - `ohlcv1d` - Daily data for all ETFs and stocks
  - `ohlcv1min` - 1-minute intraday data

## How to Access Your Data

### Option 1: File System (CSV/Parquet)
```python
import pandas as pd

# Read CSV
df_csv = pd.read_csv(r'C:\etf_data_project\data\daily\csv\SPY_daily_20250907.csv')

# Read Parquet
df_parquet = pd.read_parquet(r'C:\etf_data_project\data\daily\parquet\SPY_daily_20250907.parquet')
```

### Option 2: QuestDB
```python
import psycopg2
import pandas as pd

# Connect to QuestDB
conn = psycopg2.connect(
    host='localhost',
    port=8812,
    database='qdb',
    user='admin',
    password='quest'
)

# Query daily data
query = "SELECT * FROM ohlcv1d WHERE symbol = 'SPY' ORDER BY date DESC LIMIT 100"
df = pd.read_sql(query, conn)
```

### Option 3: Web Browser (QuestDB Console)
1. Open browser: http://localhost:9000
2. Click 'Console' tab
3. Run SQL queries directly

## Data Summary

### Daily Data (as of 2025-09-07)
| Format | Location | ETFs | Files |
|--------|----------|------|-------|
| CSV | `data\daily\csv\` | SPY, QQQ, IWM, VXX | 4 files |
| Parquet | `data\daily\parquet\` | SPY, QQQ, IWM, VXX | 4 files |
| QuestDB | `ohlcv1d` table | SPY, QQQ, IWM, VXX + 11 stocks | 15,108 records |

### 1-Minute Data (as of 2025-09-08)
| Format | Location | Stocks | Files |
|--------|----------|--------|-------|
| CSV | `data\1min\csv\` | AAPL, MSFT, etc. | 12+ files |
| Parquet | `data\1min\parquet\` | AAPL, MSFT, etc. | 12+ files |
| QuestDB | `ohlcv1min` table | AAPL, MSFT | 2,340 records |

## File Naming Convention
- Daily: `{SYMBOL}_daily_{YYYYMMDD}.{format}`
- 1-Minute: `{SYMBOL}_1min_{YYYYMMDD}.{format}`

## Quick Commands

### Check what's in each folder:
```bash
# From C:\etf_data_project
dir data\daily\csv\
dir data\daily\parquet\
dir data\1min\csv\
dir data\1min\parquet\
```

### Check QuestDB data:
```bash
python check_questdb_data.py
```

### View specific file:
```bash
# View first few lines of CSV
type data\daily\csv\SPY_daily_20250907.csv | more
```

## Storage Space Used
- Daily CSV files: ~11 KB total (4 files)
- Daily Parquet files: ~48 KB total (4 files)
- 1-Minute CSV files: ~800 KB total
- 1-Minute Parquet files: ~500 KB total
- QuestDB: Managed internally

## YES, Your Data is Saved in THREE Places! ✅

1. **CSV Files** - Human-readable text format in `data\daily\csv\` and `data\1min\csv\`
2. **Parquet Files** - Compressed binary format in `data\daily\parquet\` and `data\1min\parquet\`
3. **QuestDB** - High-performance time-series database accessible via SQL

All three formats contain the same data, giving you flexibility in how you access and analyze it!