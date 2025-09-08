# 🚀 ETF DATA PROCESSING PROJECT - READY TO RUN!

## ✅ WHAT I'VE BUILT FOR YOU

I've created a complete ETF data analysis system that:

### 📊 **Fetches 5 Years of Price Data** for:
- **SPY** (S&P 500 Index ETF)
- **QQQ** (Nasdaq 100 Index ETF) 
- **IWM** (Russell 2000 Index ETF)
- **VXX** (VIX Volatility Index ETF)

### 💾 **Saves Data in Multiple Formats:**
- **Parquet files** (`data/parquet/` folder)
- **CSV files** (`data/csv/` folder)
- **QuestDB table** called `ohlcv1d`

### 📈 **Analyzes ETFs by:**
- **Higher Volatility** (better for trading)
- **Lower Expense Ratios** (better for fees)

---

## 🏃 HOW TO RUN IT (3 Options)

### **Option 1: SUPER EASY - Double-click the batch file**
```
Just double-click: run_etf_analysis.bat
```

### **Option 2: Command Line**
```powershell
cd "c:\etf_data_project"
python run_analysis.py
```

### **Option 3: Manual Steps**
```powershell
cd "c:\etf_data_project"
python etf_fmp_processor.py
```

---

## ⚠️ BEFORE YOU RUN - 2 REQUIRED STEPS:

### **Step 1: Get Your FREE FMP API Key**
1. Go to: https://financialmodelingprep.com/
2. Sign up for free account
3. Get your API key
4. Open `.env` file in this project
5. Replace `your_actual_fmp_api_key_here` with your real key

### **Step 2: Download & Start QuestDB**
1. Download QuestDB from: https://questdb.io/get-questdb/
2. Extract the files
3. Run `questdb.exe` (Windows) 
4. QuestDB will be available at: http://localhost:9000

---

## 📁 FILES I CREATED

| File | Purpose |
|------|---------|
| `etf_fmp_processor.py` | 🎯 **Main processor** - fetches data, calculates volatility, saves files |
| `setup_questdb.py` | 🗄️ Creates the QuestDB table schema |
| `run_analysis.py` | 🤖 **Complete pipeline** - runs everything automatically |
| `test_setup.py` | 🧪 Tests if everything is configured correctly |
| `run_etf_analysis.bat` | 🖱️ **Windows batch file** - just double-click to run |
| `test_fmp_api.py` | ✅ Simple test for your FMP API key |
| `README_ETF_ANALYSIS.md` | 📖 Complete documentation |

---

## 🎯 WHAT YOU'LL GET

### **Data Files:**
- `data/parquet/SPY_5y_ohlcv.parquet` (and QQQ, IWM, VXX)
- `data/csv/SPY_5y_ohlcv.csv` (and QQQ, IWM, VXX)
- `data/etf_analysis_results.csv` (summary with volatility rankings)

### **QuestDB Table:**
- Table name: `ohlcv1d`
- Contains OHLCV data for all ETFs
- Queryable via web interface at http://localhost:9000

### **Analysis Results:**
ETFs ranked by:
1. **Higher Volatility first** ⬆️
2. **Lower expense ratio first** ⬇️

---

## 🧪 QUICK TEST

Want to test if everything works? Run:
```powershell
cd "c:\etf_data_project"
python test_setup.py
```

This checks:
- ✅ Python packages installed
- ✅ Environment configured 
- ✅ QuestDB connection
- ✅ FMP API access

---

## 📊 SAMPLE QUESTDB QUERIES

Once data is loaded, try these queries in QuestDB web console:

```sql
-- View latest prices for all ETFs
SELECT * FROM ohlcv1d ORDER BY ts DESC LIMIT 20;

-- Get SPY data only
SELECT * FROM ohlcv1d WHERE symbol = 'SPY' ORDER BY ts DESC LIMIT 10;

-- Calculate average volumes
SELECT symbol, AVG(volume) as avg_volume 
FROM ohlcv1d 
GROUP BY symbol 
ORDER BY avg_volume DESC;
```

---

## ❓ TROUBLESHOOTING

**"FMP_API_KEY not configured"**
→ Edit `.env` file and add your real API key

**"QuestDB not running"**
→ Download QuestDB and run `questdb.exe`

**"Module not found"**
→ Run: `pip install -r requirements.txt`

---

## 🎉 READY TO GO!

**Your project is 100% ready to run!** 

Just:
1. ✅ Add your FMP API key to `.env` file  
2. ✅ Start QuestDB
3. ✅ Double-click `run_etf_analysis.bat` OR run `python run_analysis.py`

The system will automatically:
- Fetch 5 years of OHLCV data
- Calculate volatility metrics  
- Save data in Parquet and CSV formats
- Populate QuestDB with structured data
- Generate analysis ranking ETFs by volatility and fees

**Total processing time: ~2-3 minutes** (depending on API response times)

🚀 **Ready to analyze some ETF data?**
