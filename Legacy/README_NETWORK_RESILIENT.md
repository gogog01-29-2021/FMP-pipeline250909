# ETF Data Processor - Network Resilient Version

This enhanced version of the ETF Data Processor is designed to work robustly even in environments with strict network controls, corporate firewalls, proxies, and SSL certificate issues.

## Features

### Network Resilience
- **Multiple HTTP Client Methods**: Tests requests, urllib3, and urllib standard library
- **SSL Configuration Options**: Default SSL, disabled SSL, custom SSL contexts
- **Proxy Auto-Detection**: Automatically detects and uses system proxy settings
- **Retry Logic**: Implements exponential backoff and retry strategies
- **Firewall Compatibility**: Works with Windows Firewall and corporate network restrictions

### Data Processing
- **5 Years Historical Data**: Fetches 5 years of price data for major ETFs
- **Multiple Data Formats**: Saves data in both Parquet and CSV formats
- **QuestDB Integration**: Loads data into QuestDB for time-series analysis
- **ETF Ranking**: Ranks ETFs by volatility and other metrics
- **Comprehensive Error Handling**: Graceful failure handling and detailed logging

## Quick Start

### Method 1: Run Everything (Recommended)
```batch
run_network_resilient.bat
```
This will:
1. Run network diagnostics
2. Configure Windows networking (requires admin rights)
3. Process ETF data using the most resilient method

### Method 2: Step by Step

1. **Test Network Connectivity**:
   ```bash
   python network_diagnostics.py
   ```

2. **Configure Network (Run as Administrator)**:
   ```powershell
   PowerShell -ExecutionPolicy Bypass -File configure_network.ps1 -Force
   ```

3. **Process ETF Data**:
   ```bash
   python etf_network_resilient.py
   ```

## Files Overview

### Core Processing Files
- `etf_network_resilient.py` - Main processor with maximum network resilience
- `network_diagnostics.py` - Comprehensive network testing and diagnostics
- `configure_network.ps1` - PowerShell script to optimize Windows networking

### Supporting Files
- `run_network_resilient.bat` - One-click execution script
- `.env` - Configuration file (copy from .env.example)
- `requirements.txt` - Python dependencies

### Legacy/Backup Files
- `etf_fmp_processor.py` - Original FMP processor
- `etf_yfinance_backup.py` - Backup processor using Yahoo Finance
- `etf_downloader.py` - Pure urllib-based downloader

## Network Troubleshooting

### Common Issues and Solutions

#### Issue: "SSL Certificate Verification Failed"
**Solutions**:
1. Run `configure_network.ps1` as Administrator
2. Use the SSL-disabled methods (automatically tried by resilient processor)
3. Update certificates: `pip install --upgrade certifi`

#### Issue: "Connection Timeout" or "No Route to Host"
**Solutions**:
1. Check if behind corporate firewall/proxy
2. Run network diagnostics to identify working methods
3. Configure proxy settings in environment variables
4. Run as Administrator for firewall rule creation

#### Issue: "API Key Invalid" but key is correct
**Solutions**:
1. Requests may not be reaching FMP servers
2. Check corporate firewall logs
3. Try different HTTP client methods
4. Use yfinance backup: `python etf_yfinance_backup.py`

#### Issue: "403 Forbidden" or "401 Unauthorized"
**Solutions**:
1. May be blocked by corporate security
2. Try different User-Agent strings (automatically tried)
3. Use VPN or different network connection
4. Contact IT department about API access

### Network Configuration Details

The `configure_network.ps1` script (run as Administrator) will:
- Test network connectivity to various endpoints
- Detect and display proxy settings
- Configure DNS to use reliable servers (8.8.8.8, 1.1.1.1)
- Create Windows Firewall rules for Python processes
- Set environment variables for SSL/proxy handling
- Enable TLS 1.2/1.3 protocols
- Clear DNS cache

### Diagnostic Information

The `network_diagnostics.py` script provides:
- Basic internet connectivity tests
- DNS resolution verification
- Proxy setting detection
- SSL configuration testing
- Multiple HTTP client method testing
- System configuration analysis
- Detailed JSON report generation

## Environment Variables

Create a `.env` file with:
```
FMP_API_KEY=your_actual_api_key_here
DATA_DIR=./data
PARQUET_DIR=./data/parquet
CSV_DIR=./data/csv

# Optional proxy settings
HTTP_PROXY=http://proxy.company.com:8080
HTTPS_PROXY=http://proxy.company.com:8080

# Optional SSL settings
PYTHONHTTPSVERIFY=0
SSL_VERIFY=false
```

## Expected Output

### Successful Run
```
ETF Data Processor - Network Resilient Version
================================================

Processing SPY (SPDR S&P 500 ETF Trust)
✓ Got historical data: 1,250 records
✓ Calculated metrics: Volatility 0.1234
✓ Saved files: Parquet and CSV
✓ Loaded to QuestDB: 1,250 records

Processing QQQ (Invesco QQQ Trust ETF)
...

PROCESSING SUMMARY
==================
Processing completed: 4/4 ETFs successful

ETF RANKING BY VOLATILITY:
VXX  | 0.8765 | -0.2345 | iPath Series B S&P 500 VIX Short-Term Futures ETN
IWM  | 0.2345 | 0.1234 | iShares Russell 2000 ETF
QQQ  | 0.1987 | 0.2876 | Invesco QQQ Trust ETF
SPY  | 0.1654 | 0.1987 | SPDR S&P 500 ETF Trust
```

### Output Files
- `./data/parquet/SPY_historical_data_YYYYMMDD_HHMMSS.parquet`
- `./data/csv/SPY_historical_data_YYYYMMDD_HHMMSS.csv`
- `./data/etf_analysis_summary_YYYYMMDD_HHMMSS.csv`
- `./network_diagnostics_report.json`

## QuestDB Integration

After processing, data is available in QuestDB at `http://localhost:9000`

Example queries:
```sql
-- View all ETF data
SELECT * FROM etf_prices ORDER BY timestamp DESC LIMIT 100;

-- Get latest prices for all ETFs
SELECT symbol, name, close, volume, timestamp 
FROM etf_prices 
WHERE timestamp = (SELECT MAX(timestamp) FROM etf_prices WHERE symbol = etf_prices.symbol);

-- Calculate volatility for each ETF
SELECT symbol, 
       stddev(close) as volatility,
       avg(volume) as avg_volume
FROM etf_prices 
WHERE timestamp > '2023-01-01'
GROUP BY symbol
ORDER BY volatility DESC;
```

## Troubleshooting Network Issues

### If Everything Fails
1. **Check Internet Connection**: Can you browse the web normally?
2. **Check Corporate Restrictions**: Are you behind a corporate firewall?
3. **Try Different Network**: Mobile hotspot, home network, etc.
4. **Use Backup Method**: Run `python etf_yfinance_backup.py`
5. **Contact IT Support**: May need API access whitelisted

### Advanced Debugging
1. **Enable Verbose Logging**: Check the console output for detailed error messages
2. **Check Diagnostics Report**: Review `network_diagnostics_report.json`
3. **Test Individual Components**: Run each script separately
4. **Check QuestDB**: Ensure QuestDB is running at `http://localhost:9000`

## Support

If you continue to experience network issues:
1. Review the network diagnostics report
2. Check with your network administrator about API access
3. Consider using the yfinance backup version
4. Try running from a different network environment

The system is designed to be as resilient as possible, but some corporate networks have very strict policies that may prevent any external API access.
