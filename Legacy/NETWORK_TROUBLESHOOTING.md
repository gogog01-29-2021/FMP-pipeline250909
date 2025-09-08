# ETF Data Pipeline - Network Troubleshooting Guide

## 🚨 CURRENT PROBLEM
Your network/firewall is blocking API calls to Financial Modeling Prep (FMP).
The API call to `https://financialmodelingprep.com/api/v3/symbol/SPY` is timing out.

## 🔍 WHAT YOU NEED TO DO (Step by Step)

### **STEP 1: Identify the Network Issue**

Run this diagnostic command:
```bash
python -c "
import socket
import requests
import time

print('=== NETWORK DIAGNOSTICS ===')

# Test 1: Basic internet
try:
    socket.create_connection(('8.8.8.8', 53), 5)
    print('✓ Internet connection: OK')
except:
    print('✗ Internet connection: FAILED')

# Test 2: DNS resolution
try:
    ip = socket.gethostbyname('financialmodelingprep.com')
    print(f'✓ DNS resolution: {ip}')
except Exception as e:
    print(f'✗ DNS resolution: FAILED - {e}')

# Test 3: HTTPS access
try:
    response = requests.get('https://financialmodelingprep.com', timeout=5)
    print(f'✓ HTTPS access: {response.status_code}')
except Exception as e:
    print(f'✗ HTTPS access: FAILED - {e}')

# Test 4: API endpoint
try:
    response = requests.get('https://financialmodelingprep.com/api/v3/symbol/SPY?apikey=X0EbPVay8gnDiRxEmgulxY8Y0pTGK3Om', timeout=10)
    print(f'✓ FMP API: {response.status_code}')
    if response.status_code == 200:
        print('✓ API KEY WORKING!')
    else:
        print(f'Response: {response.text[:100]}')
except Exception as e:
    print(f'✗ FMP API: FAILED - {e}')
"
```

### **STEP 2: Possible Network Issues & Solutions**

#### **A. Corporate Firewall (Most Likely)**
**Symptoms**: 
- Internet browsing works
- API calls timeout or fail
- IT department controls network

**Solutions**:
1. **Contact IT Department**:
   - Ask them to whitelist `financialmodelingprep.com`
   - Request access to financial APIs for development
   - Provide business justification

2. **Use Different Network**:
   - Mobile hotspot from your phone
   - Home network (if working from home)
   - Public WiFi (carefully)

#### **B. Proxy Server Required**
**Symptoms**:
- Corporate environment
- Browser works but Python scripts don't
- Proxy settings in IE/Windows

**Solutions**:
1. **Auto-detect proxy**:
```python
import requests
proxies = requests.utils.getproxies()
print(f"Detected proxies: {proxies}")
```

2. **Manually set proxy in code**:
```python
proxies = {
    'http': 'http://proxy.company.com:8080',
    'https': 'http://proxy.company.com:8080'
}
response = requests.get(url, proxies=proxies)
```

#### **C. SSL Certificate Issues**
**Symptoms**:
- SSL verification errors
- Certificate authority problems

**Solutions**:
1. **Disable SSL verification** (temporary):
```python
import requests
import urllib3
urllib3.disable_warnings()
response = requests.get(url, verify=False)
```

#### **D. Windows Firewall**
**Symptoms**:
- Python processes blocked
- Local firewall rules

**Solutions**:
1. **Run as Administrator**:
   - Right-click PowerShell → "Run as Administrator"
   - Add firewall exception for Python

2. **Add firewall rule**:
```powershell
New-NetFirewallRule -DisplayName "Python FMP API" -Direction Outbound -Program "python.exe" -Action Allow
```

### **STEP 3: Quick Network Tests**

Run these one by one to identify the issue:

**Test Basic Connectivity**:
```bash
ping 8.8.8.8
ping financialmodelingprep.com
```

**Test with curl** (if available):
```bash
curl -I https://financialmodelingprep.com
curl "https://financialmodelingprep.com/api/v3/symbol/SPY?apikey=X0EbPVay8gnDiRxEmgulxY8Y0pTGK3Om"
```

**Test with PowerShell**:
```powershell
Invoke-RestMethod -Uri "https://financialmodelingprep.com/api/v3/symbol/SPY?apikey=X0EbPVay8gnDiRxEmgulxY8Y0pTGK3Om"
```

### **STEP 4: Alternative Solutions if Network Can't Be Fixed**

#### **Option 1: Different API Provider**
- Alpha Vantage (free tier)
- Yahoo Finance (yfinance library)
- Quandl/NASDAQ Data Link

#### **Option 2: VPN Solution**
- Use company-approved VPN
- Connect to different geographic location

#### **Option 3: Different Environment**
- Cloud environment (AWS, Azure, Google Cloud)
- Different computer/network
- Docker container with different network settings

## 🎯 NEXT STEPS FOR YOU

1. **Run the diagnostic command above** to see exactly what's failing
2. **Check with IT department** about API access
3. **Try from different network** (mobile hotspot)
4. **Report back the diagnostic results** so we can create a targeted fix

## 📞 Questions for Your IT Department

"Hi, I'm developing a financial data analysis application that needs to access the Financial Modeling Prep API at `financialmodelingprep.com`. The API calls are currently being blocked by our firewall. Could you please:

1. Whitelist the domain `financialmodelingprep.com` for HTTPS traffic
2. Allow outbound connections to port 443 for this domain
3. Let me know if there are any proxy settings I need to configure

This is for legitimate business/educational purposes for ETF data analysis."

## 🔧 Immediate Workaround

If you can't wait for network fixes, we can:
1. Export data from your browser (manual download)
2. Use a different data source that works
3. Create a web-scraping solution that mimics browser behavior

**The key is identifying exactly WHERE the network is blocking the requests so we can fix it properly!**
