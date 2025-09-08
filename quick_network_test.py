import requests
import os
from dotenv import load_dotenv

load_dotenv()

print("=== FMP API Network Test ===")
api_key = os.getenv('FMP_API_KEY')
print(f"API Key loaded: {api_key[:15]}..." if api_key else "NO API KEY")

# Test 1: Basic connectivity
print("\n1. Testing basic connectivity...")
try:
    import socket
    socket.create_connection(('8.8.8.8', 53), timeout=5).close()
    print("✓ Internet connectivity: OK")
except Exception as e:
    print(f"✗ Internet connectivity: {e}")

# Test 2: FMP domain resolution
print("\n2. Testing FMP domain...")
try:
    import socket
    ip = socket.gethostbyname('financialmodelingprep.com')
    print(f"✓ FMP domain resolves to: {ip}")
except Exception as e:
    print(f"✗ FMP domain resolution failed: {e}")

# Test 3: HTTPS connection to FMP
print("\n3. Testing HTTPS connection to FMP...")
try:
    response = requests.get('https://financialmodelingprep.com', timeout=10)
    print(f"✓ HTTPS connection: {response.status_code}")
except Exception as e:
    print(f"✗ HTTPS connection failed: {e}")

# Test 4: FMP API call
print("\n4. Testing FMP API...")
if api_key and api_key != 'your_fmp_api_key_here':
    try:
        url = f"https://financialmodelingprep.com/api/v3/profile/SPY"
        params = {'apikey': api_key}
        response = requests.get(url, params=params, timeout=15)
        print(f"API Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✓ API SUCCESS! Got {len(data)} items")
            if data:
                print(f"Sample data: {data[0].get('companyName', 'N/A')}")
        else:
            print(f"✗ API Error: {response.status_code}")
            print(f"Response: {response.text[:200]}")
            
    except Exception as e:
        print(f"✗ API call failed: {e}")
else:
    print("✗ No valid API key found")

print("\n=== Diagnosis Complete ===")
