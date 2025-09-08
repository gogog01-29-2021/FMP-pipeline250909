#!/usr/bin/env python3
"""
Debug FMP API connectivity
"""
import requests
import json
from dotenv import load_dotenv
import os

def test_fmp_api():
    load_dotenv()
    
    api_key = os.getenv('FMP_API_KEY')
    print(f"🔑 API Key: {api_key[:10]}...")
    
    # Test 1: Simple profile request
    print("\n🧪 Test 1: Profile request")
    url = f"https://financialmodelingprep.com/api/v3/profile/SPY"
    params = {'apikey': api_key}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        print(f"📡 Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Data received: {len(data)} items")
            if data:
                profile = data[0]
                print(f"   Symbol: {profile.get('symbol')}")
                print(f"   Name: {profile.get('companyName')}")
                print(f"   Price: ${profile.get('price')}")
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            print(f"Response: {response.text[:200]}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request Error: {e}")
    
    # Test 2: Historical data request (short period)
    print("\n🧪 Test 2: Historical data (1 month)")
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/SPY"
    params = {
        'from': '2025-08-01',
        'to': '2025-09-01',
        'apikey': api_key
    }
    
    try:
        response = requests.get(url, params=params, timeout=15)
        print(f"📡 Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Response received")
            
            if 'historical' in data:
                historical = data['historical']
                print(f"📊 Historical data points: {len(historical)}")
                if historical:
                    print(f"   Latest: {historical[0]['date']} - ${historical[0]['close']}")
            else:
                print(f"❌ No 'historical' key in response")
                print(f"Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            print(f"Response: {response.text[:200]}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request Error: {e}")

if __name__ == "__main__":
    test_fmp_api()
