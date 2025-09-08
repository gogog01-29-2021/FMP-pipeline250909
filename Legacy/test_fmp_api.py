"""
Simple test to verify FMP API functionality
"""
import requests
import json

def test_fmp_api():
    # Note: You need to set this to your actual FMP API key
    FMP_API_KEY = "YOUR_FMP_API_KEY_HERE"
    
    if FMP_API_KEY == "YOUR_FMP_API_KEY_HERE":
        print("❌ Please set your FMP_API_KEY in this script or .env file")
        print("Get your free API key from: https://financialmodelingprep.com/")
        return False
    
    print("🧪 Testing FMP API connection...")
    
    # Test with SPY profile
    url = "https://financialmodelingprep.com/api/v3/profile/SPY"
    params = {'apikey': FMP_API_KEY}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        if data and len(data) > 0:
            profile = data[0]
            print(f"✅ FMP API working!")
            print(f"   Symbol: {profile.get('symbol')}")
            print(f"   Company: {profile.get('companyName')}")
            print(f"   Price: ${profile.get('price')}")
            print(f"   Market Cap: ${profile.get('mktCap'):,}" if profile.get('mktCap') else "")
            return True
        else:
            print("❌ FMP API returned empty data")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ FMP API request failed: {e}")
        return False

if __name__ == "__main__":
    test_fmp_api()
