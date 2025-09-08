#!/usr/bin/env python3
"""
Simple FMP API Test - No external dependencies
"""
import urllib.request
import urllib.parse
import json
import os

def simple_api_test():
    api_key = "X0EbPVay8gnDiRxEmgulxY8Y0pTGK3Om"
    
    # Test the most basic API call
    url = f"https://financialmodelingprep.com/api/v3/profile/SPY?apikey={api_key}"
    
    print(f"🧪 Testing FMP API...")
    print(f"🔗 URL: {url}")
    
    try:
        response = urllib.request.urlopen(url, timeout=30)
        content = response.read().decode('utf-8')
        
        print(f"✅ Response received!")
        print(f"📊 Status: {response.getcode()}")
        print(f"📝 Content length: {len(content)}")
        
        # Try to parse JSON
        try:
            data = json.loads(content)
            print(f"✅ JSON parsed successfully!")
            print(f"📊 Data type: {type(data)}")
            
            if isinstance(data, list) and len(data) > 0:
                profile = data[0]
                print(f"🏷️  Symbol: {profile.get('symbol', 'N/A')}")
                print(f"🏢 Company: {profile.get('companyName', 'N/A')}")
                print(f"💰 Price: ${profile.get('price', 'N/A')}")
                return True
            elif isinstance(data, dict):
                print(f"📋 Keys: {list(data.keys())}")
                return True
            else:
                print(f"❌ Unexpected data format: {data}")
                return False
                
        except json.JSONDecodeError as e:
            print(f"❌ JSON parsing error: {e}")
            print(f"Raw content (first 500 chars): {content[:500]}")
            return False
            
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return False

if __name__ == "__main__":
    success = simple_api_test()
    if success:
        print(f"\n✅ API is working! The issue might be elsewhere.")
    else:
        print(f"\n❌ API test failed. Check your API key or network connection.")
