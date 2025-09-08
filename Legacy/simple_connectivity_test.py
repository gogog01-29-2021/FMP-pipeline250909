import sys
import os
print(f"Python version: {sys.version}")
print(f"Current directory: {os.getcwd()}")
print("Starting simple connectivity test...")

# Test 1: Basic socket connection
import socket
try:
    socket.create_connection(("8.8.8.8", 53), timeout=5).close()
    print("✓ Basic internet connectivity: OK")
except Exception as e:
    print(f"✗ Basic internet connectivity: FAILED - {e}")

# Test 2: DNS resolution
try:
    ip = socket.gethostbyname("google.com")
    print(f"✓ DNS resolution: OK (google.com -> {ip})")
except Exception as e:
    print(f"✗ DNS resolution: FAILED - {e}")

# Test 3: Simple HTTP request
try:
    import urllib.request
    with urllib.request.urlopen("http://httpbin.org/get", timeout=10) as response:
        if response.getcode() == 200:
            print("✓ HTTP request: OK")
        else:
            print(f"✗ HTTP request: Failed with code {response.getcode()}")
except Exception as e:
    print(f"✗ HTTP request: FAILED - {e}")

# Test 4: HTTPS request  
try:
    import ssl
    import urllib.request
    
    # Create SSL context that ignores certificate errors
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    with urllib.request.urlopen("https://httpbin.org/get", timeout=10, context=ssl_context) as response:
        if response.getcode() == 200:
            print("✓ HTTPS request (SSL relaxed): OK")
        else:
            print(f"✗ HTTPS request: Failed with code {response.getcode()}")
except Exception as e:
    print(f"✗ HTTPS request: FAILED - {e}")

print("\nSimple connectivity test completed.")
