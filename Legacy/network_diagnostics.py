"""
Comprehensive Network Diagnostics for ETF Data Processor
Tests all possible network configurations and methods
"""

import requests
import urllib3
import urllib.request
import urllib.parse
import ssl
import socket
import json
import os
import sys
import time
from datetime import datetime
import subprocess
import platform
from pathlib import Path
import logging

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NetworkDiagnostics:
    def __init__(self):
        # Load API key from environment
        from dotenv import load_dotenv
        load_dotenv()
        
        self.fmp_api_key = os.getenv('FMP_API_KEY', 'your_fmp_api_key_here')
        self.base_url = "https://financialmodelingprep.com/api/v3"
        
        print(f"Network Diagnostics Tool")
        print(f"=" * 50)
        print(f"Python Version: {sys.version}")
        print(f"Platform: {platform.platform()}")
        print(f"API Key Set: {'Yes' if self.fmp_api_key != 'your_fmp_api_key_here' else 'No'}")
        print(f"=" * 50)
    
    def test_basic_connectivity(self):
        """Test basic internet connectivity"""
        print(f"\n1. Testing Basic Internet Connectivity")
        print(f"-" * 40)
        
        test_hosts = [
            ('Google DNS', '8.8.8.8', 53),
            ('Cloudflare DNS', '1.1.1.1', 53),
            ('Google HTTP', 'google.com', 80),
            ('Google HTTPS', 'google.com', 443),
            ('FMP Domain', 'financialmodelingprep.com', 443)
        ]
        
        results = {}
        for name, host, port in test_hosts:
            try:
                start_time = time.time()
                socket.create_connection((host, port), timeout=10).close()
                duration = time.time() - start_time
                results[name] = {'status': 'SUCCESS', 'duration': duration}
                print(f"✓ {name}: OK ({duration:.2f}s)")
            except Exception as e:
                results[name] = {'status': 'FAILED', 'error': str(e)}
                print(f"✗ {name}: FAILED - {e}")
        
        return results
    
    def test_dns_resolution(self):
        """Test DNS resolution"""
        print(f"\n2. Testing DNS Resolution")
        print(f"-" * 40)
        
        test_domains = [
            'google.com',
            'yahoo.com',
            'financialmodelingprep.com',
            'finance.yahoo.com'
        ]
        
        results = {}
        for domain in test_domains:
            try:
                start_time = time.time()
                ip_address = socket.gethostbyname(domain)
                duration = time.time() - start_time
                results[domain] = {'status': 'SUCCESS', 'ip': ip_address, 'duration': duration}
                print(f"✓ {domain}: {ip_address} ({duration:.2f}s)")
            except Exception as e:
                results[domain] = {'status': 'FAILED', 'error': str(e)}
                print(f"✗ {domain}: FAILED - {e}")
        
        return results
    
    def detect_proxy_settings(self):
        """Detect system proxy settings"""
        print(f"\n3. Detecting Proxy Settings")
        print(f"-" * 40)
        
        proxies = {}
        
        # Check environment variables
        env_proxies = {}
        for proto in ['http', 'https']:
            for var_name in [f'{proto}_proxy', f'{proto.upper()}_PROXY']:
                proxy_val = os.environ.get(var_name)
                if proxy_val:
                    env_proxies[proto] = proxy_val
        
        if env_proxies:
            print(f"Environment Proxies: {env_proxies}")
            proxies.update(env_proxies)
        else:
            print("No environment proxy variables found")
        
        # Check Windows registry (if on Windows)
        if platform.system() == 'Windows':
            try:
                import winreg
                with winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                                    r'Software\Microsoft\Windows\CurrentVersion\Internet Settings') as key:
                    try:
                        proxy_enable, _ = winreg.QueryValueEx(key, 'ProxyEnable')
                        if proxy_enable:
                            proxy_server, _ = winreg.QueryValueEx(key, 'ProxyServer')
                            proxies['http'] = f'http://{proxy_server}'
                            proxies['https'] = f'http://{proxy_server}'
                            print(f"Windows Registry Proxy: {proxy_server}")
                        else:
                            print("Windows proxy disabled in registry")
                    except FileNotFoundError:
                        print("No proxy settings found in Windows registry")
            except Exception as e:
                print(f"Could not read Windows registry: {e}")
        
        # Check requests library proxy detection
        try:
            req_proxies = requests.utils.getproxies()
            if req_proxies:
                print(f"Requests library detected proxies: {req_proxies}")
                proxies.update(req_proxies)
            else:
                print("Requests library found no proxies")
        except Exception as e:
            print(f"Error detecting proxies with requests: {e}")
        
        return proxies
    
    def test_ssl_configurations(self):
        """Test different SSL configurations"""
        print(f"\n4. Testing SSL Configurations")
        print(f"-" * 40)
        
        test_url = "https://financialmodelingprep.com"
        
        ssl_configs = {
            'Default SSL': {
                'verify': True,
                'ssl_context': None
            },
            'SSL Verification Disabled': {
                'verify': False,
                'ssl_context': None
            },
            'Custom SSL Context': {
                'verify': True,
                'ssl_context': self._create_custom_ssl_context()
            },
            'Relaxed SSL Context': {
                'verify': False,
                'ssl_context': self._create_relaxed_ssl_context()
            }
        }
        
        results = {}
        for config_name, config in ssl_configs.items():
            try:
                if 'urllib' in config_name.lower():
                    # Test with urllib
                    request = urllib.request.Request(test_url)
                    context = config['ssl_context'] if config['ssl_context'] else ssl.create_default_context()
                    with urllib.request.urlopen(request, timeout=10, context=context) as response:
                        status_code = response.getcode()
                else:
                    # Test with requests
                    session = requests.Session()
                    if not config['verify']:
                        session.verify = False
                    
                    response = session.get(test_url, timeout=10)
                    status_code = response.status_code
                
                if status_code == 200:
                    results[config_name] = {'status': 'SUCCESS', 'code': status_code}
                    print(f"✓ {config_name}: OK")
                else:
                    results[config_name] = {'status': 'FAILED', 'code': status_code}
                    print(f"✗ {config_name}: HTTP {status_code}")
                    
            except Exception as e:
                results[config_name] = {'status': 'ERROR', 'error': str(e)}
                print(f"✗ {config_name}: ERROR - {e}")
        
        return results
    
    def _create_custom_ssl_context(self):
        """Create a custom SSL context"""
        context = ssl.create_default_context()
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED
        return context
    
    def _create_relaxed_ssl_context(self):
        """Create a relaxed SSL context"""
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context
    
    def test_http_methods(self):
        """Test different HTTP client methods"""
        print(f"\n5. Testing HTTP Client Methods")
        print(f"-" * 40)
        
        test_url = f"{self.base_url}/symbol/SPY"
        params = {'apikey': self.fmp_api_key} if self.fmp_api_key != 'your_fmp_api_key_here' else {}
        
        methods = {
            'Requests Default': self._test_requests_default,
            'Requests with Retries': self._test_requests_with_retries,
            'Requests SSL Disabled': self._test_requests_ssl_disabled,
            'Urllib3': self._test_urllib3,
            'Urllib Standard': self._test_urllib_standard,
            'Urllib SSL Disabled': self._test_urllib_ssl_disabled
        }
        
        results = {}
        for method_name, method_func in methods.items():
            try:
                start_time = time.time()
                response_data = method_func(test_url, params)
                duration = time.time() - start_time
                
                if response_data:
                    results[method_name] = {
                        'status': 'SUCCESS',
                        'duration': duration,
                        'data_length': len(str(response_data))
                    }
                    print(f"✓ {method_name}: OK ({duration:.2f}s, {len(str(response_data))} chars)")
                else:
                    results[method_name] = {'status': 'FAILED', 'error': 'No data returned'}
                    print(f"✗ {method_name}: No data returned")
                    
            except Exception as e:
                results[method_name] = {'status': 'ERROR', 'error': str(e)}
                print(f"✗ {method_name}: ERROR - {e}")
            
            # Small delay between tests
            time.sleep(0.5)
        
        return results
    
    def _test_requests_default(self, url, params):
        """Test with requests default settings"""
        response = requests.get(url, params=params, timeout=15)
        return response.json() if response.status_code == 200 else None
    
    def _test_requests_with_retries(self, url, params):
        """Test with requests and retry logic"""
        from requests.adapters import HTTPAdapter
        from requests.packages.urllib3.util.retry import Retry
        
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        response = session.get(url, params=params, timeout=15)
        return response.json() if response.status_code == 200 else None
    
    def _test_requests_ssl_disabled(self, url, params):
        """Test with requests and SSL disabled"""
        session = requests.Session()
        session.verify = False
        response = session.get(url, params=params, timeout=15)
        return response.json() if response.status_code == 200 else None
    
    def _test_urllib3(self, url, params):
        """Test with urllib3"""
        http = urllib3.PoolManager(
            cert_reqs='CERT_NONE',
            assert_hostname=False
        )
        
        url_with_params = f"{url}?{urllib.parse.urlencode(params)}"
        response = http.request('GET', url_with_params, timeout=15)
        
        if response.status == 200:
            return json.loads(response.data.decode('utf-8'))
        return None
    
    def _test_urllib_standard(self, url, params):
        """Test with standard urllib"""
        url_with_params = f"{url}?{urllib.parse.urlencode(params)}"
        request = urllib.request.Request(url_with_params)
        
        with urllib.request.urlopen(request, timeout=15) as response:
            data = response.read().decode('utf-8')
            return json.loads(data)
    
    def _test_urllib_ssl_disabled(self, url, params):
        """Test with urllib and SSL verification disabled"""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        url_with_params = f"{url}?{urllib.parse.urlencode(params)}"
        request = urllib.request.Request(url_with_params)
        
        with urllib.request.urlopen(request, timeout=15, context=ssl_context) as response:
            data = response.read().decode('utf-8')
            return json.loads(data)
    
    def test_system_configuration(self):
        """Test system configuration that might affect networking"""
        print(f"\n6. Testing System Configuration")
        print(f"-" * 40)
        
        # Test firewall status (Windows)
        if platform.system() == 'Windows':
            try:
                result = subprocess.run(
                    ['netsh', 'advfirewall', 'show', 'allprofiles', 'state'],
                    capture_output=True, text=True, timeout=10
                )
                if 'ON' in result.stdout:
                    print("✓ Windows Firewall: ENABLED")
                else:
                    print("⚠ Windows Firewall: DISABLED")
            except Exception as e:
                print(f"Could not check firewall status: {e}")
        
        # Test Python SSL module
        try:
            import ssl
            print(f"✓ Python SSL module: {ssl.OPENSSL_VERSION}")
        except Exception as e:
            print(f"✗ Python SSL module: ERROR - {e}")
        
        # Test certificate store
        try:
            import certifi
            cert_path = certifi.where()
            if os.path.exists(cert_path):
                print(f"✓ Certificate store: {cert_path}")
            else:
                print(f"✗ Certificate store: NOT FOUND")
        except ImportError:
            print("⚠ Certifi module not installed")
    
    def run_comprehensive_test(self):
        """Run all diagnostic tests"""
        print(f"\nStarting Comprehensive Network Diagnostics")
        print(f"Time: {datetime.now()}")
        print(f"{'='*60}")
        
        all_results = {}
        
        # Run all tests
        all_results['basic_connectivity'] = self.test_basic_connectivity()
        all_results['dns_resolution'] = self.test_dns_resolution()
        all_results['proxy_settings'] = self.detect_proxy_settings()
        all_results['ssl_configurations'] = self.test_ssl_configurations()
        all_results['http_methods'] = self.test_http_methods()
        self.test_system_configuration()
        
        # Generate summary
        self.generate_diagnostic_report(all_results)
        
        return all_results
    
    def generate_diagnostic_report(self, results):
        """Generate a comprehensive diagnostic report"""
        print(f"\n{'='*60}")
        print(f"DIAGNOSTIC SUMMARY REPORT")
        print(f"{'='*60}")
        
        # Basic connectivity summary
        basic_success = sum(1 for r in results['basic_connectivity'].values() if r['status'] == 'SUCCESS')
        basic_total = len(results['basic_connectivity'])
        print(f"Basic Connectivity: {basic_success}/{basic_total} tests passed")
        
        # DNS resolution summary
        dns_success = sum(1 for r in results['dns_resolution'].values() if r['status'] == 'SUCCESS')
        dns_total = len(results['dns_resolution'])
        print(f"DNS Resolution: {dns_success}/{dns_total} tests passed")
        
        # SSL configuration summary
        ssl_success = sum(1 for r in results['ssl_configurations'].values() if r['status'] == 'SUCCESS')
        ssl_total = len(results['ssl_configurations'])
        print(f"SSL Configurations: {ssl_success}/{ssl_total} tests passed")
        
        # HTTP methods summary
        http_success = sum(1 for r in results['http_methods'].values() if r['status'] == 'SUCCESS')
        http_total = len(results['http_methods'])
        print(f"HTTP Methods: {http_success}/{http_total} tests passed")
        
        # Working methods
        working_methods = [method for method, result in results['http_methods'].items() 
                          if result['status'] == 'SUCCESS']
        
        print(f"\nRecommended Methods for ETF Data Fetching:")
        if working_methods:
            for method in working_methods:
                duration = results['http_methods'][method].get('duration', 'N/A')
                print(f"✓ {method} (Duration: {duration:.2f}s)")
        else:
            print("✗ No HTTP methods working - Network connectivity issues detected")
        
        # Proxy information
        if results['proxy_settings']:
            print(f"\nProxy Configuration Detected:")
            for proto, proxy in results['proxy_settings'].items():
                print(f"  {proto}: {proxy}")
        else:
            print(f"\nNo proxy configuration detected")
        
        # Save detailed report
        report_data = {
            'timestamp': datetime.now().isoformat(),
            'system_info': {
                'platform': platform.platform(),
                'python_version': sys.version
            },
            'test_results': results
        }
        
        report_file = Path('./network_diagnostics_report.json')
        with open(report_file, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        print(f"\nDetailed report saved: {report_file}")
        
        # Provide recommendations
        print(f"\nRecommendations:")
        if http_success == 0:
            print("❌ No HTTP methods working. Possible causes:")
            print("   - Corporate firewall blocking all outbound connections")
            print("   - Proxy authentication required")
            print("   - DNS issues preventing domain resolution")
            print("   - SSL certificate validation failures")
            print("\nTry running: configure_network.ps1 as Administrator")
        elif http_success < http_total / 2:
            print("⚠ Limited connectivity. Consider:")
            print("   - Using SSL-disabled methods for testing")
            print("   - Configuring proxy settings if behind corporate firewall")
            print("   - Running network configuration script")
        else:
            print("✅ Good connectivity detected!")
            print("   - Recommend using the working HTTP methods identified above")

def main():
    """Main execution function"""
    diagnostics = NetworkDiagnostics()
    results = diagnostics.run_comprehensive_test()
    
    print(f"\n{'='*60}")
    print(f"Network diagnostics completed!")
    print(f"Check network_diagnostics_report.json for detailed results")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
