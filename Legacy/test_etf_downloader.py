import unittest
from unittest.mock import patch, Mock
import pandas as pd
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etf_downloader import ETFDownloader

class TestETFDownloader(unittest.TestCase):
    
    def setUp(self):
        self.downloader = ETFDownloader()
    
    def test_init(self):
        """Test ETFDownloader initialization"""
        self.assertIsInstance(self.downloader.etfs, dict)
        self.assertGreater(len(self.downloader.etfs), 0)
    
    def test_etf_list_contains_expected_symbols(self):
        """Test that ETF list contains expected symbols"""
        expected_symbols = ['SPY', 'VOO', 'QQQ', 'IWM', 'VXX', 'VIXY']
        for symbol in expected_symbols:
            self.assertIn(symbol, self.downloader.etfs)
    
    def test_etf_metadata_structure(self):
        """Test that ETF metadata has correct structure"""
        for symbol, info in self.downloader.etfs.items():
            self.assertIn('name', info)
            self.assertIn('expense_ratio', info)
            self.assertIn('index', info)
            self.assertIsInstance(info['expense_ratio'], (int, float))
    
    @patch('etf_downloader.requests.get')
    def test_get_historical_data_success(self, mock_get):
        """Test successful data retrieval"""
        # Mock successful API response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            'historical': [
                {
                    'date': '2023-01-01',
                    'open': 100.0,
                    'high': 102.0,
                    'low': 99.0,
                    'close': 101.0,
                    'adjClose': 101.0,
                    'volume': 1000000,
                    'change': 1.0,
                    'changePercent': 1.0,
                    'vwap': 100.5,
                    'unadjustedVolume': 1000000
                }
            ]
        }
        mock_get.return_value = mock_response
        
        # Set API key for test
        self.downloader.api_key = 'test_key'
        
        result = self.downloader.get_historical_data('SPY', 1)
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertGreater(len(result), 0)
        self.assertIn('symbol', result.columns)
        self.assertIn('close', result.columns)
    
    @patch('etf_downloader.requests.get')
    def test_get_historical_data_api_error(self, mock_get):
        """Test API error handling"""
        mock_get.side_effect = Exception("API Error")
        
        # Set API key for test
        self.downloader.api_key = 'test_key'
        
        result = self.downloader.get_historical_data('SPY', 1)
        
        self.assertIsNone(result)
    
    def test_get_etf_summary(self):
        """Test ETF summary generation"""
        # This should not raise any exceptions
        try:
            self.downloader.get_etf_summary()
        except Exception as e:
            self.fail(f"get_etf_summary raised an exception: {e}")

if __name__ == '__main__':
    unittest.main()
