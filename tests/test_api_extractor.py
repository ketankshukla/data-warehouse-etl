"""
Tests for the API Extractor module.
"""
import unittest
from unittest.mock import patch, MagicMock
import json
import pandas as pd
import requests
import time

from src.extractors.api_extractor import APIExtractor, RateLimiter


class TestAPIExtractor(unittest.TestCase):
    """Test cases for the API Extractor."""

    def setUp(self):
        """Set up test fixtures."""
        self.basic_config = {
            "url": "https://api.example.com",
            "endpoint": "/v1/data",
            "method": "GET",
            "headers": {"Accept": "application/json"},
            "params": {"limit": 100}
        }
        
        # Sample API response data
        self.sample_response = {
            "data": [
                {"id": 1, "name": "Item 1", "details": {"color": "red", "size": "large"}},
                {"id": 2, "name": "Item 2", "details": {"color": "blue", "size": "medium"}},
                {"id": 3, "name": "Item 3", "details": {"color": "green", "size": "small"}}
            ],
            "meta": {
                "page": 1,
                "total_pages": 2,
                "total_items": 5
            }
        }
        
        # Sample paginated response data
        self.paginated_responses = [
            {
                "data": [
                    {"id": 1, "name": "Item 1", "details": {"color": "red", "size": "large"}},
                    {"id": 2, "name": "Item 2", "details": {"color": "blue", "size": "medium"}}
                ],
                "meta": {
                    "page": 1,
                    "total_pages": 2,
                    "has_more": True,
                    "next_page": 2
                }
            },
            {
                "data": [
                    {"id": 3, "name": "Item 3", "details": {"color": "green", "size": "small"}}
                ],
                "meta": {
                    "page": 2,
                    "total_pages": 2,
                    "has_more": False,
                    "next_page": None
                }
            }
        ]
    
    @patch('requests.request')
    def test_basic_extraction(self, mock_request):
        """Test basic API data extraction."""
        # Configure the mock
        mock_response = MagicMock()
        mock_response.json.return_value = self.sample_response
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response
        
        # Create and run extractor
        extractor = APIExtractor(self.basic_config)
        result = extractor.extract()
        
        # Assertions
        mock_request.assert_called_once()
        self.assertEqual(len(result), 1)  # Result should be a list
        self.assertEqual(result[0], self.sample_response)  # Data should match our sample
    
    @patch('requests.request')
    def test_extraction_with_items_path(self, mock_request):
        """Test API extraction with items path configuration."""
        # Add items path to config
        config = self.basic_config.copy()
        config["pagination"] = {
            "type": "none",
            "items_path": "data"
        }
        
        # Configure the mock
        mock_response = MagicMock()
        mock_response.json.return_value = self.sample_response
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response
        
        # Create and run extractor
        extractor = APIExtractor(config)
        result = extractor.extract()
        
        # Assertions
        self.assertEqual(len(result), 3)  # Should have 3 items from the data array
        self.assertEqual(result[0]["id"], 1)  # First item should have id 1
    
    @patch('requests.request')
    def test_pagination_offset(self, mock_request):
        """Test offset-based pagination."""
        # Configure mock for pagination
        mock_response1 = MagicMock()
        mock_response1.json.return_value = self.paginated_responses[0]
        mock_response1.raise_for_status.return_value = None
        
        mock_response2 = MagicMock()
        mock_response2.json.return_value = self.paginated_responses[1]
        mock_response2.raise_for_status.return_value = None
        
        mock_request.side_effect = [mock_response1, mock_response2]
        
        # Configure extractor with pagination
        config = self.basic_config.copy()
        config["pagination"] = {
            "type": "offset",
            "param": "page",
            "size_param": "limit",
            "page_size": 2,
            "max_pages": 5,
            "items_path": "data",
            "has_more_data_path": "meta.has_more"
        }
        
        # Create and run extractor
        extractor = APIExtractor(config)
        result = extractor.extract()
        
        # Assertions
        self.assertEqual(mock_request.call_count, 2)  # Should have made 2 requests
        self.assertEqual(len(result), 3)  # Should have 3 items total
        self.assertEqual(result[0]["id"], 1)  # First item should be from first page
        self.assertEqual(result[2]["id"], 3)  # Last item should be from second page
    
    @patch('requests.request')
    def test_authentication_basic(self, mock_request):
        """Test basic authentication setup."""
        # Configure the mock
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "success"}
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response
        
        # Configure with basic auth
        config = self.basic_config.copy()
        config["auth"] = {
            "type": "basic",
            "username": "user",
            "password": "pass"
        }
        
        # Create and run extractor
        extractor = APIExtractor(config)
        extractor.extract()
        
        # Check that auth was properly set up
        mock_request.assert_called_once()
        kwargs = mock_request.call_args[1]
        self.assertIsInstance(kwargs["auth"], tuple)
        self.assertEqual(kwargs["auth"], ("user", "pass"))
    
    @patch('requests.request')
    def test_authentication_bearer(self, mock_request):
        """Test bearer token authentication setup."""
        # Configure the mock
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "success"}
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response
        
        # Configure with bearer auth
        config = self.basic_config.copy()
        config["auth"] = {
            "type": "bearer",
            "token": "abc123token"
        }
        
        # Create and run extractor
        extractor = APIExtractor(config)
        extractor.extract()
        
        # Check that auth header was properly set up
        mock_request.assert_called_once()
        kwargs = mock_request.call_args[1]
        self.assertIn("Authorization", kwargs["headers"])
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer abc123token")
    
    @patch('requests.request')
    def test_rate_limiting(self, mock_request):
        """Test rate limiting functionality."""
        # Configure the mock
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "success"}
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response
        
        # Configure with rate limiting
        config = self.basic_config.copy()
        config["rate_limit"] = {
            "requests_per_minute": 60  # 1 request per second
        }
        
        # Create extractor
        extractor = APIExtractor(config)
        
        # Mock the rate limiter's wait_if_needed method to verify it's called
        with patch.object(extractor.rate_limiter, 'wait_if_needed') as mock_wait:
            extractor.extract()
            mock_wait.assert_called_once()
    
    @patch('requests.request')
    def test_error_handling_and_retries(self, mock_request):
        """Test error handling and retry functionality."""
        # Create a rate-limited response first, then successful
        rate_limited_response = MagicMock()
        rate_limited_response.raise_for_status.side_effect = requests.exceptions.HTTPError("429 Too Many Requests")
        rate_limited_response.status_code = 429
        
        success_response = MagicMock()
        success_response.json.return_value = {"status": "success"}
        success_response.raise_for_status.return_value = None
        
        # Mock the request to fail then succeed
        mock_request.side_effect = [
            requests.exceptions.HTTPError("429 Too Many Requests", response=rate_limited_response),
            success_response
        ]
        
        # Configure with retry settings
        config = self.basic_config.copy()
        config["retry_count"] = 3
        config["retry_delay"] = 0.1  # Short delay for testing
        
        # Create and run extractor
        extractor = APIExtractor(config)
        
        # Should retry and eventually succeed
        result = extractor.extract()
        
        # Assertions
        self.assertEqual(mock_request.call_count, 2)  # Should have made 2 requests (1 fail, 1 success)
        self.assertEqual(result[0]["status"], "success")  # Should have gotten success response


class TestRateLimiter(unittest.TestCase):
    """Test cases for the Rate Limiter."""
    
    def test_rate_limiter_per_minute(self):
        """Test per-minute rate limiting."""
        # Create a rate limiter with 60 requests per minute
        limiter = RateLimiter(requests_per_minute=60)
        
        # The interval should be 1 second
        self.assertEqual(limiter.min_interval, 1.0)
        
        # First request should not wait
        with patch('time.sleep') as mock_sleep:
            limiter.wait_if_needed()
            mock_sleep.assert_not_called()
        
        # Update the last request time to now
        limiter.last_request_time = time.time()
        
        # Next request should wait if it's too soon
        with patch('time.sleep') as mock_sleep:
            with patch('time.time', return_value=limiter.last_request_time + 0.5):  # Half a second later
                limiter.wait_if_needed()
                mock_sleep.assert_called_once()
                sleep_time = mock_sleep.call_args[0][0]
                self.assertAlmostEqual(sleep_time, 0.5, delta=0.1)  # Should wait about 0.5 seconds
    
    def test_rate_limiter_daily_limit(self):
        """Test daily rate limiting."""
        # Create a rate limiter with 1000 requests per day
        limiter = RateLimiter(requests_per_minute=0, requests_per_day=1000)
        
        # Track a bunch of requests
        for _ in range(999):
            limiter.daily_request_count += 1
        
        # Next request should still work (we're at 999, limit is 1000)
        with patch('time.sleep') as mock_sleep:
            limiter.wait_if_needed()
            mock_sleep.assert_not_called()
        
        # One more request should hit the limit and require waiting
        limiter.daily_request_count += 1  # Now we're at 1000
        
        with patch('time.sleep') as mock_sleep:
            with patch('time.time', return_value=limiter.day_start_time + 1000):  # Some time has passed
                limiter.wait_if_needed()
                mock_sleep.assert_called_once()  # Should wait until tomorrow


if __name__ == '__main__':
    unittest.main()
