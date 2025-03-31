"""
API Extractor module for the Data Warehouse ETL Framework.
Provides functionality to extract data from REST APIs.
"""
import json
import logging
import time
from typing import Any, Dict, List, Optional, Union, Tuple
import requests
from requests.auth import HTTPBasicAuth

from src.extractors.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)


class APIExtractor(BaseExtractor):
    """
    Extractor for retrieving data from REST APIs.
    
    Supports various authentication methods, pagination strategies,
    and response formats.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the API extractor with configuration.
        
        Args:
            config: Dictionary containing configuration parameters for the API
                - url: Base URL for the API
                - endpoint: Specific API endpoint (optional)
                - method: HTTP method (GET, POST, etc.) - defaults to GET
                - headers: Custom HTTP headers (optional)
                - params: Query parameters (optional)
                - body: Request body for POST/PUT requests (optional)
                - auth: Authentication configuration (optional)
                    - type: "basic", "bearer", "api_key", or "oauth2"
                    - username/password: For basic auth
                    - token: For bearer auth
                    - key/param_name/location: For API key auth
                    - client_id/client_secret/token_url: For OAuth2
                - pagination: Pagination configuration (optional)
                    - type: "offset", "cursor", or "link"
                    - param: Parameter name for page/offset
                    - size_param: Parameter name for page size
                    - page_size: Number of items per page
                    - max_pages: Maximum number of pages to retrieve
                    - items_path: JSONPath to the items in the response
                    - next_cursor_path: JSONPath to next cursor for cursor pagination
                    - next_link_path: JSONPath to next link for link pagination
                - rate_limit: Rate limiting configuration (optional)
                    - requests_per_minute: Maximum requests per minute
                    - requests_per_day: Maximum requests per day
        """
        super().__init__(config)
        
        # Base API configuration
        self.base_url = config.get("url", "")
        if not self.base_url:
            raise ValueError("API URL is required")
            
        self.endpoint = config.get("endpoint", "")
        self.url = f"{self.base_url.rstrip('/')}/{self.endpoint.lstrip('/')}" if self.endpoint else self.base_url
        
        # Request configuration
        self.method = config.get("method", "GET").upper()
        self.headers = config.get("headers", {})
        self.params = config.get("params", {})
        self.body = config.get("body", None)
        self.timeout = config.get("timeout", 30)
        self.verify_ssl = config.get("verify_ssl", True)
        
        # Authentication setup
        auth_config = config.get("auth", {})
        self.auth_type = auth_config.get("type", None)
        self.auth = None
        self.setup_authentication(auth_config)
        
        # Pagination configuration
        self.pagination_config = config.get("pagination", {})
        self.pagination_type = self.pagination_config.get("type", "none")
        self.items_path = self.pagination_config.get("items_path", None)
        
        # Pagination parameters
        self.page_param = self.pagination_config.get("page_param", "page")
        self.page_size_param = self.pagination_config.get("page_size_param", "per_page")
        self.page_size = self.pagination_config.get("page_size", 100)
        self.start_page = self.pagination_config.get("start_page", 1)
        self.max_pages = self.pagination_config.get("max_pages", 100)
        
        # Rate limiting
        rate_limit_config = config.get("rate_limit", {})
        self.requests_per_minute = rate_limit_config.get("requests_per_minute", 0)
        self.requests_per_day = rate_limit_config.get("requests_per_day", 0)
        self.rate_limiter = None
        
        if self.requests_per_minute > 0 or self.requests_per_day > 0:
            self.setup_rate_limiter()
            
        # Response format
        self.response_format = config.get("response_format", "json").lower()
        
        # Error handling
        self.retry_count = config.get("retry_count", 3)
        self.retry_delay = config.get("retry_delay", 1)
        self.retry_statuses = config.get("retry_statuses", [429, 500, 502, 503, 504])
        
        logger.info(f"Initialized API extractor for {self.url}")
    
    def setup_authentication(self, auth_config: Dict[str, Any]) -> None:
        """Set up API authentication based on the provided configuration."""
        if not auth_config:
            logger.debug("No authentication configured")
            return
            
        auth_type = auth_config.get("type", "").lower()
        
        if auth_type == "basic":
            # Basic Auth
            username = auth_config.get("username", "")
            password = auth_config.get("password", "")
            self.auth = (username, password)
            logger.debug("Set up Basic Authentication")
            
        elif auth_type == "bearer":
            # Bearer token auth
            token = auth_config.get("token", "")
            if token:
                self.headers["Authorization"] = f"Bearer {token}"
                logger.debug("Set up Bearer Token Authentication")
            
        elif auth_type == "api_key":
            # API Key auth
            key = auth_config.get("key", "")
            location = auth_config.get("location", "query").lower()
            param_name = auth_config.get("param_name", "api_key")
            
            if location == "query":
                self.params[param_name] = key
                logger.debug(f"Set up API Key Authentication in query parameter '{param_name}'")
            elif location == "header":
                self.headers[param_name] = key
                logger.debug(f"Set up API Key Authentication in header '{param_name}'")
            
        elif auth_type == "oauth2":
            # OAuth 2.0 client credentials flow
            client_id = auth_config.get("client_id", "")
            client_secret = auth_config.get("client_secret", "")
            token_url = auth_config.get("token_url", "")
            
            if client_id and client_secret and token_url:
                token = self.get_oauth2_token(client_id, client_secret, token_url)
                if token:
                    self.headers["Authorization"] = f"Bearer {token}"
                    logger.debug("Set up OAuth 2.0 Authentication")
        
        else:
            logger.warning(f"Unsupported authentication type: {auth_type}")
    
    def get_oauth2_token(self, client_id: str, client_secret: str, token_url: str, scope: Optional[str] = None) -> Optional[str]:
        """
        Retrieve an OAuth 2.0 access token using client credentials flow.
        
        Args:
            client_id: OAuth client ID
            client_secret: OAuth client secret
            token_url: URL to request token from
            scope: OAuth scope (optional)
            
        Returns:
            Access token if successful, None otherwise
        """
        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        }
        
        if scope:
            data["scope"] = scope
            
        try:
            response = requests.post(token_url, data=data, timeout=30)
            response.raise_for_status()
            
            token_data = response.json()
            return token_data.get("access_token")
            
        except Exception as e:
            logger.error(f"Failed to retrieve OAuth token: {str(e)}")
            return None
    
    def setup_rate_limiter(self) -> None:
        """Set up rate limiting for API requests."""
        self.rate_limiter = RateLimiter(
            requests_per_minute=self.requests_per_minute,
            requests_per_day=self.requests_per_day
        )
        logger.debug(f"Set up rate limiting: {self.requests_per_minute} req/min, {self.requests_per_day} req/day")
    
    def extract(self) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Extract data from the API source.
        
        Returns:
            Extracted data as a list of dictionaries or a single dictionary
        """
        logger.info(f"Extracting data from API: {self.url}")
        
        start_time = time.time()
        metadata = self.get_metadata()
        metadata["timestamp"] = start_time
        
        try:
            # Check if pagination is enabled
            if self.pagination_type != "none":
                data = self.extract_with_pagination()
            else:
                response_data = self.make_request()
                # If an items_path is specified, extract the items
                if self.items_path:
                    data = self.extract_items(response_data)
                else:
                    data = response_data
                
            duration = time.time() - start_time
            logger.info(f"API extraction completed in {duration:.2f} seconds")
            
            # Add metadata
            metadata["duration_seconds"] = duration
            metadata["record_count"] = len(data) if isinstance(data, list) else 1
            
            # Return the data with metadata
            if isinstance(data, list):
                return data
            else:
                return [data]  # Return as list for consistent handling
                
        except Exception as e:
            logger.error(f"Error during API extraction: {str(e)}")
            raise
    
    def make_request(self, url: Optional[str] = None, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Make a request to the API with the configured options.
        
        Args:
            url (str, optional): URL to request. Defaults to the configured URL.
            params (dict, optional): Parameters to include in the request.
                Defaults to the configured params.
        
        Returns:
            dict: JSON response from the API
            
        Raises:
            requests.HTTPError: If the API returns an error status code
        """
        # Use the configured URL and params if not provided
        if url is None:
            url = f"{self.url}{self.endpoint}"
        
        # Start with base params and update with provided params
        request_params = self.params.copy() if self.params else {}
        if params:
            request_params.update(params)
            
        # Apply rate limiting if configured
        if hasattr(self, 'rate_limiter') and self.rate_limiter:
            self.rate_limiter.wait_if_needed()
        
        # Prepare auth for different auth types
        auth = None
        if self.auth_type == "basic":
            if isinstance(self.auth, tuple) and len(self.auth) == 2:
                auth = (self.auth[0], self.auth[1])
            
        # Make the request with retries
        retries = 0
        retry_count = getattr(self, 'retry_count', 3)
        retry_backoff = getattr(self, 'retry_backoff', 0.1)
        
        while retries <= retry_count:
            try:
                logger.debug(f"Making {self.method} request to {url}")
                
                response = requests.request(
                    method=self.method,
                    url=url,
                    params=request_params,
                    headers=self.headers,
                    auth=auth,
                    json=self.body if self.method in ['POST', 'PUT', 'PATCH'] else None,
                    timeout=self.timeout,
                    verify=self.verify_ssl
                )
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                retries += 1
                if retries <= retry_count:
                    wait_time = retry_backoff * (2 ** (retries - 1))
                    logger.warning(f"Request failed: {str(e)}. Retrying in {wait_time}s ({retries}/{retry_count})")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Request failed after {retry_count} retries: {str(e)}")
                    raise
    
    def extract_with_pagination(self) -> List[Dict[str, Any]]:
        """
        Extract data from an API using pagination.
        
        Returns:
            List of records from all pages
        """
        if self.pagination_type == "offset":
            return self.extract_with_offset_pagination()
        elif self.pagination_type == "cursor":
            return self.extract_with_cursor_pagination()
        elif self.pagination_type == "link":
            return self.extract_with_link_pagination()
        else:
            logger.warning(f"Unsupported pagination type: {self.pagination_type}")
            return []
    
    def extract_with_offset_pagination(self) -> List[Dict[str, Any]]:
        """
        Extract data using offset-based pagination.
        
        Returns:
            List of items from all pages
        """
        all_items = []
        current_page = self.start_page
        max_pages = getattr(self, 'max_pages', 100)
        page_param = self.pagination_config.get("page_param", "page")
        page_size_param = self.pagination_config.get("page_size_param", "per_page")
        page_size = self.pagination_config.get("page_size", 100)
        
        try:
            # Continue fetching pages until we reach the limit or run out of data
            while current_page <= max_pages:
                logger.debug(f"Retrieving page {current_page}/{max_pages}")
                
                # Create pagination parameters
                page_params = {
                    page_param: current_page
                }
                if page_size_param:
                    page_params[page_size_param] = page_size
                
                response_data = self.make_request(params=page_params)
                
                # Extract items from response
                items = self.extract_items(response_data)
                
                if not items:
                    logger.debug(f"No more items found on page {current_page}")
                    break
                    
                all_items.extend(items)
                
                # Check if we need to continue pagination
                has_more = self.check_has_more(response_data)
                if not has_more:
                    logger.debug(f"No more pages to fetch after page {current_page}")
                    break
                    
                current_page += 1
                
            logger.info(f"Extracted {len(all_items)} items from {current_page} pages")
            return all_items
            
        except Exception as e:
            logger.error(f"Error during offset pagination: {str(e)}")
            raise
    
    def extract_with_cursor_pagination(self) -> List[Dict[str, Any]]:
        """
        Extract data using cursor-based pagination.
        
        Returns:
            List of items from all pages
        """
        all_items = []
        cursor = None
        
        try:
            # Continue fetching pages until we reach the limit or run out of data
            while True:
                logger.debug(f"Retrieving page with cursor: {cursor}")
                
                # Create pagination parameters
                page_params = {}
                if cursor:
                    page_params[self.pagination_config.get("cursor_param", "cursor")] = cursor
                
                response_data = self.make_request(params=page_params)
                
                # Extract items from response
                items = self.extract_items(response_data)
                
                if not items:
                    logger.debug("No more items found")
                    break
                    
                all_items.extend(items)
                
                # Get next cursor
                cursor_path = self.pagination_config.get("next_cursor_path")
                cursor = self.extract_value_from_path(response_data, cursor_path)
                
                if not cursor:
                    logger.debug("No next cursor found")
                    break
                    
            logger.info(f"Extracted {len(all_items)} items")
            return all_items
            
        except Exception as e:
            logger.error(f"Error during cursor pagination: {str(e)}")
            raise
    
    def extract_with_link_pagination(self) -> List[Dict[str, Any]]:
        """
        Extract data using link-based pagination.
        
        Returns:
            List of items from all pages
        """
        all_items = []
        next_url = None
        
        try:
            # Continue fetching pages until we reach the limit or run out of data
            while True:
                logger.debug(f"Retrieving page with next link: {next_url}")
                
                if next_url:
                    response = requests.get(next_url, headers=self.headers, auth=self.auth, timeout=self.timeout)
                    response.raise_for_status()
                    response_data = response.json()
                else:
                    response_data = self.make_request()
                
                # Extract items from response
                items = self.extract_items(response_data)
                
                if not items:
                    logger.debug("No more items found")
                    break
                    
                all_items.extend(items)
                
                # Get next link
                next_link_path = self.pagination_config.get("next_link_path")
                next_url = self.extract_value_from_path(response_data, next_link_path)
                
                if not next_url:
                    logger.debug("No next link found")
                    break
                    
            logger.info(f"Extracted {len(all_items)} items")
            return all_items
            
        except Exception as e:
            logger.error(f"Error during link pagination: {str(e)}")
            raise
    
    def extract_items(self, response_data: Any) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Extract items from the API response.
        
        Args:
            response_data: API response data
            
        Returns:
            List of items or a single item
        """
        if not self.items_path:
            return response_data
        
        return self.extract_value_from_path(response_data, self.items_path)
    
    def extract_value_from_path(self, data: Any, path: Optional[str]) -> Any:
        """
        Extract a value from a nested dictionary using a dot-notation path.
        
        Args:
            data: Dictionary or list to extract from
            path: Dot-notation path (e.g., "data.results")
            
        Returns:
            Extracted value
        """
        if not path:
            return data
        
        parts = path.split(".")
        current = data
        
        for part in parts:
            if isinstance(current, dict):
                if part not in current:
                    logger.warning(f"Path part '{part}' not found in data")
                    return None
                current = current[part]
            elif isinstance(current, list) and part.isdigit():
                index = int(part)
                if index >= len(current):
                    logger.warning(f"Index {index} out of range in list of length {len(current)}")
                    return None
                current = current[index]
            else:
                logger.warning(f"Cannot extract '{part}' from a {type(current).__name__}")
                return None
        
        return current
    
    def check_has_more(self, response_data: Any) -> bool:
        """
        Check if there are more pages of data available.
        
        Args:
            response_data: API response data
            
        Returns:
            True if more data is available, False otherwise
        """
        has_more_path = self.pagination_config.get("has_more_data_path")
        
        if has_more_path:
            # Use explicit "has more data" flag from response
            has_more = self.extract_value_from_path(response_data, has_more_path)
            if has_more is not None:
                # Convert to boolean if needed
                if isinstance(has_more, str):
                    return has_more.lower() == "true"
                return bool(has_more)
        
        # If no explicit flag, check if we received fewer items than page size
        # (indicates we've reached the end)
        if self.page_size and len(response_data) < self.page_size:
            return False
        
        # Default to true - we'll continue until we hit max_pages
        return True
    
    def validate_source(self) -> bool:
        """
        Validate that the API is accessible.
        
        Returns:
            True if validation succeeds, False otherwise
        """
        logger.info(f"Validating API accessibility: {self.url}")
        
        try:
            # Attempt to make a request with HEAD method to validate accessibility
            validate_method = "HEAD" if self.method == "GET" else self.method
            
            response = requests.request(
                method=validate_method,
                url=self.url,
                params=self.params,
                headers=self.headers,
                auth=self.auth if isinstance(self.auth, tuple) else None,
                timeout=self.timeout,
                verify=self.verify_ssl
            )
            
            response.raise_for_status()
            logger.info(f"API validation successful: {self.url}")
            return True
            
        except Exception as e:
            logger.error(f"API validation failed: {str(e)}")
            return False
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the API source.
        
        Returns:
            Dictionary with metadata information
        """
        metadata = super().get_metadata()
        metadata.update({
            "url": self.url,
            "method": self.method,
            "pagination_type": self.pagination_type,
            "pagination_params": {
                "page_size": self.page_size,
                "max_pages": self.max_pages
            }
        })
        return metadata


class RateLimiter:
    """Handles API rate limiting to prevent exceeding allowed request quotas."""
    
    def __init__(self, requests_per_minute: int = 60, requests_per_day: Optional[int] = None):
        """
        Initialize the rate limiter.
        
        Args:
            requests_per_minute: Maximum requests per minute (0 means unlimited)
            requests_per_day: Maximum requests per day (0 means unlimited)
        """
        self.requests_per_minute = requests_per_minute
        self.requests_per_day = requests_per_day
        
        # Calculate interval between requests (in seconds)
        self.min_interval = 60.0 / self.requests_per_minute if self.requests_per_minute > 0 else 0
        
        # Request tracking
        self.last_request_time = 0
        self.daily_request_count = 0
        self.day_start_time = time.time()  # Reset each day
    
    def wait_if_needed(self) -> bool:
        """
        Wait if necessary to respect rate limits.
        
        Returns:
            True after rate limit is respected
        """
        current_time = time.time()
        
        # Check daily limit
        if self.requests_per_day:
            # Check if we're in a new day
            seconds_in_day = 24 * 60 * 60
            if current_time - self.day_start_time > seconds_in_day:
                # Reset for new day
                self.day_start_time = current_time
                self.daily_request_count = 0
                
            # Check if we've hit the daily limit
            if self.daily_request_count >= self.requests_per_day:
                seconds_until_reset = seconds_in_day - (current_time - self.day_start_time)
                logger.warning(f"Daily rate limit reached. Next reset in {seconds_until_reset/60:.1f} minutes")
                # Sleep until tomorrow
                time.sleep(seconds_until_reset)
                # Reset counters
                self.day_start_time = time.time()
                self.daily_request_count = 0
        
        # Check per-minute limit
        if self.min_interval > 0:
            elapsed = current_time - self.last_request_time
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
                
        # Update tracking
        self.last_request_time = time.time()
        self.daily_request_count += 1
        
        return True
