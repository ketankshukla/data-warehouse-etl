"""
JSON Extractor module for the Data Warehouse ETL Framework.
Provides functionality to extract data from JSON files or APIs.
"""
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from .base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

class JSONExtractor(BaseExtractor):
    """
    Extractor for JSON data sources, including files and APIs.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the JSON extractor with configuration.
        
        Args:
            config: Dictionary containing configuration parameters including:
                - source_type: 'file' or 'api'
                - file_path: Path to JSON file (for 'file' source_type)
                - url: API URL (for 'api' source_type)
                - headers: HTTP headers for API requests
                - params: Query parameters for API requests
                - auth: Authentication information for API requests
                - record_path: Path to records in nested JSON (e.g., 'data.records')
                - normalize: Whether to normalize semi-structured JSON data
        """
        super().__init__(config)
        self.source_type = config.get("source_type", "file")
        self.file_path = config.get("file_path")
        self.url = config.get("url")
        self.headers = config.get("headers", {})
        self.params = config.get("params", {})
        self.auth = config.get("auth")
        self.record_path = config.get("record_path")
        self.normalize = config.get("normalize", False)
    
    def validate_source(self) -> bool:
        """
        Validate that the JSON source is accessible.
        
        Returns:
            True if the source is valid, False otherwise
        """
        if self.source_type == "file":
            if not self.file_path:
                logger.error("File path not provided in configuration")
                return False
            
            if not os.path.exists(self.file_path):
                logger.error(f"File not found: {self.file_path}")
                return False
            
            if not os.path.isfile(self.file_path):
                logger.error(f"Path is not a file: {self.file_path}")
                return False
            
            # Check if file has read permission
            if not os.access(self.file_path, os.R_OK):
                logger.error(f"No read permission for file: {self.file_path}")
                return False
            
            return True
        
        elif self.source_type == "api":
            if not self.url:
                logger.error("API URL not provided in configuration")
                return False
            
            # Basic URL validation
            if not (self.url.startswith("http://") or self.url.startswith("https://")):
                logger.error(f"Invalid URL format: {self.url}")
                return False
            
            return True
        
        else:
            logger.error(f"Unsupported source type: {self.source_type}")
            return False
    
    def _extract_from_file(self) -> Any:
        """
        Extract data from a JSON file.
        
        Returns:
            JSON data as Python dict/list
        """
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data
        except Exception as e:
            logger.error(f"Error reading JSON file: {str(e)}")
            raise
    
    def _extract_from_api(self) -> Any:
        """
        Extract data from a JSON API.
        
        Returns:
            JSON data as Python dict/list
        """
        try:
            response = requests.get(
                self.url, 
                headers=self.headers,
                params=self.params,
                auth=self.auth
            )
            response.raise_for_status()  # Raise exception for 4XX/5XX responses
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error making API request: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON response: {str(e)}")
            raise
    
    def _extract_nested_data(self, data: Any) -> Any:
        """
        Extract nested data using record_path if specified.
        
        Args:
            data: The JSON data object
        
        Returns:
            The extracted nested data
        """
        if not self.record_path:
            return data
        
        try:
            nested_data = data
            for key in self.record_path.split('.'):
                nested_data = nested_data[key]
            return nested_data
        except (KeyError, TypeError) as e:
            logger.error(f"Error extracting nested data with path '{self.record_path}': {str(e)}")
            raise KeyError(f"Invalid record path: {self.record_path}")
    
    def extract(self) -> pd.DataFrame:
        """
        Extract data from the JSON source.
        
        Returns:
            Pandas DataFrame containing the JSON data
        
        Raises:
            FileNotFoundError: If the file does not exist
            requests.RequestException: If the API request fails
            Exception: For other extraction errors
        """
        if not self.validate_source():
            raise ValueError(f"Invalid or inaccessible JSON source")
        
        try:
            # Extract data based on source type
            if self.source_type == "file":
                logger.info(f"Reading JSON file: {self.file_path}")
                data = self._extract_from_file()
            else:  # api
                logger.info(f"Fetching data from API: {self.url}")
                data = self._extract_from_api()
            
            # Extract nested data if record_path is specified
            if self.record_path:
                data = self._extract_nested_data(data)
            
            # Convert to DataFrame
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                if self.normalize:
                    # Normalize semi-structured JSON data
                    df = pd.json_normalize(data)
                else:
                    df = pd.DataFrame([data])
            else:
                raise ValueError(f"Unexpected JSON data format: {type(data)}")
            
            logger.info(f"Successfully extracted {len(df)} records")
            return df
        
        except Exception as e:
            logger.error(f"Error extracting data from JSON source: {str(e)}")
            raise
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the extracted JSON data.
        
        Returns:
            Dictionary with metadata information
        """
        metadata = super().get_metadata()
        
        if self.source_type == "file":
            source = self.file_path
            if self.validate_source():
                file_stat = os.stat(self.file_path)
                metadata.update({
                    "size_bytes": file_stat.st_size,
                    "last_modified": datetime.fromtimestamp(file_stat.st_mtime).isoformat()
                })
        else:  # api
            source = self.url
        
        metadata.update({
            "source": source,
            "source_type": self.source_type,
            "timestamp": datetime.now().isoformat(),
            "format": "JSON",
            "record_path": self.record_path
        })
            
        return metadata
