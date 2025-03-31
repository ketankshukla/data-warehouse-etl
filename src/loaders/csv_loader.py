"""
CSV loader for the Data Warehouse ETL Framework.
Loads data to CSV files.
"""
import os
import logging
from typing import Dict, Any, Optional, List, Union

import pandas as pd

from src.loaders.base_loader import BaseLoader
from src.utils.logging_utils import get_etl_logger

class CSVLoader(BaseLoader):
    """
    Loader for CSV file destinations.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize CSV loader with configuration.
        
        Args:
            config: Dictionary containing configuration parameters
        """
        super().__init__(config)
        self.logger = get_etl_logger(__name__, component="CSVLoader")
        self.logger.info("Initializing CSV loader")
        
        # Required parameters
        self.file_path = self.config.get("file_path")
        if not self.file_path:
            self.logger.error("file_path is required for CSV loader")
            raise ValueError("file_path is required for CSV loader")
        
        # Optional parameters with defaults
        self.delimiter = self.config.get("delimiter", ",")
        self.encoding = self.config.get("encoding", "utf-8")
        self.mode = self.config.get("mode", "w")
        self.index = self.config.get("index", False)
        self.header = self.config.get("include_header", True)
        self.date_format = self.config.get("date_format", None)
        self.decimal = self.config.get("decimal", ".")
        self.quoting = self.config.get("quoting", None)
        self.create_dirs = self.config.get("create_dirs", False)
        
        self.logger.debug(f"CSV loader configured with file_path={self.file_path}, "
                      f"delimiter='{self.delimiter}', encoding={self.encoding}, mode={self.mode}")
    
    def validate_destination(self) -> bool:
        """
        Validate that the CSV destination is writable.
        
        Returns:
            True if the destination is valid, False otherwise
        """
        # Check if file path is absolute or relative
        if not os.path.isabs(self.file_path):
            self.file_path = os.path.abspath(self.file_path)
        
        # Check if the destination directory exists
        dest_dir = os.path.dirname(self.file_path)
        if not os.path.exists(dest_dir):
            if self.create_dirs:
                try:
                    os.makedirs(dest_dir, exist_ok=True)
                    self.logger.info(f"Created directory structure: {dest_dir}")
                except OSError as e:
                    self.logger.error(f"Failed to create directory {dest_dir}: {str(e)}")
                    return False
            else:
                self.logger.error(f"Destination directory does not exist: {dest_dir}")
                return False
        
        # Check if the destination is writable
        try:
            # Try to open the file in write mode
            with open(self.file_path, 'a') as _:
                pass
            
            self.logger.info(f"CSV destination validated: {self.file_path}")
            return True
            
        except IOError as e:
            self.logger.error(f"Destination is not writable: {self.file_path}, error: {str(e)}")
            return False
    
    def load(self, data: Union[pd.DataFrame, List[pd.DataFrame]]) -> bool:
        """
        Load data to CSV file.
        
        Args:
            data: DataFrame or list of DataFrames to load
            
        Returns:
            True if loading was successful, False otherwise
        """
        self.logger.info(f"Loading data to CSV file: {self.file_path}")
        
        if not self.validate_destination():
            self.logger.error(f"CSV destination validation failed for {self.file_path}")
            return False
        
        try:
            # Handle different input types
            if isinstance(data, list):
                if not data:
                    self.logger.warning("Empty data list provided, nothing to load")
                    return True
                
                # Convert list of DataFrames to a single DataFrame
                if all(isinstance(df, pd.DataFrame) for df in data):
                    # Concatenate all DataFrames
                    combined_data = pd.concat(data, ignore_index=True)
                    self.logger.info(f"Combined {len(data)} DataFrames with total {len(combined_data)} rows")
                else:
                    self.logger.error("Input data list contains non-DataFrame objects")
                    return False
            elif isinstance(data, pd.DataFrame):
                combined_data = data
            else:
                self.logger.error(f"Unsupported data type: {type(data)}")
                return False
            
            # Write DataFrame to CSV
            combined_data.to_csv(
                self.file_path,
                sep=self.delimiter,
                encoding=self.encoding,
                mode=self.mode,
                index=self.index,
                header=self.header,
                date_format=self.date_format,
                decimal=self.decimal,
                quoting=self.quoting
            )
            
            rows = len(combined_data)
            self.logger.info(f"Successfully loaded {rows} rows to {self.file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading data to CSV file {self.file_path}: {str(e)}")
            return False
