"""
CSV extractor for the Data Warehouse ETL Framework.
Extracts data from CSV files.
"""
import os
import logging
from typing import Dict, Any, Optional

import pandas as pd

from src.extractors.base_extractor import BaseExtractor
from src.utils.logging_utils import get_etl_logger

class CSVExtractor(BaseExtractor):
    """
    Extractor for CSV files.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize CSV extractor with configuration.
        
        Args:
            config: Dictionary containing configuration parameters
        """
        super().__init__(config)
        self.logger = get_etl_logger(__name__, component="CSVExtractor")
        self.logger.info("Initializing CSV extractor")
        
        # Required parameters
        self.file_path = self.config.get("file_path")
        if not self.file_path:
            self.logger.error("file_path is required for CSV extractor")
            raise ValueError("file_path is required for CSV extractor")
        
        # Optional parameters with defaults
        self.delimiter = self.config.get("delimiter", ",")
        self.encoding = self.config.get("encoding", "utf-8")
        self.header = self.config.get("header", 0)
        self.dtype = self.config.get("dtype", None)
        self.parse_dates = self.config.get("parse_dates", None)
        self.skiprows = self.config.get("skiprows", None)
        self.nrows = self.config.get("nrows", None)
        self.usecols = self.config.get("usecols", None)
        self.chunksize = self.config.get("chunksize", None)
        
        self.logger.debug(f"CSV extractor configured with file_path={self.file_path}, "
                       f"delimiter='{self.delimiter}', encoding={self.encoding}")
    
    def validate_source(self) -> bool:
        """
        Validate that the CSV source file exists and is accessible.
        
        Returns:
            True if the source is valid, False otherwise
        """
        if not self.file_path:
            self.logger.error("file_path is required for CSV extractor")
            return False
        
        # Check if file path is absolute or relative
        if not os.path.isabs(self.file_path):
            self.file_path = os.path.abspath(self.file_path)
        
        if not os.path.exists(self.file_path):
            self.logger.error(f"CSV file does not exist: {self.file_path}")
            return False
        
        if not os.path.isfile(self.file_path):
            self.logger.error(f"Path is not a file: {self.file_path}")
            return False
        
        # Attempt to open the file to check permissions
        try:
            with open(self.file_path, 'r', encoding=self.encoding) as _:
                pass
        except IOError as e:
            self.logger.error(f"Unable to open CSV file {self.file_path}: {str(e)}")
            return False
        except UnicodeDecodeError:
            self.logger.error(f"Unable to decode CSV file {self.file_path} with encoding {self.encoding}")
            return False
        
        self.logger.info(f"CSV source validated: {self.file_path}")
        return True
    
    def extract(self) -> pd.DataFrame:
        """
        Extract data from CSV file and return as a DataFrame.
        
        Returns:
            Pandas DataFrame containing the CSV data
        """
        self.logger.info(f"Extracting data from CSV file: {self.file_path}")
        
        if not self.validate_source():
            self.logger.error(f"CSV source validation failed for {self.file_path}")
            return pd.DataFrame()
        
        try:
            # Extract data using pandas read_csv
            data = pd.read_csv(
                self.file_path,
                delimiter=self.delimiter,
                encoding=self.encoding,
                header=self.header,
                dtype=self.dtype,
                parse_dates=self.parse_dates,
                skiprows=self.skiprows,
                nrows=self.nrows,
                usecols=self.usecols
            )
            
            # Log extraction results
            rows, cols = data.shape
            self.logger.info(f"Successfully extracted {rows} rows and {cols} columns from {self.file_path}")
            
            # Basic data quality check
            if data.empty:
                self.logger.warning(f"Extracted DataFrame is empty from {self.file_path}")
            else:
                # Check for missing values
                missing_values = data.isna().sum().sum()
                if missing_values > 0:
                    self.logger.warning(f"Extracted data contains {missing_values} missing values")
            
            return data
            
        except pd.errors.EmptyDataError:
            self.logger.error(f"CSV file is empty: {self.file_path}")
            return pd.DataFrame()
        except pd.errors.ParserError as e:
            self.logger.error(f"Error parsing CSV file {self.file_path}: {str(e)}")
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error extracting data from CSV file {self.file_path}: {str(e)}")
            return pd.DataFrame()
