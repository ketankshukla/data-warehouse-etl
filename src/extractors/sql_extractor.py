"""
SQL extractor for the Data Warehouse ETL Framework.
Extracts data from SQL databases.
"""
import logging
from typing import Dict, Any, Optional, List, Union

import pandas as pd
from sqlalchemy import create_engine, text

from src.extractors.base_extractor import BaseExtractor
from src.utils.logging_utils import get_etl_logger

class SQLExtractor(BaseExtractor):
    """
    Extractor for SQL data sources.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize SQL extractor with configuration.
        
        Args:
            config: Dictionary containing configuration parameters
        """
        super().__init__(config)
        self.logger = get_etl_logger(__name__, component="SQLExtractor")
        self.logger.info("Initializing SQL extractor")
        
        # Required parameters
        self.connection_string = self.config.get("connection_string")
        if not self.connection_string:
            self.logger.error("connection_string is required for SQL extractor")
            raise ValueError("connection_string is required for SQL extractor")
        
        self.query = self.config.get("query")
        self.table_name = self.config.get("table_name")
        
        if not self.query and not self.table_name:
            self.logger.error("Either query or table_name is required for SQL extractor")
            raise ValueError("Either query or table_name is required for SQL extractor")
        
        # Optional parameters
        self.schema = self.config.get("schema")
        self.chunksize = self.config.get("chunksize")
        self.params = self.config.get("params", {})
        
        # Initialize connection engine lazily
        self._engine = None
        
        self.logger.debug(f"SQL extractor configured with connection_string={self.connection_string}")
    
    @property
    def engine(self):
        """
        Get or create SQLAlchemy engine.
        
        Returns:
            SQLAlchemy engine
        """
        if not self._engine:
            self.logger.debug("Creating SQLAlchemy engine")
            self._engine = create_engine(self.connection_string)
        return self._engine
    
    def validate_source(self) -> bool:
        """
        Validate that the SQL database is accessible.
        
        Returns:
            True if the source is valid, False otherwise
        """
        try:
            # Test connection by executing a simple query
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.logger.info(f"SQL database connection validated: {self.connection_string}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to SQL database: {str(e)}")
            return False
    
    def extract(self) -> pd.DataFrame:
        """
        Extract data from SQL database and return as a DataFrame.
        
        Returns:
            Pandas DataFrame containing the query results
        """
        self.logger.info("Extracting data from SQL database")
        
        if not self.validate_source():
            self.logger.error("SQL source validation failed")
            return pd.DataFrame()
        
        try:
            # Determine the SQL query
            if self.query:
                sql = text(self.query)
                self.logger.info(f"Using provided SQL query")
            else:
                # Construct query from table_name and schema
                table_ref = f"{self.schema}.{self.table_name}" if self.schema else self.table_name
                sql = f"SELECT * FROM {table_ref}"
                self.logger.info(f"Using generated SQL query: {sql}")
            
            # Execute query and get DataFrame
            data = pd.read_sql(
                sql=sql,
                con=self.engine,
                params=self.params,
                chunksize=self.chunksize
            )
            
            # Handle chunked data
            if self.chunksize:
                # If chunked, return first chunk for demonstration
                # In a real implementation, you might process chunks or concatenate them
                self.logger.info(f"Reading data in chunks of {self.chunksize} rows")
                first_chunk = next(data)
                self.logger.info(f"Read first chunk with {len(first_chunk)} rows")
                return first_chunk
            
            # Log extraction results
            rows, cols = data.shape
            self.logger.info(f"Successfully extracted {rows} rows and {cols} columns from SQL database")
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error extracting data from SQL database: {str(e)}")
            return pd.DataFrame()
