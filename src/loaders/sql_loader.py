"""
SQL Database Loader module for the Data Warehouse ETL Framework.
Provides functionality to load data into SQL databases.
"""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
import json

import pandas as pd
from sqlalchemy import create_engine, inspect, Table, MetaData, Column, Integer, String, Float, DateTime
from sqlalchemy.engine import Engine
from sqlalchemy.schema import CreateSchema

from .base_loader import BaseLoader

logger = logging.getLogger(__name__)

class SQLLoader(BaseLoader):
    """
    Loader for SQL database destinations.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the SQL loader with configuration.
        
        Args:
            config: Dictionary containing configuration parameters including:
                - connection_string: SQLAlchemy connection string
                - table_name: Name of the target table
                - schema: Database schema (optional)
                - if_exists: Strategy for handling existing table ('fail', 'replace', 'append')
                - create_schema: Whether to create schema if it doesn't exist (default: False)
                - dtype: Dictionary of column data types (optional)
                - index: Whether to include index in the table (default: False)
                - chunksize: Number of rows to insert at once (optional)
                - method: SQLAlchemy insertion method (optional, default: None)
                - create_table_metadata: Dictionary with table creation metadata (optional)
                - serialize_json: Whether to serialize JSON data (default: True)
                - json_columns: List of columns to serialize as JSON (optional)
        """
        super().__init__(config)
        self.connection_string = config.get("connection_string")
        self.table_name = config.get("table_name")
        self.schema = config.get("schema")
        self.if_exists = config.get("if_exists", "fail")
        self.create_schema = config.get("create_schema", False)
        self.dtype = config.get("dtype")
        self.index = config.get("index", False)
        self.chunksize = config.get("chunksize")
        self.method = config.get("method")
        self.create_table_metadata = config.get("create_table_metadata")
        self.engine = None
        self.serialize_json = config.get("serialize_json", True)
        self.json_columns = config.get("json_columns", [])
    
    def _create_engine(self) -> Engine:
        """
        Create SQLAlchemy engine from connection string.
        
        Returns:
            SQLAlchemy Engine object
        """
        if not self.connection_string:
            raise ValueError("Connection string not provided in configuration")
        
        try:
            return create_engine(self.connection_string)
        except Exception as e:
            logger.error(f"Error creating database engine: {str(e)}")
            raise
    
    def validate_destination(self) -> bool:
        """
        Validate that the database destination is accessible.
        
        Returns:
            True if the destination is valid, False otherwise
        """
        if not self.connection_string:
            logger.error("Connection string not provided in configuration")
            return False
        
        if not self.table_name:
            logger.error("Table name not provided in configuration")
            return False
        
        try:
            # Create engine if not already created
            if not self.engine:
                self.engine = self._create_engine()
            
            # Test connection by getting database info
            inspector = inspect(self.engine)
            
            # Check if schema exists (if specified)
            if self.schema:
                schemas = inspector.get_schema_names()
                if self.schema not in schemas:
                    if not self.create_schema:
                        logger.error(f"Schema '{self.schema}' does not exist")
                        return False
            
            # Check if table exists (only relevant for 'fail' if_exists strategy)
            if self.if_exists == 'fail':
                tables = inspector.get_table_names(schema=self.schema)
                if self.table_name in tables:
                    logger.error(f"Table '{self.table_name}' already exists in schema '{self.schema}'")
                    return False
            
            return True
        
        except Exception as e:
            logger.error(f"Error validating database connection: {str(e)}")
            return False
    
    def _ensure_schema_exists(self) -> bool:
        """
        Ensure that the specified schema exists, creating it if necessary.
        
        Returns:
            True if schema exists or was created, False otherwise
        """
        if not self.schema:
            return True  # No schema specified, use default
        
        try:
            inspector = inspect(self.engine)
            schemas = inspector.get_schema_names()
            
            if self.schema in schemas:
                return True  # Schema already exists
            
            if self.create_schema:
                # Create the schema
                with self.engine.begin() as connection:
                    connection.execute(CreateSchema(self.schema))
                logger.info(f"Created schema '{self.schema}'")
                return True
            else:
                logger.error(f"Schema '{self.schema}' does not exist and create_schema is False")
                return False
            
        except Exception as e:
            logger.error(f"Error checking/creating schema: {str(e)}")
            return False
    
    def _create_table_if_needed(self, df: pd.DataFrame) -> bool:
        """
        Create the target table if it doesn't exist and if_exists is not 'append' or 'replace'.
        
        Args:
            df: DataFrame with data to load, used for schema inference
            
        Returns:
            True if table exists or was created, False otherwise
        """
        if self.if_exists in ('append', 'replace'):
            return True  # Table will be created/replaced by pandas to_sql
        
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names(schema=self.schema)
            
            if self.table_name in tables:
                return True  # Table already exists
            
            if not self.create_table_metadata:
                logger.error(f"Table '{self.table_name}' does not exist and no metadata provided for creation")
                return False
            
            # Create the table using metadata
            metadata = MetaData(schema=self.schema)
            
            # Define columns from metadata
            columns = []
            for col_name, col_def in self.create_table_metadata.get("columns", {}).items():
                col_type = col_def.get("type", "string").lower()
                
                if col_type == "integer":
                    col = Column(col_name, Integer)
                elif col_type == "float":
                    col = Column(col_name, Float)
                elif col_type == "datetime":
                    col = Column(col_name, DateTime)
                else:  # default to string
                    length = col_def.get("length", 255)
                    col = Column(col_name, String(length))
                
                # Add primary key if specified
                if col_def.get("primary_key", False):
                    col.primary_key = True
                
                # Add nullable constraint if specified
                if "nullable" in col_def:
                    col.nullable = col_def["nullable"]
                
                columns.append(col)
            
            # Create the table
            table = Table(self.table_name, metadata, *columns)
            metadata.create_all(self.engine)
            
            logger.info(f"Created table '{self.table_name}' in schema '{self.schema}'")
            return True
            
        except Exception as e:
            logger.error(f"Error creating table: {str(e)}")
            return False
    
    def _preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess DataFrame to handle complex data types before SQL insertion.
        
        Args:
            df: DataFrame to preprocess
        
        Returns:
            Preprocessed DataFrame
        """
        if not self.serialize_json:
            return df
        
        result_df = df.copy()
        
        # Find columns with lists or dicts
        complex_columns = []
        
        # Check explicitly specified JSON columns
        if self.json_columns:
            complex_columns = [col for col in self.json_columns if col in result_df.columns]
        else:
            # Auto-detect complex columns
            for col in result_df.columns:
                if any(isinstance(val, (list, dict)) for val in result_df[col].dropna()):
                    complex_columns.append(col)
        
        # Serialize complex columns to JSON strings
        for col in complex_columns:
            logger.debug(f"Serializing complex data in column: {col}")
            result_df[col] = result_df[col].apply(
                lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
            )
        
        if complex_columns:
            logger.info(f"Serialized {len(complex_columns)} complex data columns to JSON strings")
        
        return result_df
    
    def load(self, data: Union[pd.DataFrame, List[pd.DataFrame]]) -> bool:
        """
        Load data to the SQL database.
        
        Args:
            data: Data to load as DataFrame or list of DataFrames
            
        Returns:
            True if loading was successful, False otherwise
        """
        if not self.validate_destination():
            raise ValueError("Invalid or inaccessible database destination")
        
        try:
            # Create engine if not already created
            if not self.engine:
                self.engine = self._create_engine()
            
            # Ensure schema exists
            if not self._ensure_schema_exists():
                raise ValueError(f"Schema '{self.schema}' does not exist or could not be created")
            
            # Handle list of DataFrames (concatenate or load individually)
            if isinstance(data, list):
                if not data:
                    logger.warning("Empty list of DataFrames provided, nothing to load")
                    return True
                
                # If chunked loading is configured, load each DataFrame separately
                if self.chunksize:
                    total_rows = 0
                    for i, df in enumerate(data):
                        success = self.load(df)
                        if not success:
                            raise ValueError(f"Failed to load chunk {i}")
                        total_rows += len(df)
                    
                    logger.info(f"Successfully loaded {total_rows} rows in {len(data)} chunks")
                    return True
                
                # Otherwise, concatenate DataFrames
                data = pd.concat(data, ignore_index=True)
            
            # Check if DataFrame is empty
            if data.empty:
                logger.warning("Empty DataFrame provided, nothing to load")
                return True
            
            # Create table if needed for custom schemas
            if self.create_table_metadata:
                if not self._create_table_if_needed(data):
                    raise ValueError(f"Failed to create table '{self.table_name}'")
            
            # Preprocess data to handle complex data types
            processed_data = self._preprocess_data(data)
            
            # Load data to SQL database
            rows_before = 0
            if self.if_exists in ('append', 'replace'):
                # Get row count before loading (if appending)
                if self.if_exists == 'append':
                    try:
                        rows_before = pd.read_sql(f"SELECT COUNT(*) FROM {self.table_name}", self.engine).iloc[0, 0]
                    except:
                        rows_before = 0
            
            # Perform the actual data loading
            processed_data.to_sql(
                name=self.table_name,
                con=self.engine,
                schema=self.schema,
                if_exists=self.if_exists,
                index=self.index,
                dtype=self.dtype,
                chunksize=self.chunksize,
                method=self.method
            )
            
            # Log success
            logger.info(f"Successfully loaded {len(processed_data)} rows to table '{self.table_name}'")
            
            # Verify row count if appending
            if self.if_exists == 'append':
                try:
                    rows_after = pd.read_sql(f"SELECT COUNT(*) FROM {self.table_name}", self.engine).iloc[0, 0]
                    rows_added = rows_after - rows_before
                    logger.info(f"Added {rows_added} new rows to table '{self.table_name}'")
                except:
                    pass
            
            return True
        
        except Exception as e:
            logger.error(f"Error loading data to database: {str(e)}")
            raise
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the loading operation.
        
        Returns:
            Dictionary with metadata information
        """
        metadata = super().get_metadata()
        
        # Mask sensitive information in connection string
        if self.connection_string:
            # Simple approach to mask passwords in connection strings
            import re
            masked_conn = re.sub(r'(:.*@)', ':***@', self.connection_string)
            destination = masked_conn
        else:
            destination = "Unknown SQL destination"
        
        metadata.update({
            "destination": destination,
            "table_name": self.table_name,
            "schema": self.schema,
            "if_exists": self.if_exists,
            "timestamp": datetime.now().isoformat(),
        })
            
        return metadata
