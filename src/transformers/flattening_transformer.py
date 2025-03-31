"""
Flattening Transformer module for the Data Warehouse ETL Framework.
Provides functionality to flatten nested data structures.
"""
import logging
from typing import Any, Dict, List, Optional, Union
import pandas as pd
import json

from src.transformers.base_transformer import BaseTransformer

logger = logging.getLogger(__name__)


class FlatteningTransformer(BaseTransformer):
    """
    Transformer that flattens nested data structures.
    
    Particularly useful for handling nested JSON or API responses
    by flattening nested objects and arrays into column format.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the flattening transformer with configuration.
        
        Args:
            config: Dictionary containing configuration parameters
                - flatten_fields: List of fields to flatten
                    - path: Path to the field to flatten
                    - prefix: Prefix for flattened field names (optional)
                    - drop_original: Whether to drop the original field after flattening (default: True)
                    - separator: Separator to use in flattened field names (default: '_')
                    - handle_lists: How to handle lists in nested structures
                        - 'expand': Create separate columns for each list index
                        - 'first': Only keep the first item in list
                        - 'join': Join list items into a string
                        - 'ignore': Do not flatten lists
                - max_depth: Maximum depth for recursion when flattening (default: 10)
        """
        super().__init__(config)
        
        self.flatten_fields = config.get("flatten_fields", [])
        if not self.flatten_fields:
            logger.warning("No fields specified for flattening")
            
        self.max_depth = config.get("max_depth", 10)
        logger.info(f"Initialized flattening transformer with {len(self.flatten_fields)} fields to flatten")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the data by flattening nested structures.
        
        Args:
            df: DataFrame containing data to transform
            
        Returns:
            Transformed DataFrame with flattened structure
        """
        logger.info("Flattening nested data structures")
        
        if df.empty:
            logger.warning("Empty DataFrame received, returning unchanged")
            return df
        
        # Make a copy to avoid modifying the original
        df_copy = df.copy()
        
        # Process each field configuration
        for field_config in self.flatten_fields:
            df_copy = self.flatten_field(df_copy, field_config)
        
        logger.info(f"Flattening complete, new column count: {len(df_copy.columns)}")
        return df_copy
    
    def flatten_field(self, df, field_config):
        """
        Flatten a specific field in the DataFrame based on configuration.
        
        Args:
            df (pd.DataFrame): DataFrame to transform
            field_config (dict): Configuration for flattening this field
            
        Returns:
            pd.DataFrame: Transformed DataFrame
        """
        # Extract configuration
        path = field_config.get("path")
        prefix = field_config.get("prefix", path)
        separator = field_config.get("separator", "_")
        drop_original = field_config.get("drop_original", True)
        handle_lists = field_config.get("handle_lists", "expand")
        
        # Create a copy to avoid modifying the original
        df_copy = df.copy()
        
        # Skip if the field doesn't exist
        if path not in df_copy.columns:
            logger.warning(f"Field '{path}' not found in DataFrame, skipping")
            return df_copy
            
        # Process each row
        for idx, row in df_copy.iterrows():
            nested_value = row[path]
            
            # Skip null values
            if pd.isna(nested_value):
                continue
                
            # Parse JSON string if needed
            if isinstance(nested_value, str):
                try:
                    nested_value = json.loads(nested_value)
                except (json.JSONDecodeError, ValueError):
                    logger.warning(f"Could not parse JSON from field '{path}', skipping")
                    continue
                    
            # Handle different data types
            if isinstance(nested_value, dict):
                self._flatten_dict(df_copy, idx, nested_value, prefix, separator)
            elif isinstance(nested_value, list):
                self._flatten_list(df_copy, idx, nested_value, path, handle_lists, separator)
                
        # Drop original field if requested
        if drop_original and path in df_copy.columns:
            df_copy = df_copy.drop(columns=[path])
            
        return df_copy

    def _flatten_dict(self, df, idx, nested_dict, prefix, separator, parent_key="", current_depth=0):
        """
        Flatten a nested dictionary into DataFrame columns.
        
        Args:
            df (pd.DataFrame): DataFrame to modify
            idx (int): Row index to update
            nested_dict (dict): Nested dictionary to flatten
            prefix (str): Prefix for new column names
            separator (str): Separator for nested keys
            parent_key (str): Parent key for recursion
            current_depth (int): Current nesting depth
        """
        # Don't process further if we've reached max depth
        if current_depth >= self.max_depth:
            if parent_key:
                col_name = f"{prefix}{separator}{parent_key}" if prefix else parent_key
                # Serialize complex objects to avoid pandas issues
                df.loc[idx, col_name] = json.dumps(nested_dict)
            return
            
        for key, value in nested_dict.items():
            # Create the full key
            if parent_key:
                full_key = f"{parent_key}{separator}{key}"
            else:
                full_key = key
                
            # Create the column name
            col_name = f"{prefix}{separator}{full_key}" if prefix else full_key
            
            # Handle nested dictionaries recursively
            if isinstance(value, dict):
                self._flatten_dict(df, idx, value, prefix, separator, full_key, current_depth + 1)
            elif isinstance(value, list):
                self._flatten_list(df, idx, value, col_name, "expand", separator, current_depth + 1)
            else:
                # Add the value to the DataFrame
                df.loc[idx, col_name] = value

    def _flatten_list(self, df, idx, nested_list, path, handle_lists, separator, current_depth=0):
        """
        Flatten a nested list into DataFrame columns.
        
        Args:
            df (pd.DataFrame): DataFrame to modify
            idx (int): Row index to update
            nested_list (list): Nested list to flatten
            path (str): Field path/name
            handle_lists (str): Strategy for handling lists ('expand', 'join', 'first')
            separator (str): Separator for joined lists
            current_depth (int): Current nesting depth
        """
        # Don't process further if we've reached max depth
        if current_depth >= self.max_depth:
            # Serialize complex objects to avoid pandas issues
            df.loc[idx, path] = json.dumps(nested_list)
            return
            
        if not nested_list:
            return
            
        if handle_lists == "expand":
            # Expand list items into separate columns
            for i, item in enumerate(nested_list):
                col_name = f"{path}_{i}"
                if isinstance(item, dict):
                    self._flatten_dict(df, idx, item, "", separator, col_name, current_depth + 1)
                elif isinstance(item, list):
                    self._flatten_list(df, idx, item, col_name, handle_lists, separator, current_depth + 1)
                else:
                    df.loc[idx, col_name] = item
                
        elif handle_lists == "join":
            # Join list items into a single string
            if all(isinstance(item, (str, int, float, bool)) for item in nested_list):
                joined_value = ", ".join(str(item) for item in nested_list)
                df.loc[idx, path] = joined_value
                
        elif handle_lists == "first":
            # Take the first item only
            if nested_list:
                if isinstance(nested_list[0], dict):
                    self._flatten_dict(df, idx, nested_list[0], "", separator, path, current_depth + 1)
                elif isinstance(nested_list[0], list):
                    self._flatten_list(df, idx, nested_list[0], path, handle_lists, separator, current_depth + 1)
                else:
                    df.loc[idx, path] = nested_list[0]
    
    def validate(self) -> bool:
        """
        Validate the transformer configuration.
        
        Returns:
            True if validation succeeds, False otherwise
        """
        if not self.flatten_fields:
            logger.warning("No fields specified for flattening")
            return False
            
        for field_config in self.flatten_fields:
            if "path" not in field_config:
                logger.error("Missing 'path' in flatten field configuration")
                return False
                
            handle_lists = field_config.get("handle_lists", "expand")
            if handle_lists not in ["expand", "first", "join", "ignore"]:
                logger.error(f"Invalid handle_lists value: {handle_lists}")
                return False
        
        return True
