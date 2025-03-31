"""
JSON Transformer module for the Data Warehouse ETL Framework.
Provides functionality for transforming JSON and API response data.
"""
import logging
import pandas as pd
import numpy as np
import json
from typing import Any, Dict, List, Optional, Union

from src.transformers.base_transformer import BaseTransformer

logger = logging.getLogger(__name__)


class JSONTransformer(BaseTransformer):
    """
    Transformer for processing JSON data, particularly from API responses.
    
    Provides functionality for selecting fields, renaming columns, 
    type conversion, and applying expressions.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the JSON transformer with configuration.
        
        Args:
            config: Dictionary containing configuration parameters
                - select_fields: List of fields to keep in the output
                - rename_fields: Dictionary mapping original field names to new names
                - type_casting: Dictionary mapping field names to target data types
                - calculated_fields: Dictionary of new fields to create with their expressions
                - drop_fields: List of fields to drop from the output
        """
        super().__init__(config)
        
        # Fields to select (keep only these fields)
        self.select_fields = config.get("select_fields", [])
        
        # Fields to rename (original_name -> new_name)
        self.rename_fields = config.get("rename_fields", {})
        
        # Type casting configuration
        self.type_casting = config.get("type_casting", {})
        
        # Expressions for calculated fields
        self.calculated_fields = config.get("calculated_fields", {})
        
        # Fields to drop
        self.drop_fields = config.get("drop_fields", [])
        
        logger.info(f"Initialized JSON transformer: {len(self.select_fields)} selected fields, "
                  f"{len(self.rename_fields)} renamed fields, {len(self.calculated_fields)} expressions")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the dataframe according to JSON transformation rules.
        
        Args:
            df (pd.DataFrame): DataFrame to transform
            
        Returns:
            pd.DataFrame: Transformed DataFrame
        """
        df_copy = df.copy()
        
        # Apply field selection if specified
        if self.select_fields:
            try:
                df_copy = df_copy[self.select_fields]
            except KeyError as e:
                logger.warning(f"Field selection failed: {e}")
                
        # Apply field renaming if specified
        if self.rename_fields:
            try:
                df_copy = df_copy.rename(columns=self.rename_fields)
            except Exception as e:
                logger.warning(f"Field renaming failed: {e}")
                
        # Apply type casting if specified
        if self.type_casting:
            for field, type_name in self.type_casting.items():
                if field in df_copy.columns:
                    try:
                        if type_name == "int":
                            df_copy[field] = pd.to_numeric(df_copy[field], errors="coerce").astype("Int64")
                        elif type_name == "float":
                            df_copy[field] = pd.to_numeric(df_copy[field], errors="coerce")
                        elif type_name == "str":
                            df_copy[field] = df_copy[field].astype(str)
                        elif type_name == "bool":
                            # Convert to Python bool to ensure proper boolean values
                            # Handle common string representations of boolean values
                            def to_bool(val):
                                if pd.isna(val):
                                    return None
                                if isinstance(val, bool):
                                    return val
                                if isinstance(val, str):
                                    val = val.lower().strip()
                                    if val in ('true', 'yes', 'y', '1'):
                                        return True
                                    if val in ('false', 'no', 'n', '0'):
                                        return False
                                return bool(val)
                            
                            df_copy[field] = df_copy[field].map(to_bool)
                        elif type_name == "date":
                            df_copy[field] = pd.to_datetime(df_copy[field], errors="coerce").dt.date
                        elif type_name == "datetime":
                            df_copy[field] = pd.to_datetime(df_copy[field], errors="coerce")
                        else:
                            logger.warning(f"Unsupported type: {type_name}")
                    except Exception as e:
                        logger.warning(f"Type casting failed for field {field}: {e}")
                        
        # Apply calculated fields if specified
        if self.calculated_fields:
            for field_name, expression in self.calculated_fields.items():
                try:
                    # Handle string concatenation expressions separately
                    if '+' in expression and "'" in expression or '"' in expression:
                        # Parse the expression to identify the fields to concatenate
                        parts = expression.split('+')
                        parts = [p.strip().strip("'").strip('"') for p in parts]
                        
                        # Initialize an empty Series for the result
                        result = pd.Series("", index=df_copy.index)
                        
                        # Process each part and concatenate
                        for part in parts:
                            if part in df_copy.columns:
                                # It's a column name
                                result = result + df_copy[part].astype(str)
                            else:
                                # It's a literal string
                                result = result + part
                        
                        df_copy[field_name] = result
                    else:
                        # Use pandas eval for other expressions
                        df_copy[field_name] = df_copy.eval(expression)
                except Exception as e:
                    logger.warning(f"Error applying expression '{expression}' for field '{field_name}': {e}")
                    
        # Drop specified fields
        if self.drop_fields:
            # Drop only fields that exist
            drop_fields = [f for f in self.drop_fields if f in df_copy.columns]
            if drop_fields:
                df_copy = df_copy.drop(columns=drop_fields)
                logger.debug(f"Dropped {len(drop_fields)} fields")
        
        logger.info(f"JSON transformation complete, resulting in {len(df_copy.columns)} columns")
        return df_copy
    
    def validate(self) -> bool:
        """
        Validate the transformer configuration.
        
        Returns:
            True if validation succeeds, False otherwise
        """
        # Check for conflicting configurations
        if self.select_fields and self.drop_fields:
            intersection = set(self.select_fields) & set(self.drop_fields)
            if intersection:
                logger.error(f"Fields appear in both select_fields and drop_fields: {intersection}")
                return False
        
        # Check rename conflicts
        rename_outputs = set(self.rename_fields.values())
        if len(rename_outputs) < len(self.rename_fields):
            logger.error("Duplicate target names in rename_fields configuration")
            return False
        
        return True
