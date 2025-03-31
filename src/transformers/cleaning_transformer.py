"""
Cleaning Transformer module for the Data Warehouse ETL Framework.
Provides functionality for data cleaning operations.
"""
import logging
from typing import Any, Dict, List, Optional, Union, Callable

import pandas as pd
import numpy as np

from .base_transformer import BaseTransformer

logger = logging.getLogger(__name__)

class CleaningTransformer(BaseTransformer):
    """
    Transformer for data cleaning operations.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the cleaning transformer with configuration.
        
        Args:
            config: Dictionary containing configuration parameters including:
                - operations: List of cleaning operations to perform
                - column_mappings: Dictionary mapping columns to operations
                - drop_columns: List of columns to drop
                - drop_na_threshold: Threshold for dropping rows with NA values
                - string_operations: Dictionary of string cleaning operations
                - numeric_operations: Dictionary of numeric cleaning operations
                - date_operations: Dictionary of date cleaning operations
                - custom_operations: Dictionary of custom cleaning functions
        """
        super().__init__(config)
        self.operations = config.get("operations", [])
        self.column_mappings = config.get("column_mappings", {})
        self.drop_columns = config.get("drop_columns", [])
        self.drop_na_threshold = config.get("drop_na_threshold")
        self.string_operations = config.get("string_operations", {})
        self.numeric_operations = config.get("numeric_operations", {})
        self.date_operations = config.get("date_operations", {})
        self.custom_operations = config.get("custom_operations", {})
    
    def transform(self, data: Union[pd.DataFrame, List[pd.DataFrame]]) -> Union[pd.DataFrame, List[pd.DataFrame]]:
        """
        Apply cleaning transformations to the input data.
        
        Args:
            data: Input data as DataFrame or list of DataFrames
            
        Returns:
            Cleaned data
        """
        if not self.validate_input(data):
            raise ValueError("Invalid input data for cleaning transformer")
        
        # Handle list of DataFrames
        if isinstance(data, list):
            return [self.transform(df) for df in data]
        
        # Copy the DataFrame to avoid modifying the original
        df = data.copy()
        
        # Apply operations based on configuration
        for operation in self.operations:
            try:
                if operation == "drop_columns":
                    df = self._drop_columns(df)
                
                elif operation == "drop_duplicates":
                    df = self._drop_duplicates(df)
                
                elif operation == "drop_na":
                    df = self._drop_na(df)
                
                elif operation == "fill_na":
                    df = self._fill_na(df)
                
                elif operation == "string_cleaning":
                    df = self._apply_string_operations(df)
                
                elif operation == "numeric_cleaning":
                    df = self._apply_numeric_operations(df)
                
                elif operation == "date_cleaning":
                    df = self._apply_date_operations(df)
                
                elif operation == "custom_operations":
                    df = self._apply_custom_operations(df)
                
                else:
                    logger.warning(f"Unknown cleaning operation: {operation}")
                
            except Exception as e:
                logger.error(f"Error applying cleaning operation '{operation}': {str(e)}")
                raise
        
        logger.info(f"Successfully applied {len(self.operations)} cleaning operations")
        return df
    
    def _drop_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Drop specified columns from DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with columns dropped
        """
        if not self.drop_columns:
            return df
        
        # Only drop columns that exist in the DataFrame
        columns_to_drop = [col for col in self.drop_columns if col in df.columns]
        if columns_to_drop:
            logger.info(f"Dropping columns: {columns_to_drop}")
            df = df.drop(columns=columns_to_drop)
        
        return df
    
    def _drop_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Drop duplicate rows from DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with duplicates removed
        """
        subset = self.config.get("duplicate_subset")
        keep = self.config.get("duplicate_keep", "first")
        
        before_count = len(df)
        df = df.drop_duplicates(subset=subset, keep=keep)
        after_count = len(df)
        
        if before_count > after_count:
            logger.info(f"Removed {before_count - after_count} duplicate rows")
        
        return df
    
    def _drop_na(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Drop rows with NA values based on threshold.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with NA rows dropped
        """
        subset = self.config.get("na_subset")
        how = self.config.get("na_how", "any")
        
        before_count = len(df)
        
        if self.drop_na_threshold is not None:
            # Drop rows where percentage of NAs exceeds threshold
            df = df.dropna(thresh=int(len(df.columns) * (1 - self.drop_na_threshold)))
        else:
            # Use standard dropna with subset and how parameters
            df = df.dropna(subset=subset, how=how)
        
        after_count = len(df)
        
        if before_count > after_count:
            logger.info(f"Removed {before_count - after_count} rows with NA values")
        
        return df
    
    def _fill_na(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fill NA values in DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with NA values filled
        """
        fill_values = self.config.get("fill_values", {})
        fill_method = self.config.get("fill_method")
        
        if fill_values:
            logger.info(f"Filling NA values with specified values for {len(fill_values)} columns")
            df = df.fillna(value=fill_values)
        
        elif fill_method:
            logger.info(f"Filling NA values using method: {fill_method}")
            df = df.fillna(method=fill_method)
        
        return df
    
    def _apply_string_operations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply string cleaning operations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with string operations applied
        """
        if not self.string_operations:
            return df
        
        for column, operations in self.string_operations.items():
            if column not in df.columns:
                logger.warning(f"Column '{column}' not found for string operations")
                continue
            
            for operation, params in operations.items():
                try:
                    if operation == "strip":
                        df[column] = df[column].str.strip()
                    
                    elif operation == "lower":
                        df[column] = df[column].str.lower()
                    
                    elif operation == "upper":
                        df[column] = df[column].str.upper()
                    
                    elif operation == "title":
                        df[column] = df[column].str.title()
                    
                    elif operation == "replace":
                        pattern = params.get("pattern", "")
                        replacement = params.get("replacement", "")
                        regex = params.get("regex", False)
                        df[column] = df[column].str.replace(pattern, replacement, regex=regex)
                    
                    elif operation == "extract":
                        pattern = params.get("pattern", "")
                        df[column] = df[column].str.extract(pattern, expand=False)
                    
                    else:
                        logger.warning(f"Unknown string operation: {operation}")
                    
                except Exception as e:
                    logger.error(f"Error applying string operation '{operation}' to column '{column}': {str(e)}")
        
        return df
    
    def _apply_numeric_operations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply numeric cleaning operations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with numeric operations applied
        """
        if not self.numeric_operations:
            return df
        
        for column, operations in self.numeric_operations.items():
            if column not in df.columns:
                logger.warning(f"Column '{column}' not found for numeric operations")
                continue
            
            for operation, params in operations.items():
                try:
                    if operation == "convert":
                        errors = params.get("errors", "coerce")
                        df[column] = pd.to_numeric(df[column], errors=errors)
                    
                    elif operation == "clip":
                        lower = params.get("lower")
                        upper = params.get("upper")
                        df[column] = df[column].clip(lower=lower, upper=upper)
                    
                    elif operation == "round":
                        decimals = params.get("decimals", 0)
                        df[column] = df[column].round(decimals=decimals)
                    
                    elif operation == "scale":
                        factor = params.get("factor", 1)
                        df[column] = df[column] * factor
                    
                    elif operation == "remove_outliers":
                        method = params.get("method", "iqr")
                        if method == "iqr":
                            q1 = df[column].quantile(0.25)
                            q3 = df[column].quantile(0.75)
                            iqr = q3 - q1
                            lower_bound = q1 - (1.5 * iqr)
                            upper_bound = q3 + (1.5 * iqr)
                            df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
                        elif method == "zscore":
                            threshold = params.get("threshold", 3)
                            mean = df[column].mean()
                            std = df[column].std()
                            df = df[abs((df[column] - mean) / std) <= threshold]
                    
                    else:
                        logger.warning(f"Unknown numeric operation: {operation}")
                    
                except Exception as e:
                    logger.error(f"Error applying numeric operation '{operation}' to column '{column}': {str(e)}")
        
        return df
    
    def _apply_date_operations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply date cleaning operations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with date operations applied
        """
        if not self.date_operations:
            return df
        
        for column, operations in self.date_operations.items():
            if column not in df.columns:
                logger.warning(f"Column '{column}' not found for date operations")
                continue
            
            for operation, params in operations.items():
                try:
                    if operation == "convert":
                        format = params.get("format")
                        errors = params.get("errors", "coerce")
                        df[column] = pd.to_datetime(df[column], format=format, errors=errors)
                    
                    elif operation == "extract_component":
                        component = params.get("component")
                        new_column = params.get("new_column", f"{column}_{component}")
                        
                        if component == "year":
                            df[new_column] = df[column].dt.year
                        elif component == "month":
                            df[new_column] = df[column].dt.month
                        elif component == "day":
                            df[new_column] = df[column].dt.day
                        elif component == "weekday":
                            df[new_column] = df[column].dt.weekday
                        elif component == "quarter":
                            df[new_column] = df[column].dt.quarter
                        else:
                            logger.warning(f"Unknown date component: {component}")
                    
                    else:
                        logger.warning(f"Unknown date operation: {operation}")
                    
                except Exception as e:
                    logger.error(f"Error applying date operation '{operation}' to column '{column}': {str(e)}")
        
        return df
    
    def _apply_custom_operations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply custom cleaning operations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with custom operations applied
        """
        if not self.custom_operations:
            return df
        
        # This is just a placeholder since actual custom functions
        # would need to be defined elsewhere and imported
        
        logger.warning("Custom operations not implemented - requires external function definitions")
        
        return df
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the cleaning transformation.
        
        Returns:
            Dictionary with metadata information
        """
        metadata = super().get_metadata()
        metadata.update({
            "operations_applied": self.operations,
            "columns_cleaned": list(set().union(
                self.drop_columns,
                self.string_operations.keys(),
                self.numeric_operations.keys(),
                self.date_operations.keys(),
                self.custom_operations.keys()
            ))
        })
        
        return metadata
