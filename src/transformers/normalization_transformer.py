"""
Normalization transformer for the Data Warehouse ETL Framework.
Provides functionality to normalize data.
"""
import logging
from typing import Dict, Any, List, Union

import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler, StandardScaler

from src.transformers.base_transformer import BaseTransformer
from src.utils.logging_utils import get_etl_logger

class NormalizationTransformer(BaseTransformer):
    """
    Transformer for data normalization operations.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize normalization transformer with configuration.
        
        Args:
            config: Dictionary containing configuration parameters
        """
        super().__init__(config)
        self.logger = get_etl_logger(__name__, component="NormalizationTransformer")
        self.logger.info("Initializing normalization transformer")
        
        # Get normalization methods configuration
        self.methods = self.config.get("methods", [])
        if not self.methods:
            self.logger.warning("No normalization methods configured")
        
        self.logger.debug(f"Normalization transformer configured with {len(self.methods)} methods")
    
    def transform(self, data: Union[pd.DataFrame, List[pd.DataFrame]]) -> Union[pd.DataFrame, List[pd.DataFrame]]:
        """
        Apply normalization transformations to the data.
        
        Args:
            data: DataFrame or list of DataFrames to transform
            
        Returns:
            Transformed DataFrame or list of DataFrames
        """
        self.logger.info("Applying normalization transformations")
        
        # Handle list of DataFrames
        if isinstance(data, list):
            if not data:
                self.logger.warning("Empty data list provided, nothing to transform")
                return data
            
            # Apply transformations to each DataFrame
            transformed_data = []
            for i, df in enumerate(data):
                if not isinstance(df, pd.DataFrame):
                    self.logger.error(f"Item {i} in data list is not a DataFrame")
                    continue
                    
                transformed_df = self._apply_transformations(df)
                transformed_data.append(transformed_df)
                
            return transformed_data
            
        # Handle single DataFrame
        elif isinstance(data, pd.DataFrame):
            if data.empty:
                self.logger.warning("Empty DataFrame provided, nothing to transform")
                return data
                
            return self._apply_transformations(data)
            
        else:
            self.logger.error(f"Unsupported data type: {type(data)}")
            return data
    
    def _apply_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all configured normalization methods to a DataFrame.
        
        Args:
            df: DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        result_df = df.copy()
        
        for method_config in self.methods:
            method_type = next(iter(method_config))
            method_params = method_config[method_type]
            
            # Apply the appropriate method
            if method_type == "numeric_scaling":
                result_df = self._apply_numeric_scaling(result_df, method_params)
            elif method_type == "date_format":
                result_df = self._apply_date_format(result_df, method_params)
            elif method_type == "standard_scaler":
                result_df = self._apply_standard_scaler(result_df, method_params)
            elif method_type == "log_transform":
                result_df = self._apply_log_transform(result_df, method_params)
            else:
                self.logger.warning(f"Unknown normalization method: {method_type}")
        
        return result_df
    
    def _apply_numeric_scaling(self, df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
        """
        Apply numeric scaling to specified columns.
        
        Args:
            df: DataFrame to transform
            params: Scaling parameters including columns to scale
            
        Returns:
            Transformed DataFrame
        """
        result_df = df.copy()
        columns = params.get("columns", {})
        
        for column, config in columns.items():
            if column not in result_df.columns:
                self.logger.warning(f"Column '{column}' not found in DataFrame")
                continue
                
            # Skip non-numeric columns
            if not pd.api.types.is_numeric_dtype(result_df[column]):
                self.logger.warning(f"Column '{column}' is not numeric, skipping scaling")
                continue
                
            method = config.get("method", "min_max")
            
            try:
                if method == "min_max":
                    feature_range = config.get("feature_range", (0, 1))
                    scaler = MinMaxScaler(feature_range=feature_range)
                    result_df[column] = scaler.fit_transform(result_df[[column]])
                    self.logger.debug(f"Applied min-max scaling to column '{column}' with range {feature_range}")
                
                elif method == "standard":
                    scaler = StandardScaler()
                    result_df[column] = scaler.fit_transform(result_df[[column]])
                    self.logger.debug(f"Applied standard scaling to column '{column}'")
                
                elif method == "manual":
                    min_val = config.get("min", df[column].min())
                    max_val = config.get("max", df[column].max())
                    result_df[column] = (result_df[column] - min_val) / (max_val - min_val)
                    self.logger.debug(f"Applied manual scaling to column '{column}' with min={min_val}, max={max_val}")
                
                else:
                    self.logger.warning(f"Unknown scaling method: {method}")
            
            except Exception as e:
                self.logger.error(f"Error scaling column '{column}': {str(e)}")
        
        return result_df
    
    def _apply_date_format(self, df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
        """
        Standardize date formats in specified columns.
        
        Args:
            df: DataFrame to transform
            params: Date formatting parameters including columns to format
            
        Returns:
            Transformed DataFrame
        """
        result_df = df.copy()
        columns = params.get("columns", {})
        
        for column, format_str in columns.items():
            if column not in result_df.columns:
                self.logger.warning(f"Column '{column}' not found in DataFrame")
                continue
                
            try:
                # Convert to datetime if not already
                if not pd.api.types.is_datetime64_dtype(result_df[column]):
                    result_df[column] = pd.to_datetime(result_df[column], errors='coerce')
                
                # Format date according to specified format
                result_df[column] = result_df[column].dt.strftime(format_str)
                self.logger.debug(f"Applied date formatting to column '{column}' with format '{format_str}'")
            
            except Exception as e:
                self.logger.error(f"Error formatting dates in column '{column}': {str(e)}")
        
        return result_df
    
    def _apply_standard_scaler(self, df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
        """
        Apply standard scaling to specified columns.
        
        Args:
            df: DataFrame to transform
            params: Scaling parameters including columns to scale
            
        Returns:
            Transformed DataFrame
        """
        result_df = df.copy()
        columns = params.get("columns", [])
        
        if not columns:
            self.logger.warning("No columns specified for standard scaling")
            return result_df
            
        try:
            # Filter out non-numeric columns
            numeric_columns = [col for col in columns if col in result_df.columns and pd.api.types.is_numeric_dtype(result_df[col])]
            
            if not numeric_columns:
                self.logger.warning("No valid numeric columns found for standard scaling")
                return result_df
                
            # Apply standard scaling to selected columns
            scaler = StandardScaler()
            result_df[numeric_columns] = scaler.fit_transform(result_df[numeric_columns])
            
            self.logger.debug(f"Applied standard scaling to columns: {numeric_columns}")
            
        except Exception as e:
            self.logger.error(f"Error applying standard scaling: {str(e)}")
            
        return result_df
    
    def _apply_log_transform(self, df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
        """
        Apply logarithmic transformation to specified columns.
        
        Args:
            df: DataFrame to transform
            params: Log transform parameters including columns to transform
            
        Returns:
            Transformed DataFrame
        """
        result_df = df.copy()
        columns = params.get("columns", [])
        base = params.get("base", np.e)  # Default to natural log
        
        for column in columns:
            if column not in result_df.columns:
                self.logger.warning(f"Column '{column}' not found in DataFrame")
                continue
                
            # Skip non-numeric columns
            if not pd.api.types.is_numeric_dtype(result_df[column]):
                self.logger.warning(f"Column '{column}' is not numeric, skipping log transform")
                continue
                
            try:
                # Handle zero or negative values if specified
                handle_zeros = params.get("handle_zeros", True)
                
                if handle_zeros:
                    # Add small constant to avoid log(0)
                    min_positive = result_df[result_df[column] > 0][column].min() if any(result_df[column] > 0) else 1
                    epsilon = min_positive / 100
                    result_df[column] = result_df[column] + epsilon
                
                # Apply log transform
                if base == np.e:
                    result_df[column] = np.log(result_df[column])
                    self.logger.debug(f"Applied natural log transform to column '{column}'")
                else:
                    result_df[column] = np.log(result_df[column]) / np.log(base)
                    self.logger.debug(f"Applied log base {base} transform to column '{column}'")
            
            except Exception as e:
                self.logger.error(f"Error applying log transform to column '{column}': {str(e)}")
        
        return result_df
