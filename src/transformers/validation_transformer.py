"""
Validation Transformer module for the Data Warehouse ETL Framework.
Provides functionality for data validation operations.
"""
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, Tuple, Callable

import pandas as pd
import numpy as np

from .base_transformer import BaseTransformer

logger = logging.getLogger(__name__)

class ValidationTransformer(BaseTransformer):
    """
    Transformer for data validation operations.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the validation transformer with configuration.
        
        Args:
            config: Dictionary containing configuration parameters including:
                - validation_rules: Dictionary of validation rules by column
                - error_handling: How to handle validation errors ('drop', 'flag', 'fill', 'raise')
                - error_values: Values to use when filling invalid data
                - add_validation_columns: Whether to add columns indicating validation status
                - validation_column_suffix: Suffix for validation columns
                - create_error_report: Whether to create a detailed validation error report
        """
        super().__init__(config)
        self.validation_rules = config.get("validation_rules", {})
        self.error_handling = config.get("error_handling", "flag")
        self.error_values = config.get("error_values", {})
        self.add_validation_columns = config.get("add_validation_columns", True)
        self.validation_column_suffix = config.get("validation_column_suffix", "_valid")
        self.create_error_report = config.get("create_error_report", False)
        self.validation_results = {}
    
    def transform(self, data: Union[pd.DataFrame, List[pd.DataFrame]]) -> Union[pd.DataFrame, List[pd.DataFrame]]:
        """
        Apply validation transformations to the input data.
        
        Args:
            data: Input data as DataFrame or list of DataFrames
            
        Returns:
            Validated data
        """
        if not self.validate_input(data):
            raise ValueError("Invalid input data for validation transformer")
        
        # Handle list of DataFrames
        if isinstance(data, list):
            return [self.transform(df) for df in data]
        
        # Copy the DataFrame to avoid modifying the original
        df = data.copy()
        
        # Reset validation results
        self.validation_results = {
            "total_records": len(df),
            "invalid_records": 0,
            "validation_by_column": {},
            "error_details": {}
        }
        
        # Apply validation rules to each column
        for column, rules in self.validation_rules.items():
            if column not in df.columns:
                logger.warning(f"Column '{column}' not found for validation")
                continue
            
            # Initialize validation results for this column
            self.validation_results["validation_by_column"][column] = {
                "total_checks": len(rules),
                "total_failures": 0,
                "failure_rate": 0.0
            }
            
            # Apply each validation rule
            column_valid = pd.Series(True, index=df.index)
            column_errors = []
            
            for rule in rules:
                rule_type = rule.get("type")
                rule_params = rule.get("params", {})
                
                try:
                    # Apply the validation rule
                    valid, error_msg = self._apply_validation_rule(df, column, rule_type, rule_params)
                    
                    # Update the combined validation result for this column
                    column_valid = column_valid & valid
                    
                    # Count validation failures
                    failure_count = (~valid).sum()
                    if failure_count > 0:
                        column_errors.append(error_msg)
                        self.validation_results["validation_by_column"][column]["total_failures"] += 1
                    
                except Exception as e:
                    logger.error(f"Error applying validation rule '{rule_type}' to column '{column}': {str(e)}")
                    raise
            
            # Calculate failure rate for this column
            total_checks = len(rules)
            total_failures = self.validation_results["validation_by_column"][column]["total_failures"]
            if total_checks > 0:
                failure_rate = total_failures / total_checks
                self.validation_results["validation_by_column"][column]["failure_rate"] = failure_rate
            
            # Store validation errors for this column
            if column_errors:
                self.validation_results["error_details"][column] = column_errors
            
            # Apply error handling for this column
            df = self._handle_validation_errors(df, column, column_valid)
            
            # Add validation column if requested
            if self.add_validation_columns:
                validation_column = f"{column}{self.validation_column_suffix}"
                df[validation_column] = column_valid
        
        # Update total invalid records
        if self.add_validation_columns:
            # Count rows with at least one validation failure
            validation_columns = [f"{col}{self.validation_column_suffix}" for col in self.validation_rules.keys() 
                                if f"{col}{self.validation_column_suffix}" in df.columns]
            if validation_columns:
                all_valid = df[validation_columns].all(axis=1)
                self.validation_results["invalid_records"] = (~all_valid).sum()
        
        # Create error report if requested
        if self.create_error_report:
            self._create_error_report(df)
        
        logger.info(f"Validated {len(self.validation_rules)} columns with "
                   f"{self.validation_results['invalid_records']} invalid records detected")
        
        return df
    
    def _apply_validation_rule(self, df: pd.DataFrame, column: str, rule_type: str, 
                              rule_params: Dict[str, Any]) -> Tuple[pd.Series, str]:
        """
        Apply a validation rule to a column.
        
        Args:
            df: Input DataFrame
            column: Column to validate
            rule_type: Type of validation rule
            rule_params: Parameters for the validation rule
            
        Returns:
            Tuple of (validation result Series, error message)
        """
        error_msg = f"Failed {rule_type} validation"
        
        # Handle different validation rule types
        if rule_type == "not_null":
            result = ~df[column].isna()
            error_msg = "Contains null values"
        
        elif rule_type == "unique":
            result = ~df[column].duplicated(keep=rule_params.get("keep", False))
            error_msg = "Contains duplicate values"
        
        elif rule_type == "min_length":
            min_length = rule_params.get("length", 1)
            # Handle both string and list/array columns
            if df[column].dtype == 'object':
                result = df[column].str.len() >= min_length
            else:
                result = df[column].apply(lambda x: len(x) if hasattr(x, '__len__') else True) >= min_length
            result = result.fillna(False)  # NaN values fail this validation
            error_msg = f"Length less than {min_length}"
        
        elif rule_type == "max_length":
            max_length = rule_params.get("length", 100)
            # Handle both string and list/array columns
            if df[column].dtype == 'object':
                result = df[column].str.len() <= max_length
            else:
                result = df[column].apply(lambda x: len(x) if hasattr(x, '__len__') else True) <= max_length
            result = result.fillna(True)  # NaN values pass this validation (they have no length)
            error_msg = f"Length greater than {max_length}"
        
        elif rule_type == "min_value":
            min_value = rule_params.get("value")
            result = df[column] >= min_value
            result = result.fillna(False)  # NaN values fail this validation
            error_msg = f"Value less than {min_value}"
        
        elif rule_type == "max_value":
            max_value = rule_params.get("value")
            result = df[column] <= max_value
            result = result.fillna(False)  # NaN values fail this validation
            error_msg = f"Value greater than {max_value}"
        
        elif rule_type == "in_set":
            valid_values = rule_params.get("values", [])
            result = df[column].isin(valid_values)
            result = result.fillna(False)  # NaN values fail this validation
            error_msg = f"Value not in set {valid_values}"
        
        elif rule_type == "regex":
            pattern = rule_params.get("pattern", "")
            result = df[column].astype(str).str.match(pattern)
            result = result.fillna(False)  # NaN values fail this validation
            error_msg = f"Value does not match pattern {pattern}"
        
        elif rule_type == "date_range":
            min_date = rule_params.get("min_date")
            max_date = rule_params.get("max_date")
            
            # Convert column to datetime if not already
            if not pd.api.types.is_datetime64_dtype(df[column]):
                date_format = rule_params.get("format")
                try:
                    column_dates = pd.to_datetime(df[column], format=date_format, errors='coerce')
                except:
                    logger.error(f"Could not convert column '{column}' to datetime")
                    return pd.Series(False, index=df.index), "Invalid date format"
            else:
                column_dates = df[column]
            
            # Apply date range check
            if min_date and max_date:
                min_date = pd.to_datetime(min_date)
                max_date = pd.to_datetime(max_date)
                result = (column_dates >= min_date) & (column_dates <= max_date)
                error_msg = f"Date not in range {min_date} to {max_date}"
            elif min_date:
                min_date = pd.to_datetime(min_date)
                result = column_dates >= min_date
                error_msg = f"Date before {min_date}"
            elif max_date:
                max_date = pd.to_datetime(max_date)
                result = column_dates <= max_date
                error_msg = f"Date after {max_date}"
            else:
                result = ~column_dates.isna()
                error_msg = "Invalid date"
            
            result = result.fillna(False)  # NaN values fail this validation
        
        elif rule_type == "custom":
            # Custom validations would require external function definitions
            logger.warning("Custom validation not implemented - requires external function definition")
            result = pd.Series(True, index=df.index)
            error_msg = "Custom validation not implemented"
        
        else:
            logger.warning(f"Unknown validation rule type: {rule_type}")
            result = pd.Series(True, index=df.index)
            error_msg = f"Unknown validation rule: {rule_type}"
        
        return result, error_msg
    
    def _handle_validation_errors(self, df: pd.DataFrame, column: str, valid: pd.Series) -> pd.DataFrame:
        """
        Handle validation errors according to the error handling strategy.
        
        Args:
            df: Input DataFrame
            column: Column being validated
            valid: Series indicating which values are valid
            
        Returns:
            DataFrame with errors handled
        """
        # Get invalid mask
        invalid = ~valid
        
        if invalid.sum() == 0:
            # No invalid values to handle
            return df
        
        # Handle errors based on the configured strategy
        if self.error_handling == "drop":
            # Drop rows with invalid values
            df = df[valid].copy()
            logger.info(f"Dropped {invalid.sum()} rows due to invalid values in column '{column}'")
        
        elif self.error_handling == "fill":
            # Fill invalid values with a specified value
            if column in self.error_values:
                fill_value = self.error_values[column]
                df.loc[invalid, column] = fill_value
                logger.info(f"Filled {invalid.sum()} invalid values in column '{column}' with {fill_value}")
            else:
                logger.warning(f"No fill value specified for column '{column}', invalid values not changed")
        
        elif self.error_handling == "raise":
            # Raise an exception for invalid values
            if invalid.sum() > 0:
                sample_invalid = df.loc[invalid, column].head(5).tolist()
                error_msg = (f"Validation failed for column '{column}': "
                            f"{invalid.sum()} invalid values found. Sample: {sample_invalid}")
                logger.error(error_msg)
                raise ValueError(error_msg)
        
        elif self.error_handling == "flag":
            # Just flag the errors (default behavior, handled by adding validation columns)
            pass
        
        else:
            logger.warning(f"Unknown error handling strategy: {self.error_handling}")
        
        return df
    
    def _create_error_report(self, df: pd.DataFrame) -> None:
        """
        Create a detailed validation error report.
        
        Args:
            df: Input DataFrame after validation
        """
        # We could implement this to generate a detailed report
        # For now, we just log a summary
        logger.info("Validation summary:")
        for column, results in self.validation_results["validation_by_column"].items():
            logger.info(f"  Column '{column}': {results['total_failures']} of {results['total_checks']} "
                       f"checks failed ({results['failure_rate']*100:.1f}%)")
        
        logger.info(f"Total invalid records: {self.validation_results['invalid_records']} "
                   f"of {self.validation_results['total_records']} "
                   f"({self.validation_results['invalid_records']/self.validation_results['total_records']*100:.1f}%)")
    
    def get_validation_results(self) -> Dict[str, Any]:
        """
        Get the validation results.
        
        Returns:
            Dictionary with validation results
        """
        return self.validation_results
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the validation transformation.
        
        Returns:
            Dictionary with metadata information
        """
        metadata = super().get_metadata()
        metadata.update({
            "columns_validated": list(self.validation_rules.keys()),
            "error_handling": self.error_handling,
            "invalid_records": self.validation_results.get("invalid_records", 0),
            "total_records": self.validation_results.get("total_records", 0)
        })
        
        return metadata
