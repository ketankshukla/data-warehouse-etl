"""
Base Transformer module for the Data Warehouse ETL Framework.
Provides abstract base class for all transformers.
"""
from abc import ABC, abstractmethod
import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)

class BaseTransformer(ABC):
    """
    Abstract base class for all data transformers.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the transformer with configuration.
        
        Args:
            config: Dictionary containing configuration parameters
        """
        self.config = config
        self.name = self.__class__.__name__
        logger.info(f"Initializing transformer: {self.name}")
    
    @abstractmethod
    def transform(self, data: Union[pd.DataFrame, List[pd.DataFrame]]) -> Union[pd.DataFrame, List[pd.DataFrame]]:
        """
        Transform the input data.
        
        Args:
            data: Input data as DataFrame or list of DataFrames
            
        Returns:
            Transformed data
        """
        pass
    
    def validate_input(self, data: Union[pd.DataFrame, List[pd.DataFrame]]) -> bool:
        """
        Validate that the input data has the expected format.
        
        Args:
            data: Input data to validate
            
        Returns:
            True if data is valid, False otherwise
        """
        if isinstance(data, pd.DataFrame):
            if data.empty:
                logger.warning("Input DataFrame is empty")
                return True  # Empty is still valid
            
            # Check required columns if specified in config
            required_columns = self.config.get("required_columns", [])
            if required_columns and not all(col in data.columns for col in required_columns):
                missing = [col for col in required_columns if col not in data.columns]
                logger.error(f"Missing required columns: {missing}")
                return False
            
            return True
            
        elif isinstance(data, list):
            if not data:
                logger.warning("Input list is empty")
                return True  # Empty is still valid
                
            # Check that all items are DataFrames
            if not all(isinstance(item, pd.DataFrame) for item in data):
                logger.error("Not all items in the list are DataFrames")
                return False
                
            # Check each DataFrame individually
            return all(self.validate_input(df) for df in data)
            
        else:
            logger.error(f"Invalid input type: {type(data)}")
            return False
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the transformation.
        
        Returns:
            Dictionary with metadata information
        """
        return {
            "transformer": self.name,
            "transformations": self.config.get("transformations", [])
        }
