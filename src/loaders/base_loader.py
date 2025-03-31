"""
Base Loader module for the Data Warehouse ETL Framework.
Provides abstract base class for all data loaders.
"""
from abc import ABC, abstractmethod
import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)

class BaseLoader(ABC):
    """
    Abstract base class for all data loaders.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the loader with configuration.
        
        Args:
            config: Dictionary containing configuration parameters
        """
        self.config = config
        self.name = self.__class__.__name__
        logger.info(f"Initializing loader: {self.name}")
    
    @abstractmethod
    def load(self, data: Union[pd.DataFrame, List[pd.DataFrame]]) -> bool:
        """
        Load data to the target destination.
        
        Args:
            data: Data to load as DataFrame or list of DataFrames
            
        Returns:
            True if loading was successful, False otherwise
        """
        pass
    
    @abstractmethod
    def validate_destination(self) -> bool:
        """
        Validate that the destination is accessible and ready to receive data.
        
        Returns:
            True if destination is valid, False otherwise
        """
        pass
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the loading operation.
        
        Returns:
            Dictionary with metadata information
        """
        return {
            "loader": self.name,
            "destination": self.config.get("destination", "Unknown")
        }
