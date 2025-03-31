"""
Base Extractor module for the Data Warehouse ETL Framework.
Provides abstract base class for all extractors.
"""
from abc import ABC, abstractmethod
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

class BaseExtractor(ABC):
    """
    Abstract base class for all data extractors.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the extractor with configuration.
        
        Args:
            config: Dictionary containing configuration parameters
        """
        self.config = config
        self.name = self.__class__.__name__
        logger.info(f"Initializing extractor: {self.name}")
    
    @abstractmethod
    def extract(self) -> Any:
        """
        Extract data from the source.
        
        Returns:
            Extracted data in a format suitable for transformation
        """
        pass
    
    @abstractmethod
    def validate_source(self) -> bool:
        """
        Validate that the source is accessible and has expected format.
        
        Returns:
            True if source is valid, False otherwise
        """
        pass
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the extracted data.
        
        Returns:
            Dictionary with metadata information
        """
        return {
            "extractor": self.name,
            "source": self.config.get("source", "Unknown"),
            "timestamp": None  # Will be populated during extraction
        }
