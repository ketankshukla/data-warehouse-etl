"""
Configuration Manager module for the Data Warehouse ETL Framework.
Provides functionality to load and validate configuration from YAML files.
"""
import logging
import os
from typing import Any, Dict, List, Optional, Union

import yaml
from yaml.parser import ParserError

logger = logging.getLogger(__name__)

class ConfigManager:
    """
    Manager for loading and validating ETL configuration.
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize the config manager.
        
        Args:
            config_path: Path to the YAML configuration file
        """
        self.config_path = config_path
        self.config = {}
        
        if config_path:
            self.load_config(config_path)
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Load configuration from a YAML file.
        
        Args:
            config_path: Path to the YAML configuration file
            
        Returns:
            Dictionary containing the configuration
            
        Raises:
            FileNotFoundError: If the config file does not exist
            ParserError: If the YAML file has syntax errors
            Exception: For other loading errors
        """
        if not os.path.exists(config_path):
            logger.error(f"Configuration file not found: {config_path}")
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        try:
            logger.info(f"Loading configuration from: {config_path}")
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
                
            if not self.config:
                logger.warning(f"Empty configuration loaded from: {config_path}")
                self.config = {}
            
            self.config_path = config_path
            logger.info(f"Successfully loaded configuration")
            
            return self.config
            
        except ParserError as e:
            logger.error(f"YAML syntax error in configuration file: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error loading configuration file: {str(e)}")
            raise
    
    def get_config(self) -> Dict[str, Any]:
        """
        Get the loaded configuration.
        
        Returns:
            Dictionary containing the configuration
        """
        return self.config
    
    def validate_config(self, required_sections: List[str] = None) -> bool:
        """
        Validate that the configuration has the required sections.
        
        Args:
            required_sections: List of section names required in the config
            
        Returns:
            True if the configuration is valid, False otherwise
        """
        if not self.config:
            logger.error("No configuration loaded")
            return False
        
        if required_sections:
            missing_sections = [section for section in required_sections if section not in self.config]
            if missing_sections:
                logger.error(f"Missing required configuration sections: {missing_sections}")
                return False
        
        # Check for common ETL sections if required_sections not specified
        if not required_sections:
            common_sections = ["extractors", "transformers", "loaders", "pipeline"]
            missing_sections = [section for section in common_sections if section not in self.config]
            if missing_sections:
                logger.warning(f"Common configuration sections missing: {missing_sections}")
        
        return True
    
    def get_section(self, section_name: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific section from the configuration.
        
        Args:
            section_name: Name of the configuration section
            
        Returns:
            Dictionary containing the section or None if not found
        """
        if not self.config:
            logger.warning("Attempting to get section from empty configuration")
            return None
        
        section = self.config.get(section_name)
        if section is None:
            logger.warning(f"Section '{section_name}' not found in configuration")
        
        return section
    
    def get_nested_value(self, path: str, default: Any = None) -> Any:
        """
        Get a nested value from the configuration using dot notation.
        
        Args:
            path: Path to the value in dot notation (e.g., 'extractors.csv.file_path')
            default: Default value to return if the path is not found
            
        Returns:
            The value at the specified path or the default if not found
        """
        if not self.config:
            return default
        
        parts = path.split('.')
        current = self.config
        
        for part in parts:
            if not isinstance(current, dict) or part not in current:
                return default
            current = current[part]
        
        return current
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the configuration to a dictionary.
        
        Returns:
            Dictionary representation of the configuration
        """
        return dict(self.config)
    
    def __str__(self) -> str:
        """
        Get a string representation of the configuration.
        
        Returns:
            String representation of the configuration
        """
        return f"ConfigManager(path={self.config_path}, sections={list(self.config.keys())})"
