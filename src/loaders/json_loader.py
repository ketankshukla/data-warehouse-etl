"""
JSON File Loader module for the Data Warehouse ETL Framework.
Provides functionality to load data into JSON files.
"""
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import numpy as np

from .base_loader import BaseLoader

logger = logging.getLogger(__name__)

class JSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder to handle Pandas and NumPy data types.
    """
    def default(self, obj):
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, pd.Series):
            return obj.tolist()
        if pd.isna(obj):
            return None
        return super().default(obj)

class JSONLoader(BaseLoader):
    """
    Loader for JSON file destinations.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the JSON loader with configuration.
        
        Args:
            config: Dictionary containing configuration parameters including:
                - file_path: Path to the target JSON file
                - encoding: File encoding (default: 'utf-8')
                - orient: JSON orientation for Pandas DataFrame conversion (default: 'records')
                - date_format: Format string for date columns (default: 'iso')
                - indent: Indentation level for pretty printing (default: 2)
                - create_dirs: Whether to create parent directories if they don't exist (default: True)
                - force_ascii: Whether to force ASCII output (default: False)
                - mode: Write mode ('w' for overwrite, 'a' for append) (default: 'w')
        """
        super().__init__(config)
        self.file_path = config.get("file_path")
        self.encoding = config.get("encoding", "utf-8")
        self.orient = config.get("orient", "records")
        self.date_format = config.get("date_format", "iso")
        self.indent = config.get("indent", 2)
        self.create_dirs = config.get("create_dirs", True)
        self.force_ascii = config.get("force_ascii", False)
        self.mode = config.get("mode", "w")
    
    def validate_destination(self) -> bool:
        """
        Validate that the JSON file destination is accessible.
        
        Returns:
            True if the destination is valid, False otherwise
        """
        if not self.file_path:
            logger.error("File path not provided in configuration")
            return False
        
        # Check if parent directory exists
        parent_dir = os.path.dirname(self.file_path)
        if parent_dir and not os.path.exists(parent_dir):
            if not self.create_dirs:
                logger.error(f"Parent directory does not exist: {parent_dir}")
                return False
            # We'll create the directory during load
        
        # If file exists, check if it has write permission
        if os.path.exists(self.file_path) and not os.access(self.file_path, os.W_OK):
            logger.error(f"No write permission for file: {self.file_path}")
            return False
        
        return True
    
    def _ensure_parent_directory(self) -> bool:
        """
        Ensure that the parent directory of the output file exists.
        
        Returns:
            True if directory exists or was created, False otherwise
        """
        parent_dir = os.path.dirname(self.file_path)
        if not parent_dir:
            return True  # File is in current directory
        
        if os.path.exists(parent_dir):
            return True  # Directory already exists
        
        if self.create_dirs:
            try:
                os.makedirs(parent_dir, exist_ok=True)
                logger.info(f"Created directory: {parent_dir}")
                return True
            except Exception as e:
                logger.error(f"Error creating directory '{parent_dir}': {str(e)}")
                return False
        else:
            logger.error(f"Parent directory does not exist: {parent_dir}")
            return False
    
    def _append_json_data(self, data_list: List[Dict]) -> bool:
        """
        Append data to an existing JSON file.
        
        Args:
            data_list: List of data records to append
            
        Returns:
            True if append was successful, False otherwise
        """
        try:
            # Read existing data
            existing_data = []
            if os.path.exists(self.file_path) and os.path.getsize(self.file_path) > 0:
                with open(self.file_path, 'r', encoding=self.encoding) as f:
                    existing_data = json.load(f)
                
                # Ensure existing data is a list
                if not isinstance(existing_data, list):
                    existing_data = [existing_data]
            
            # Append new data
            combined_data = existing_data + data_list
            
            # Write back to file
            with open(self.file_path, 'w', encoding=self.encoding) as f:
                json.dump(combined_data, f, cls=JSONEncoder, indent=self.indent, ensure_ascii=self.force_ascii)
            
            return True
            
        except Exception as e:
            logger.error(f"Error appending to JSON file: {str(e)}")
            return False
    
    def load(self, data: Union[pd.DataFrame, List[pd.DataFrame]]) -> bool:
        """
        Load data to a JSON file.
        
        Args:
            data: Data to load as DataFrame or list of DataFrames
            
        Returns:
            True if loading was successful, False otherwise
        """
        if not self.validate_destination():
            raise ValueError("Invalid or inaccessible JSON file destination")
        
        try:
            # Ensure parent directory exists
            if not self._ensure_parent_directory():
                raise ValueError(f"Could not create parent directory for file: {self.file_path}")
            
            # Handle list of DataFrames (concatenate or load individually)
            if isinstance(data, list):
                if not data:
                    logger.warning("Empty list of DataFrames provided, nothing to load")
                    return True
                
                # Concatenate DataFrames
                data = pd.concat(data, ignore_index=True)
            
            # Check if DataFrame is empty
            if data.empty:
                logger.warning("Empty DataFrame provided, nothing to load")
                if self.mode == 'w':
                    # Create an empty JSON array in write mode
                    with open(self.file_path, 'w', encoding=self.encoding) as f:
                        f.write('[]')
                return True
            
            # Convert DataFrame to JSON format
            if self.mode == 'a' and os.path.exists(self.file_path) and os.path.getsize(self.file_path) > 0:
                # For append mode, convert to records and append to existing file
                data_records = data.to_dict(orient='records')
                success = self._append_json_data(data_records)
                if not success:
                    raise ValueError("Failed to append data to JSON file")
            else:
                # For write mode or new file, directly convert and write
                with open(self.file_path, 'w', encoding=self.encoding) as f:
                    if self.orient == 'records':
                        # Handle records format specially for better control
                        records = data.to_dict(orient='records')
                        json.dump(records, f, cls=JSONEncoder, indent=self.indent, ensure_ascii=self.force_ascii)
                    else:
                        # Use pandas to_json for other formats
                        f.write(data.to_json(
                            orient=self.orient,
                            date_format=self.date_format,
                            indent=self.indent,
                            force_ascii=self.force_ascii
                        ))
            
            # Log success
            rows_written = len(data)
            logger.info(f"Successfully wrote {rows_written} rows to JSON file: {self.file_path}")
            
            return True
        
        except Exception as e:
            logger.error(f"Error loading data to JSON file: {str(e)}")
            raise
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the loading operation.
        
        Returns:
            Dictionary with metadata information
        """
        metadata = super().get_metadata()
        metadata.update({
            "destination": self.file_path,
            "format": "JSON",
            "orient": self.orient,
            "encoding": self.encoding,
            "mode": self.mode,
            "timestamp": datetime.now().isoformat()
        })
        
        # Add file stats if file exists
        if os.path.exists(self.file_path):
            file_stat = os.stat(self.file_path)
            metadata.update({
                "size_bytes": file_stat.st_size,
                "last_modified": datetime.fromtimestamp(file_stat.st_mtime).isoformat()
            })
            
        return metadata
