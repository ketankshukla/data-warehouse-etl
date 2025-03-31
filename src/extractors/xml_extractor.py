"""
XML Extractor module for the Data Warehouse ETL Framework.
Provides functionality to extract data from XML files.
"""
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import xml.etree.ElementTree as ET

from .base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

class XMLExtractor(BaseExtractor):
    """
    Extractor for XML data sources.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the XML extractor with configuration.
        
        Args:
            config: Dictionary containing configuration parameters including:
                - file_path: Path to the XML file
                - root_element: The root element to start extraction from
                - record_tag: XML tag that represents each record
                - namespaces: XML namespaces mapping (optional)
                - encoding: File encoding (default: 'utf-8')
        """
        super().__init__(config)
        self.file_path = config.get("file_path")
        self.root_element = config.get("root_element")
        self.record_tag = config.get("record_tag")
        self.namespaces = config.get("namespaces", {})
        self.encoding = config.get("encoding", "utf-8")
    
    def validate_source(self) -> bool:
        """
        Validate that the XML file exists and is accessible.
        
        Returns:
            True if the file is valid, False otherwise
        """
        if not self.file_path:
            logger.error("File path not provided in configuration")
            return False
        
        if not os.path.exists(self.file_path):
            logger.error(f"File not found: {self.file_path}")
            return False
        
        if not os.path.isfile(self.file_path):
            logger.error(f"Path is not a file: {self.file_path}")
            return False
        
        # Check if file has read permission
        if not os.access(self.file_path, os.R_OK):
            logger.error(f"No read permission for file: {self.file_path}")
            return False
        
        # Required parameters for extraction
        if not self.record_tag:
            logger.error("Record tag not provided in configuration")
            return False
        
        return True
    
    def _element_to_dict(self, element: ET.Element) -> Dict[str, Any]:
        """
        Convert an XML element to a dictionary.
        
        Args:
            element: The XML element to convert
            
        Returns:
            Dictionary representation of the element
        """
        result = {}
        
        # Add element attributes
        result.update(element.attrib)
        
        # Add element text if not None and not just whitespace
        if element.text and element.text.strip():
            result["_text"] = element.text.strip()
        
        # Process child elements
        for child in element:
            tag = child.tag
            # Remove namespace prefix if present
            if "}" in tag:
                tag = tag.split("}", 1)[1]
                
            child_dict = self._element_to_dict(child)
            
            # Handle multiple elements with the same tag
            if tag in result:
                if isinstance(result[tag], list):
                    result[tag].append(child_dict)
                else:
                    result[tag] = [result[tag], child_dict]
            else:
                result[tag] = child_dict
                
        return result
    
    def extract(self) -> pd.DataFrame:
        """
        Extract data from the XML file.
        
        Returns:
            Pandas DataFrame containing the XML data
        
        Raises:
            FileNotFoundError: If the file does not exist
            Exception: For other extraction errors
        """
        if not self.validate_source():
            raise FileNotFoundError(f"Invalid or inaccessible XML file: {self.file_path}")
        
        try:
            logger.info(f"Reading XML file: {self.file_path}")
            tree = ET.parse(self.file_path)
            root = tree.getroot()
            
            # Navigate to the specified root element if provided
            if self.root_element:
                elements = root.findall(self.root_element, self.namespaces)
                if not elements:
                    logger.error(f"Root element '{self.root_element}' not found in XML")
                    raise ValueError(f"Root element '{self.root_element}' not found in XML")
                root = elements[0]
            
            # Find all record elements
            records = root.findall(f".//{self.record_tag}", self.namespaces)
            logger.info(f"Found {len(records)} records with tag '{self.record_tag}'")
            
            if not records:
                logger.warning(f"No records found with tag '{self.record_tag}'")
                return pd.DataFrame()
            
            # Convert XML records to list of dictionaries
            data = [self._element_to_dict(record) for record in records]
            
            # Convert to DataFrame
            df = pd.json_normalize(data)
            logger.info(f"Successfully extracted {len(df)} rows from XML file")
            
            return df
        
        except ET.ParseError as e:
            logger.error(f"Error parsing XML file: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error extracting data from XML file: {str(e)}")
            raise
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the extracted XML data.
        
        Returns:
            Dictionary with metadata information
        """
        metadata = super().get_metadata()
        metadata.update({
            "source": self.file_path,
            "timestamp": datetime.now().isoformat(),
            "format": "XML",
            "record_tag": self.record_tag,
            "root_element": self.root_element,
            "encoding": self.encoding
        })
        
        # Add file stats if file exists
        if self.validate_source():
            file_stat = os.stat(self.file_path)
            metadata.update({
                "size_bytes": file_stat.st_size,
                "last_modified": datetime.fromtimestamp(file_stat.st_mtime).isoformat()
            })
            
        return metadata
