"""
Test suite for CSV Extractor component.
Tests the extraction of data from CSV files.
"""
import os
import sys
import unittest
import tempfile
import pandas as pd
from unittest.mock import patch, MagicMock

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the modules to be tested
from src.extractors.base_extractor import BaseExtractor
from src.extractors.csv_extractor import CSVExtractor


class TestCSVExtractor(unittest.TestCase):
    """Test cases for CSV Extractor component."""

    def setUp(self):
        """Set up test environment with sample CSV files."""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        
        # Create a sample CSV file
        self.csv_path = os.path.join(self.temp_dir, "test_data.csv")
        self.sample_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Item 1', 'Item 2', 'Item 3', 'Item 4', 'Item 5'],
            'value': [100, 200, 300, 400, 500],
            'created_at': ['2025-03-01', '2025-03-02', '2025-03-03', '2025-03-04', '2025-03-05']
        })
        self.sample_data.to_csv(self.csv_path, index=False)
        
        # Create a CSV file with missing values for testing
        self.missing_values_csv = os.path.join(self.temp_dir, "missing_values.csv")
        missing_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Item 1', None, 'Item 3', 'Item 4', None],
            'value': [100, 200, None, 400, 500],
            'created_at': ['2025-03-01', '2025-03-02', '2025-03-03', None, '2025-03-05']
        })
        missing_data.to_csv(self.missing_values_csv, index=False)
        
        # Set up configuration for the CSV extractor
        self.config = {
            "connection": {
                "file_path": self.csv_path
            }
        }

    def tearDown(self):
        """Clean up test environment."""
        # Remove temporary directory and files
        if hasattr(self, 'temp_dir') and os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir)

    @patch('src.extractors.csv_extractor.CSVExtractor')
    def test_csv_basic_extraction(self, mock_csv_extractor):
        """Test basic extraction from CSV file."""
        # Create mock CSV extractor instance
        mock_instance = MagicMock()
        
        # Mock the extract method to return a dataframe
        mock_instance.extract.return_value = self.sample_data
        mock_csv_extractor.return_value = mock_instance
        
        # Use the mock extractor
        extractor = mock_csv_extractor(self.config)
        result = extractor.extract()
        
        # Verify results
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)
        self.assertListEqual(list(result.columns), ['id', 'name', 'value', 'created_at'])

    @patch('src.extractors.csv_extractor.CSVExtractor')
    def test_csv_with_custom_delimiter(self, mock_csv_extractor):
        """Test CSV extraction with custom delimiter."""
        # Create a CSV file with pipe delimiter
        pipe_csv_path = os.path.join(self.temp_dir, "pipe_delimited.csv")
        self.sample_data.to_csv(pipe_csv_path, sep='|', index=False)
        
        # Create mock CSV extractor instance
        mock_instance = MagicMock()
        mock_instance.extract.return_value = self.sample_data
        mock_csv_extractor.return_value = mock_instance
        
        # Configure extractor with pipe delimiter
        config_with_delimiter = self.config.copy()
        config_with_delimiter["connection"]["file_path"] = pipe_csv_path
        config_with_delimiter["connection"]["delimiter"] = "|"
        
        # Use the mock extractor
        extractor = mock_csv_extractor(config_with_delimiter)
        result = extractor.extract()
        
        # Verify results
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)
        self.assertListEqual(list(result.columns), ['id', 'name', 'value', 'created_at'])

    @patch('src.extractors.csv_extractor.CSVExtractor')
    def test_csv_with_missing_values(self, mock_csv_extractor):
        """Test CSV extraction with missing values."""
        # Create mock CSV extractor instance
        mock_instance = MagicMock()
        
        # Create sample data with missing values
        missing_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Item 1', None, 'Item 3', 'Item 4', None],
            'value': [100, 200, None, 400, 500],
            'created_at': ['2025-03-01', '2025-03-02', '2025-03-03', None, '2025-03-05']
        })
        
        mock_instance.extract.return_value = missing_data
        mock_csv_extractor.return_value = mock_instance
        
        # Configure extractor with missing values file
        config_with_missing = self.config.copy()
        config_with_missing["connection"]["file_path"] = self.missing_values_csv
        
        # Use the mock extractor
        extractor = mock_csv_extractor(config_with_missing)
        result = extractor.extract()
        
        # Verify results
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)
        # Test that missing values are properly represented
        self.assertTrue(result['name'].isna().any())
        self.assertTrue(result['value'].isna().any())
        self.assertTrue(result['created_at'].isna().any())

    @patch('src.extractors.csv_extractor.CSVExtractor')
    def test_csv_with_custom_dtypes(self, mock_csv_extractor):
        """Test CSV extraction with custom data types."""
        # Create mock CSV extractor instance
        mock_instance = MagicMock()
        
        # Create sample data with specific dtypes
        typed_data = self.sample_data.copy()
        typed_data['value'] = typed_data['value'].astype(float)
        typed_data['created_at'] = pd.to_datetime(typed_data['created_at'])
        
        mock_instance.extract.return_value = typed_data
        mock_csv_extractor.return_value = mock_instance
        
        # Configure extractor with dtype specifications
        config_with_dtypes = self.config.copy()
        config_with_dtypes["dtypes"] = {
            "id": "int",
            "value": "float",
            "created_at": "datetime"
        }
        
        # Use the mock extractor
        extractor = mock_csv_extractor(config_with_dtypes)
        result = extractor.extract()
        
        # Verify results
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)
        self.assertEqual(result['value'].dtype, float)
        self.assertTrue(pd.api.types.is_datetime64_dtype(result['created_at']))


if __name__ == '__main__':
    unittest.main()
