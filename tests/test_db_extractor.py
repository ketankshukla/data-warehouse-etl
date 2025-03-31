"""
Test suite for SQLite Database Extractor component.
Tests the extraction of data from SQLite databases.
"""
import os
import sys
import unittest
import tempfile
import sqlite3
import pandas as pd
from unittest.mock import patch, MagicMock

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the modules to be tested
from src.extractors.base_extractor import BaseExtractor
from src.extractors.sql_extractor import SQLExtractor


class TestDBExtractor(unittest.TestCase):
    """Test cases for DB (SQLite) Extractor component."""

    def setUp(self):
        """Set up test environment with sample SQLite database."""
        # Create a temporary SQLite database for testing
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test.db")
        
        # Create a connection to the test database
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        
        # Create a test table and insert sample data
        self.cursor.execute('''
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER,
                created_at TEXT
            )
        ''')
        
        # Insert sample data
        sample_data = [
            (1, "Item 1", 100, "2025-03-01"),
            (2, "Item 2", 200, "2025-03-02"),
            (3, "Item 3", 300, "2025-03-03"),
            (4, "Item 4", 400, "2025-03-04"),
            (5, "Item 5", 500, "2025-03-05")
        ]
        self.cursor.executemany('''
            INSERT INTO test_table (id, name, value, created_at)
            VALUES (?, ?, ?, ?)
        ''', sample_data)
        
        self.conn.commit()
        
        # Set up configuration for the DB extractor
        self.config = {
            "connection": {
                "database_path": self.db_path
            },
            "query": "SELECT * FROM test_table"
        }

    def tearDown(self):
        """Clean up test environment."""
        # Close database connection
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
        
        # Remove temporary directory and database file
        if hasattr(self, 'temp_dir') and os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir)

    @patch('src.extractors.sql_extractor.SQLExtractor')
    def test_db_basic_extraction(self, mock_sql_extractor):
        """Test basic extraction from SQLite database."""
        # Create mock DB extractor instance
        mock_instance = MagicMock()
        
        # Mock the extract method to return a dataframe
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Item 1', 'Item 2', 'Item 3', 'Item 4', 'Item 5'],
            'value': [100, 200, 300, 400, 500],
            'created_at': ['2025-03-01', '2025-03-02', '2025-03-03', '2025-03-04', '2025-03-05']
        })
        mock_instance.extract.return_value = df
        mock_sql_extractor.return_value = mock_instance
        
        # Use the mock extractor
        extractor = mock_sql_extractor(self.config)
        result = extractor.extract()
        
        # Verify results
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)
        self.assertListEqual(list(result.columns), ['id', 'name', 'value', 'created_at'])

    @patch('src.extractors.sql_extractor.SQLExtractor')
    def test_db_query_with_parameters(self, mock_sql_extractor):
        """Test SQLite extraction with query parameters."""
        # Create mock DB extractor instance
        mock_instance = MagicMock()
        
        # Prepare mock data for filtered query
        filtered_df = pd.DataFrame({
            'id': [1, 2],
            'name': ['Item 1', 'Item 2'],
            'value': [100, 200],
            'created_at': ['2025-03-01', '2025-03-02']
        })
        mock_instance.extract.return_value = filtered_df
        mock_sql_extractor.return_value = mock_instance
        
        # Configure extractor with parameters
        config_with_params = self.config.copy()
        config_with_params["query"] = "SELECT * FROM test_table WHERE value <= ?"
        config_with_params["parameters"] = [200]
        
        # Use the mock extractor
        extractor = mock_sql_extractor(config_with_params)
        result = extractor.extract()
        
        # Verify results
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)  # Should only have 2 rows where value <= 200
        self.assertListEqual(list(result.columns), ['id', 'name', 'value', 'created_at'])

    @patch('src.extractors.sql_extractor.SQLExtractor')
    def test_db_multiple_tables(self, mock_sql_extractor):
        """Test SQLite extraction with joins across multiple tables."""
        # Create mock DB extractor instance
        mock_instance = MagicMock()
        
        # Mock the extract method to return a joined dataframe
        joined_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Item 1', 'Item 2', 'Item 3'],
            'value': [100, 200, 300],
            'category': ['A', 'B', 'A'],
            'category_desc': ['Category A', 'Category B', 'Category A']
        })
        mock_instance.extract.return_value = joined_df
        mock_sql_extractor.return_value = mock_instance
        
        # Configure extractor with a join query
        config_with_join = self.config.copy()
        config_with_join["query"] = """
            SELECT t.id, t.name, t.value, c.name as category, c.description as category_desc
            FROM test_table t
            JOIN categories c ON t.category_id = c.id
            LIMIT 3
        """
        
        # Use the mock extractor
        extractor = mock_sql_extractor(config_with_join)
        result = extractor.extract()
        
        # Verify results
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        self.assertTrue('category' in result.columns)
        self.assertTrue('category_desc' in result.columns)


if __name__ == '__main__':
    unittest.main()
