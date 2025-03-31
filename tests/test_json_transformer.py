"""
Tests for the JSON Transformer module.
"""
import unittest
import pandas as pd
import numpy as np
from src.transformers.json_transformer import JSONTransformer


class TestJSONTransformer(unittest.TestCase):
    """Test cases for the JSON Transformer."""

    def setUp(self):
        """Set up test fixtures."""
        # Sample data for testing
        self.sample_data = pd.DataFrame([
            {
                "id": 1,
                "user_id": "123",
                "first_name": "John",
                "last_name": "Doe",
                "age": "34",
                "active": "true",
                "score": "85.5",
                "created_at": "2023-01-15T10:30:00",
                "metadata": '{"source": "api", "version": "1.0"}',
                "address": {
                    "street": "123 Main St",
                    "city": "New York",
                    "zip": "10001"
                }
            },
            {
                "id": 2,
                "user_id": "456",
                "first_name": "Jane",
                "last_name": "Smith",
                "age": "28",
                "active": "false",
                "score": "92.3",
                "created_at": "2023-02-20T14:45:00",
                "metadata": '{"source": "web", "version": "1.1"}',
                "address": {
                    "street": "456 Oak Ave",
                    "city": "Boston",
                    "zip": "02108"
                }
            },
            {
                "id": 3,
                "user_id": "789",
                "first_name": "Bob",
                "last_name": "Johnson",
                "age": None,  # Test handling of None values
                "active": None,
                "score": None,
                "created_at": "invalid-date",  # Test handling of invalid data
                "metadata": None,
                "address": None
            }
        ])

    def test_select_fields(self):
        """Test selecting specific fields."""
        # Configure the transformer to select specific fields
        config = {
            "select_fields": ["id", "first_name", "last_name", "age"]
        }
        
        transformer = JSONTransformer(config)
        result = transformer.transform(self.sample_data)
        
        # Assertions
        self.assertEqual(len(result.columns), 4)
        self.assertTrue(all(col in result.columns for col in ["id", "first_name", "last_name", "age"]))
        self.assertFalse("user_id" in result.columns)  # Should be excluded

    def test_rename_fields(self):
        """Test renaming fields."""
        # Configure the transformer to rename fields
        config = {
            "rename_fields": {
                "first_name": "given_name",
                "last_name": "family_name",
                "user_id": "external_id"
            }
        }
        
        transformer = JSONTransformer(config)
        result = transformer.transform(self.sample_data)
        
        # Assertions
        self.assertIn("given_name", result.columns)
        self.assertIn("family_name", result.columns)
        self.assertIn("external_id", result.columns)
        self.assertNotIn("first_name", result.columns)
        self.assertNotIn("last_name", result.columns)
        self.assertNotIn("user_id", result.columns)
        
        # Check that data was preserved
        self.assertEqual(result.iloc[0]["given_name"], "John")
        self.assertEqual(result.iloc[1]["family_name"], "Smith")

    def test_type_casting(self):
        """Test type casting functionality."""
        # Configure the transformer for type casting
        config = {
            "type_casting": {
                "id": "int",
                "age": "int",
                "active": "bool",
                "score": "float",
                "created_at": "datetime"
            }
        }
        
        transformer = JSONTransformer(config)
        result = transformer.transform(self.sample_data)
        
        # Assertions - check if the types were cast correctly
        # Note: pandas/numpy types are different from Python native types
        self.assertEqual(result.iloc[0]["age"], 34)
        self.assertEqual(result.iloc[1]["age"], 28)
        self.assertTrue(pd.isna(result.iloc[2]["age"]))  # None should remain as NA
        
        # Check boolean conversion
        self.assertTrue(result.iloc[0]["active"])  # "yes" should be converted to True
        self.assertFalse(result.iloc[1]["active"])  # "no" should be converted to False
        self.assertTrue(pd.isna(result.iloc[2]["active"]))  # None should remain as NA
        
        # Check float conversion
        self.assertEqual(result.iloc[0]["score"], 85.5)
        self.assertEqual(result.iloc[1]["score"], 92.3)
        
        # Check datetime conversion
        self.assertIsInstance(result.iloc[0]["created_at"], pd.Timestamp)
        
        # Check numpy types
        self.assertIsInstance(result.iloc[0]["id"], np.int64)
        self.assertIsInstance(result.iloc[0]["age"], np.int64)
        # Python bool or numpy bool are both acceptable
        self.assertTrue(isinstance(result.iloc[0]["active"], bool) or isinstance(result.iloc[0]["active"], np.bool_))
        self.assertIsInstance(result.iloc[0]["score"], np.float64)

    def test_expressions(self):
        """Test applying expressions to create calculated fields."""
        # Create test data
        data = pd.DataFrame([
            {"first_name": "John", "last_name": "Doe", "price": 10.5, "quantity": 2},
            {"first_name": "Jane", "last_name": "Smith", "price": 15.0, "quantity": 3}
        ])
        
        # Configure the transformer with expressions
        config = {
            "calculated_fields": {
                "full_name": "first_name + ' ' + last_name",
                "total": "price * quantity",
                "discounted_price": "price * 0.9"
            }
        }
        
        transformer = JSONTransformer(config)
        result = transformer.transform(data)
        
        # Assertions
        self.assertIn("full_name", result.columns)
        self.assertIn("total", result.columns)
        self.assertIn("discounted_price", result.columns)
        
        # Check expression results
        self.assertEqual(result.iloc[0]["full_name"], "John Doe")
        self.assertEqual(result.iloc[1]["full_name"], "Jane Smith")
        
        self.assertAlmostEqual(result.iloc[0]["total"], 21.0)  # 10.5 * 2
        self.assertAlmostEqual(result.iloc[1]["total"], 45.0)  # 15.0 * 3
        
        self.assertAlmostEqual(result.iloc[0]["discounted_price"], 9.45)  # 10.5 * 0.9
        self.assertAlmostEqual(result.iloc[1]["discounted_price"], 13.5)  # 15.0 * 0.9

    def test_drop_fields(self):
        """Test dropping specific fields."""
        # Configure the transformer to drop fields
        config = {
            "drop_fields": ["metadata", "address", "created_at"]
        }
        
        transformer = JSONTransformer(config)
        result = transformer.transform(self.sample_data)
        
        # Assertions
        self.assertNotIn("metadata", result.columns)
        self.assertNotIn("address", result.columns)
        self.assertNotIn("created_at", result.columns)
        
        # Other fields should remain
        self.assertIn("id", result.columns)
        self.assertIn("first_name", result.columns)

    def test_combined_features(self):
        """Test combining multiple transformer features."""
        # Create test data with mixed fields
        data = pd.DataFrame([
            {"id": "1", "given_name": "John", "family_name": "Doe", "active": "yes", "score": "85.5"},
            {"id": "2", "given_name": "Jane", "family_name": "Smith", "active": "no", "score": "92.3"}
        ])
        
        # Configure transformer with multiple features
        config = {
            "select_fields": ["id", "given_name", "family_name", "active", "score"],
            "rename_fields": {
                "given_name": "first_name", 
                "family_name": "last_name"
            },
            "type_casting": {
                "id": "int",
                "active": "bool",
                "score": "float"
            },
            "calculated_fields": {
                "full_name": "first_name + ' ' + last_name"
            },
            "drop_fields": ["first_name", "last_name"]  # Drop after using in expression
        }
        
        transformer = JSONTransformer(config)
        result = transformer.transform(data)
        
        # Assertions
        self.assertEqual(list(result.columns), ["id", "active", "score", "full_name"])  # Check exact columns
        self.assertNotIn("first_name", result.columns)  # Should be dropped
        self.assertNotIn("last_name", result.columns)  # Should be dropped
        
        self.assertEqual(result.iloc[0]["full_name"], "John Doe")
        self.assertEqual(result.iloc[1]["full_name"], "Jane Smith")
        
        # Check type conversions
        self.assertEqual(result.iloc[0]["active"], True)
        self.assertEqual(result.iloc[1]["active"], False)

    def test_validation(self):
        """Test validation of transformer configuration."""
        # Test valid configuration
        valid_config = {
            "select_fields": ["id", "name"],
            "drop_fields": ["metadata"]
        }
        
        transformer = JSONTransformer(valid_config)
        self.assertTrue(transformer.validate())
        
        # Test invalid configuration (conflict between select and drop)
        invalid_config = {
            "select_fields": ["id", "name", "metadata"],
            "drop_fields": ["metadata"]
        }
        
        transformer = JSONTransformer(invalid_config)
        self.assertFalse(transformer.validate())
        
        # Test invalid rename configuration (duplicate target names)
        invalid_rename_config = {
            "rename_fields": {
                "first_name": "name",
                "last_name": "name"  # Duplicate target
            }
        }
        
        transformer = JSONTransformer(invalid_rename_config)
        self.assertFalse(transformer.validate())

    def test_empty_dataframe(self):
        """Test handling of empty DataFrames."""
        # Create an empty DataFrame with the same columns
        empty_df = pd.DataFrame(columns=self.sample_data.columns)
        
        config = {
            "select_fields": ["id", "first_name", "last_name"],
            "rename_fields": {"first_name": "given_name"}
        }
        
        transformer = JSONTransformer(config)
        result = transformer.transform(empty_df)
        
        # Assertions
        self.assertTrue(result.empty)
        self.assertEqual(len(result.columns), 3)
        self.assertIn("id", result.columns)
        self.assertIn("given_name", result.columns)
        self.assertIn("last_name", result.columns)


if __name__ == '__main__':
    unittest.main()
