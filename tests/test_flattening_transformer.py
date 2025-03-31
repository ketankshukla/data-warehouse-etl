"""
Tests for the Flattening Transformer module.
"""
import unittest
import pandas as pd
import json
from src.transformers.flattening_transformer import FlatteningTransformer


class TestFlatteningTransformer(unittest.TestCase):
    """Test cases for the Flattening Transformer."""

    def setUp(self):
        """Set up test fixtures."""
        # Sample nested data for testing
        self.nested_data = pd.DataFrame([
            {
                "id": 1,
                "name": "Product 1",
                "details": json.dumps({
                    "color": "red",
                    "size": "large",
                    "dimensions": {
                        "width": 10,
                        "height": 20,
                        "depth": 5
                    }
                }),
                "tags": json.dumps(["electronics", "sale", "featured"])
            },
            {
                "id": 2,
                "name": "Product 2",
                "details": json.dumps({
                    "color": "blue",
                    "size": "medium",
                    "dimensions": {
                        "width": 8,
                        "height": 15,
                        "depth": 4
                    }
                }),
                "tags": json.dumps(["clothing", "new"])
            },
            {
                "id": 3,
                "name": "Product 3",
                "details": None,  # Test handling of None values
                "tags": json.dumps([])  # Test handling of empty arrays
            }
        ])
        
        # Non-JSON string to test error handling
        self.bad_data = pd.DataFrame([
            {
                "id": 4,
                "name": "Bad Product",
                "details": "Not a JSON string",
                "tags": "Also not JSON"
            }
        ])
        
        # Data with nested objects that are already parsed (not JSON strings)
        self.parsed_nested_data = pd.DataFrame([
            {
                "id": 5,
                "name": "Parsed Product",
                "details": {
                    "color": "green",
                    "features": ["waterproof", "durable"],
                    "specs": {
                        "weight": 2.5,
                        "material": "plastic"
                    }
                }
            }
        ])

    def test_basic_flattening(self):
        """Test basic flattening of nested structures."""
        # Configure the transformer to flatten the 'details' field
        config = {
            "flatten_fields": [
                {
                    "path": "details",
                    "drop_original": True
                }
            ]
        }
        
        transformer = FlatteningTransformer(config)
        result = transformer.transform(self.nested_data)
        
        # Assertions
        self.assertNotIn("details", result.columns)  # Original field should be dropped
        self.assertIn("details_color", result.columns)  # Flattened fields should be present
        self.assertIn("details_dimensions_width", result.columns)  # Nested fields should be flattened
        
        # Check specific values
        self.assertEqual(result.iloc[0]["details_color"], "red")
        self.assertEqual(result.iloc[0]["details_dimensions_width"], 10)
        
        # Check handling of None values
        self.assertTrue(pd.isna(result.iloc[2]["details_color"]))

    def test_flattening_with_prefix(self):
        """Test flattening with custom prefix."""
        # Configure the transformer with custom prefix
        config = {
            "flatten_fields": [
                {
                    "path": "details",
                    "prefix": "product_detail",
                    "drop_original": True
                }
            ]
        }
        
        transformer = FlatteningTransformer(config)
        result = transformer.transform(self.nested_data)
        
        # Assertions
        self.assertIn("product_detail_color", result.columns)
        self.assertEqual(result.iloc[1]["product_detail_color"], "blue")

    def test_flattening_with_custom_separator(self):
        """Test flattening with custom separator."""
        # Configure the transformer with custom separator
        config = {
            "flatten_fields": [
                {
                    "path": "details",
                    "separator": ".",
                    "drop_original": True
                }
            ]
        }
        
        transformer = FlatteningTransformer(config)
        result = transformer.transform(self.nested_data)
        
        # Assertions
        self.assertIn("details.color", result.columns)
        self.assertIn("details.dimensions.width", result.columns)

    def test_flattening_array_expand(self):
        """Test flattening arrays with expand strategy."""
        # Configure the transformer to flatten the 'tags' field with expand strategy
        config = {
            "flatten_fields": [
                {
                    "path": "tags",
                    "handle_lists": "expand",
                    "drop_original": True
                }
            ]
        }
        
        transformer = FlatteningTransformer(config)
        result = transformer.transform(self.nested_data)
        
        # Assertions
        self.assertNotIn("tags", result.columns)
        self.assertIn("tags_0", result.columns)  # First tag
        self.assertIn("tags_1", result.columns)  # Second tag
        
        # Check specific values
        self.assertEqual(result.iloc[0]["tags_0"], "electronics")
        self.assertEqual(result.iloc[1]["tags_0"], "clothing")
        
        # Check handling of empty arrays
        self.assertTrue(pd.isna(result.iloc[2]["tags_0"]))

    def test_flattening_array_first(self):
        """Test flattening arrays with first item strategy."""
        # Configure the transformer to flatten the 'tags' field with first strategy
        config = {
            "flatten_fields": [
                {
                    "path": "tags",
                    "handle_lists": "first",
                    "drop_original": False
                }
            ]
        }
        
        transformer = FlatteningTransformer(config)
        result = transformer.transform(self.nested_data)
        
        # Assertions
        self.assertIn("tags", result.columns)  # Field should be kept and updated
        
        # Check specific values
        self.assertEqual(result.iloc[0]["tags"], "electronics")
        self.assertEqual(result.iloc[1]["tags"], "clothing")

    def test_flattening_array_join(self):
        """Test flattening arrays with join strategy."""
        # Configure the transformer to flatten the 'tags' field with join strategy
        config = {
            "flatten_fields": [
                {
                    "path": "tags",
                    "handle_lists": "join",
                    "drop_original": False
                }
            ]
        }
        
        transformer = FlatteningTransformer(config)
        result = transformer.transform(self.nested_data)
        
        # Assertions
        self.assertIn("tags", result.columns)
        
        # Check specific values (tags should be joined with commas)
        self.assertEqual(result.iloc[0]["tags"], "electronics, sale, featured")
        self.assertEqual(result.iloc[1]["tags"], "clothing, new")

    def test_keep_original(self):
        """Test keeping original fields during flattening."""
        # Configure the transformer to keep original fields
        config = {
            "flatten_fields": [
                {
                    "path": "details",
                    "drop_original": False
                }
            ]
        }
        
        transformer = FlatteningTransformer(config)
        result = transformer.transform(self.nested_data)
        
        # Assertions
        self.assertIn("details", result.columns)  # Original should be kept
        self.assertIn("details_color", result.columns)  # Flattened fields should be added

    def test_max_depth(self):
        """Test max depth limitation during flattening."""
        # Create simplified test data with nested structure
        simple_nested_data = pd.DataFrame([
            {
                "id": 1,
                "nested": json.dumps({
                    "level1": "value1",
                    "level2": {
                        "sublevel1": "subvalue1",
                        "sublevel2": "subvalue2"
                    }
                })
            }
        ])
        
        # Configure the transformer with a max depth of 1
        config = {
            "flatten_fields": [
                {
                    "path": "nested",
                    "drop_original": True
                }
            ],
            "max_depth": 1  # Only flatten one level deep
        }
        
        transformer = FlatteningTransformer(config)
        result = transformer.transform(simple_nested_data)
        
        # First level should be flattened
        self.assertIn("nested_level1", result.columns)
        
        # Second level should be present as a single field, not flattened
        self.assertIn("nested_level2", result.columns) 
        self.assertNotIn("nested_level2_sublevel1", result.columns)

    def test_error_handling(self):
        """Test error handling with invalid JSON data."""
        # Configure the transformer
        config = {
            "flatten_fields": [
                {
                    "path": "details",
                    "drop_original": True
                }
            ]
        }
        
        transformer = FlatteningTransformer(config)
        result = transformer.transform(self.bad_data)
        
        # Assertions - should not crash and handle gracefully
        self.assertEqual(len(result), 1)  # Should still have one row
        self.assertTrue(all(col not in result.columns for col in [
            "details_color", "details_size"]))  # No flattened columns should be created

    def test_already_parsed_objects(self):
        """Test flattening with already parsed (non-string) nested objects."""
        # Configure the transformer
        config = {
            "flatten_fields": [
                {
                    "path": "details",
                    "drop_original": True
                }
            ]
        }
        
        transformer = FlatteningTransformer(config)
        result = transformer.transform(self.parsed_nested_data)
        
        # Assertions
        self.assertNotIn("details", result.columns)
        self.assertIn("details_color", result.columns)
        self.assertIn("details_specs_weight", result.columns)
        
        # Check specific values
        self.assertEqual(result.iloc[0]["details_color"], "green")
        self.assertEqual(result.iloc[0]["details_specs_weight"], 2.5)

    def test_multiple_fields(self):
        """Test flattening multiple fields simultaneously."""
        # Configure the transformer to flatten both 'details' and 'tags'
        config = {
            "flatten_fields": [
                {
                    "path": "details",
                    "drop_original": True
                },
                {
                    "path": "tags",
                    "handle_lists": "expand",
                    "drop_original": True
                }
            ]
        }
        
        transformer = FlatteningTransformer(config)
        result = transformer.transform(self.nested_data)
        
        # Assertions
        self.assertNotIn("details", result.columns)
        self.assertNotIn("tags", result.columns)
        self.assertIn("details_color", result.columns)
        self.assertIn("tags_0", result.columns)


if __name__ == '__main__':
    unittest.main()
