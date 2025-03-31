# End-to-End ETL Process with JSON Files

This guide walks through the complete process of extracting data from a JSON file, transforming it, and loading it to a destination using the Data Warehouse ETL Framework.

## Table of Contents
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Configuration Setup](#configuration-setup)
4. [Extraction Phase](#extraction-phase)
5. [Transformation Phase](#transformation-phase)
6. [Loading Phase](#loading-phase)
7. [Execution and Monitoring](#execution-and-monitoring)
8. [Troubleshooting](#troubleshooting)
9. [Advanced Configuration](#advanced-configuration)

## Overview

JSON (JavaScript Object Notation) is a widely used data interchange format that supports nested structures and complex data types. This guide demonstrates how the ETL Framework processes JSON files from extraction to loading.

The example we'll follow is processing a product catalog JSON file, transforming the nested structure, and loading it to both a normalized SQL database and a flattened CSV file.

## Prerequisites

Before starting, ensure you have:

1. The Data Warehouse ETL Framework installed
2. Python 3.8 or higher
3. Required dependencies installed (`pip install -r requirements.txt`)
4. A JSON file to process
5. Target destination access configured

## Configuration Setup

### Sample JSON File Structure

Our example uses a JSON file with product data (`products.json`):

```json
[
  {
    "product_id": "P1001",
    "name": "Ergonomic Office Chair",
    "description": "Adjustable office chair with lumbar support",
    "price": 299.99,
    "category": "Furniture",
    "specifications": {
      "dimensions": {
        "width": 65,
        "depth": 67,
        "height": 115
      },
      "weight_kg": 15.2,
      "color": "Black",
      "material": "Mesh and Metal"
    },
    "inventory": {
      "in_stock": true,
      "quantity": 42,
      "warehouse_locations": ["EAST-5", "WEST-3"]
    },
    "reviews": [
      {
        "user_id": "U1001",
        "rating": 4.5,
        "comment": "Very comfortable for long work sessions"
      },
      {
        "user_id": "U1042",
        "rating": 5.0,
        "comment": "Best office chair I've ever had"
      }
    ]
  },
  {
    "product_id": "P1002",
    "name": "Standing Desk",
    "description": "Adjustable height standing desk",
    "price": 449.99,
    "category": "Furniture",
    "specifications": {
      "dimensions": {
        "width": 120,
        "depth": 80,
        "height": {
          "min": 72,
          "max": 120
        }
      },
      "weight_kg": 36.5,
      "color": "Walnut",
      "material": "Engineered Wood and Steel"
    },
    "inventory": {
      "in_stock": true,
      "quantity": 13,
      "warehouse_locations": ["CENTRAL-2"]
    },
    "reviews": [
      {
        "user_id": "U2053",
        "rating": 4.0,
        "comment": "Smooth height adjustment mechanism"
      }
    ]
  }
]
```

### YAML Configuration

Create a YAML configuration file (`json_etl_config.yaml`) that defines the ETL process:

```yaml
job:
  name: product_catalog_processing
  description: "Process product catalog from JSON to normalized database and CSV"

extractors:
  - name: product_json
    type: json
    config:
      file_path: data/input/products.json
      encoding: utf-8
      orient: records  # Specifies that the JSON is an array of objects
      
transformers:
  - name: structure_normalizer
    type: custom
    config:
      # Flatten nested structures with dot notation
      flatten_fields:
        - path: "specifications.dimensions"
          prefix: "dim_"
        - path: "inventory"
          prefix: "inv_"
      # Extract nested arrays to separate tables
      extract_arrays:
        - path: "reviews"
          join_key: "product_id"
          
  - name: data_cleaner
    type: cleaning
    config:
      # String transformations
      string_transforms:
        name: "title"
        description: "strip"
      # Type conversions
      type_conversions:
        price: "float"
        
loaders:
  - name: product_database
    type: sql
    config:
      connection_string: "sqlite:///data/output/product_catalog.db"
      table_name: "products"
      if_exists: "replace"
      index: false
      
  - name: reviews_database
    type: sql
    config:
      connection_string: "sqlite:///data/output/product_catalog.db"
      table_name: "reviews"
      if_exists: "replace"
      index: false
      source_dataframe: "reviews"  # Uses the extracted reviews dataframe
      
  - name: csv_export
    type: csv
    config:
      file_path: data/output/products_flattened.csv
      delimiter: ","
      include_header: true
      encoding: utf-8
```

## Extraction Phase

When the ETL job runs, here's exactly what happens during the extraction phase:

1. **Initialization**:
   - The ETLPipeline loads the configuration file and identifies the `json` extractor
   - A JSONExtractor instance is created with the specified configuration

2. **Configuration Validation**:
   - The extractor validates that the JSON file exists at the specified path
   - Configuration parameters are checked (encoding, orient, etc.)

3. **File Reading Process**:
   - The extractor opens the JSON file using the specified encoding
   - The JSON content is parsed into Python objects
   - The `orient` parameter determines how the JSON structure is interpreted:
     - `records`: Array of objects (most common)
     - `index`: Dictionary of dictionaries with outer keys as index
     - `columns`: Dictionary of arrays with keys as column names
     - `values`: Just the values array
     - `split`: Split data and index in separate origins

4. **Nested Structure Handling**:
   - The JSONExtractor natively preserves nested structures
   - Complex nested objects are maintained as dictionaries
   - Arrays within the JSON are maintained as lists
   - This allows for processing complex hierarchical data

5. **Result Preparation**:
   - The extracted data is loaded into a pandas DataFrame
   - For nested structures, pandas automatically creates complex data types
   - The extractor adds metadata (source name, record count, timestamp)
   - The DataFrame is returned to the pipeline for the transformation phase

### JSON-Specific Considerations

- **Schema Inference**: Unlike CSV, JSON doesn't have a fixed schema, so the extractor infers it from the data
- **Inconsistent Structures**: The extractor handles missing fields across records
- **Large File Handling**: For large JSON files, streaming parser can be used to avoid memory issues
- **JSON Lines Format**: Support for JSONL (one JSON object per line) with `lines=True` setting

## Transformation Phase

Once the data is extracted, the transformation phase proceeds:

1. **Structure Normalizer Transformer**:
   - Receives the DataFrame with nested JSON structures
   - Flattens specified nested objects with dot notation:
     - `specifications.dimensions.width` becomes `dim_width`
     - `specifications.dimensions.depth` becomes `dim_depth`
     - `inventory.in_stock` becomes `inv_in_stock`
   - For arrays like `reviews`, creates separate DataFrames:
     - Extracts each review as a separate row
     - Adds the parent `product_id` to each row for joining
     - Stores these as additional DataFrames in the pipeline context

2. **Data Cleaner Transformer**:
   - Receives the flattened product DataFrame
   - Processes the data according to configured cleaning operations:
     - Converts product names to title case
     - Strips whitespace from descriptions
     - Ensures price values are floating-point numbers

3. **Transformation Pipeline Flow**:
   - Each transformer operates on the output of the previous transformer
   - The pipeline now maintains multiple DataFrames (products and reviews)
   - Transformers can operate on specific DataFrames by name

## Loading Phase

After transformation, the loading phase begins:

1. **SQL Loader for Products**:
   - The first SQLLoader loads the products DataFrame into the database
   - Connection is established to the SQLite database
   - The products table is created (or replaced) based on the DataFrame schema
   - Data is loaded in batches to optimize performance
   - Complex JSON structures that weren't flattened are automatically serialized

2. **SQL Loader for Reviews**:
   - The second SQLLoader targets the extracted reviews DataFrame
   - A separate reviews table is created in the same database
   - The product_id field maintains the relationship to the parent products

3. **CSV Loader**:
   - The CSVLoader writes the flattened products data to a CSV file
   - Headers are included based on the configuration
   - The data is written with the specified delimiter and encoding

4. **Loading Coordination**:
   - All loading operations happen in sequence
   - Each loader reports its progress and results
   - The pipeline tracks the success/failure of each loading operation

## Execution and Monitoring

During execution, the ETL process provides real-time monitoring:

1. **Command Line Execution**:
   ```powershell
   python main.py --config config/json_etl_config.yaml --log-level INFO
   ```

2. **Logging Output**:
   The framework logs each step of the process:
   ```
   2025-03-24 09:15:22 [INFO] [pipeline:92] ETL job 'product_catalog_processing' started
   2025-03-24 09:15:22 [INFO] [pipeline:103] Initialization phase complete
   2025-03-24 09:15:22 [INFO] [json_extractor:51] Reading JSON file from data/input/products.json
   2025-03-24 09:15:22 [INFO] [json_extractor:78] Read 2 records from JSON file
   2025-03-24 09:15:22 [INFO] [pipeline:142] Extraction phase complete, extracted 2 records
   2025-03-24 09:15:22 [INFO] [structure_normalizer:61] Flattening nested JSON structures
   2025-03-24 09:15:22 [INFO] [structure_normalizer:83] Extracted 3 reviews from 2 products
   2025-03-24 09:15:22 [INFO] [data_cleaner:92] Applying cleaning transformations...
   2025-03-24 09:15:22 [INFO] [pipeline:178] Transformation phase complete
   2025-03-24 09:15:22 [INFO] [sql_loader:114] Connecting to database
   2025-03-24 09:15:22 [INFO] [sql_loader:143] Creating table 'products'
   2025-03-24 09:15:22 [INFO] [sql_loader:165] Inserted 2 records into table 'products'
   2025-03-24 09:15:22 [INFO] [sql_loader:114] Connecting to database
   2025-03-24 09:15:22 [INFO] [sql_loader:143] Creating table 'reviews'
   2025-03-24 09:15:22 [INFO] [sql_loader:165] Inserted 3 records into table 'reviews'
   2025-03-24 09:15:23 [INFO] [csv_loader:87] Writing data to CSV file data/output/products_flattened.csv
   2025-03-24 09:15:23 [INFO] [csv_loader:102] Wrote 2 records to CSV file
   2025-03-24 09:15:23 [INFO] [pipeline:214] Loading phase complete
   2025-03-24 09:15:23 [INFO] [pipeline:228] ETL job completed successfully in 0.56 seconds
   ```

3. **Performance Monitoring**:
   - The framework collects metrics at each stage
   - Total records processed at each stage
   - Execution time for each phase
   - Memory usage statistics

4. **Results**:
   - The processed data is now available in:
     - The SQLite database with separate tables for products and reviews
     - A flattened CSV file with the product data
   - A job summary report is generated

## Troubleshooting

Common issues when processing JSON files and their solutions:

1. **Invalid JSON Format**:
   - Verify the JSON file has valid syntax
   - Use a JSON validator to check for formatting errors
   - Check for encoding issues that might corrupt the JSON structure

2. **Inconsistent Structure**:
   - If some records are missing fields, the framework will insert NULLs
   - For required fields, use the validation transformer to filter incomplete records

3. **Deeply Nested Structures**:
   - For very deep nesting, use multiple flattening operations
   - Consider custom transformers for complex restructuring needs

4. **Array Handling Issues**:
   - If arrays have inconsistent item structures, use schema validation
   - For very large arrays, consider processing them in chunks

5. **Memory Issues with Large Files**:
   - Use the streaming option: `streaming: true`
   - This processes the JSON file in chunks to reduce memory usage

## Advanced Configuration

For more complex JSON processing needs:

1. **Custom JSON Path Selectors**:
   ```yaml
   extractors:
     - name: json_extractor
       type: json
       config:
         file_path: "data/input/products.json"
         json_path: "$.products[*]"
   ```

2. **Multi-Level Flattening**:
   ```yaml
   transformers:
     - name: flattener
       type: json_flattener
       config:
         flatten_nested:
           - path: "specifications.dimensions"
             prefix: "dim_"
           - path: "variants"
             prefix: "variant_"
             max_items: 3
   ```

3. **JSON Schema Validation**:
   ```yaml
   transformers:
     - name: schema_validator
       type: json_validator
       config:
         schema_path: "schemas/product_schema.json"
         on_error: "filter"  # Options: filter, fail, continue
   ```

4. **Advanced JSON Path Selection**:
   ```yaml
   transformers:
     - name: json_selector
       type: json_path
       config:
         selections:
           - path: "$.name"
             alias: "product_name"
           - path: "$.specifications.dimensions[*]"
             alias: "dimensions"
           - path: "$.variants[?(@.stock > 0)]"
             alias: "in_stock_variants"
   ```

This end-to-end guide covers the complete process for handling JSON files in the Data Warehouse ETL Framework, from initial configuration to final data loading and reporting.

## Technical Implementation Details

### Module Structure and Program Flow

The ETL Framework processes JSON files through several key modules and classes:

```
src/
├── extractors/
│   ├── base_extractor.py         # BaseExtractor abstract class
│   ├── json_extractor.py         # JSONExtractor implementation
│   └── extractor_factory.py      # Factory for creating extractors
├── transformers/
│   ├── base_transformer.py       # BaseTransformer abstract class
│   ├── json_flattener.py         # JSON flattening transformer
│   ├── json_validator.py         # JSON schema validation
│   └── transformer_factory.py    # Factory for creating transformers
├── loaders/
│   ├── base_loader.py            # BaseLoader abstract class
│   ├── json_loader.py            # JSONLoader implementation
│   ├── sql_loader.py             # SQLLoader implementation
│   └── loader_factory.py         # Factory for creating loaders
├── utils/
│   ├── config_manager.py         # Configuration handling
│   ├── json_utils.py             # JSON parsing utilities
│   └── logging_utils.py          # Logging utilities
├── cli.py                        # Command-line interface
└── pipeline.py                   # ETLPipeline orchestrator
```

### Process Flow Diagram

```
┌─────────────────┐          ┌──────────────┐          ┌──────────────┐          ┌──────────────┐          ┌───────────┐
│  main.py  │          │  ETLPipeline │          │JSONExtractor │          │JSONTransform │          │  Loaders  │
└─────┬─────┘          └──────┬───────┘          └──────┬───────┘          └──────┬───────┘          └─────┬─────┘
      │                       │                         │                         │                         │
      │ parse_args()          │                         │                         │                         │
      │───────────────────────>                         │                         │                         │
      │                       │                         │                         │                         │
      │                       │ load_config()           │                         │                         │
      │                       │◄──────────────          │                         │                         │
      │                       │                         │                         │                         │
      │                       │ _initialize_components()│                         │                         │
      │                       │◄──────────────          │                         │                         │
      │                       │                         │                         │                         │
      │                       │                         │                         │                         │
      │                       │ validate_source()       │                         │                         │
      │                       │────────────────────────>│                         │                         │
      │                       │                         │                         │                         │
      │                       │ _run_extraction()       │                         │                         │
      │                       │────────────────────────>│                         │                         │
      │                       │                         │                         │                         │
      │                       │                         │ extract()               │                         │
      │                       │                         │◄──────────              │                         │
      │                       │                         │                         │                         │
      │                       │                         │ json.load()             │                         │
      │                       │                         │◄──────────              │                         │
      │                       │                         │                         │                         │
      │                       │                         │ apply JSON path         │                         │
      │                       │                         │◄──────────              │                         │
      │                       │                         │                         │                         │
      │                       │                         │ pd.json_normalize()     │                         │
      │                       │                         │◄──────────              │                         │
      │                       │                         │                         │                         │
      │                       │ DataFrame               │                         │                         │
      │                       │<────────────────────────│                         │                         │
      │                       │                         │                         │                         │
      │                       │ _run_transformation()   │                         │                         │
      │                       │────────────────────────────────────────────────>  │                         │
      │                       │                         │                         │                         │
      │                       │                         │                         │ flatten_nested_dict()   │
      │                       │                         │                         │◄──────────              │
      │                       │                         │                         │                         │
      │                       │                         │                         │ validate_against_schema()│
      │                       │                         │                         │◄──────────              │
      │                       │                         │                         │                         │
      │                       │ Transformed DataFrame   │                         │                         │
      │                       │<────────────────────────────────────────────────  │                         │
      │                       │                         │                         │                         │
      │                       │ _run_loading()          │                         │                         │
      │                       │──────────────────────────────────────────────────────────────────────────>  │
      │                       │                         │                         │                         │
      │                       │                         │                         │                         │ load()
      │                       │                         │                         │                         │◄──────
      │                       │                         │                         │                         │
      │                       │ Loading Result          │                         │                         │
      │                       │<──────────────────────────────────────────────────────────────────────────  │
      │                       │                         │                         │                         │
      │ run() result          │                         │                         │                         │
      │<──────────────────────│                         │                         │                         │
      │                       │                         │                         │                         │
┌─────┴─────┐          ┌──────┴───────┐          ┌──────┴───────┐          ┌──────┴───────┐          ┌─────┴─────┐
│  main.py  │          │  ETLPipeline │          │JSONExtractor │          │JSONTransform │          │  Loaders  │
└───────────┘          └──────────────┘          └──────────────┘          └──────────────┘          └───────────┘
```

### Key Classes and Methods

#### 1. JSONExtractor Class (extractors/json_extractor.py)

Responsible for extracting data from JSON files:

```python
class JSONExtractor(BaseExtractor):
    def __init__(self, config):
        """Initialize with extraction configuration."""
        super().__init__(config)
        self.file_path = config.get('file_path')
        self.encoding = config.get('encoding', 'utf-8')
        self.json_path = config.get('json_path', None)
        self.root_node = config.get('root_node', None)
        self.normalize = config.get('normalize', True)
        
    def validate_source(self):
        """Validate the JSON source exists and is accessible."""
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"JSON file not found: {self.file_path}")
        
        # Try to parse a small portion to validate JSON syntax
        try:
            with open(self.file_path, 'r', encoding=self.encoding) as f:
                sample = f.read(1024)  # Read just enough to validate syntax
                json.loads(sample + (']' if sample.lstrip().startswith('[') else '}'))
            return True
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON syntax: {str(e)}")
            raise
        
    def extract(self):
        """Extract data from the JSON file into a pandas DataFrame."""
        self.logger.info(f"Reading JSON file from {self.file_path}")
        
        try:
            with open(self.file_path, 'r', encoding=self.encoding) as f:
                json_data = json.load(f)
            
            # Apply JSON path selector if specified
            if self.json_path:
                import jsonpath_ng.ext as jsonpath
                expr = jsonpath.parse(self.json_path)
                matches = [match.value for match in expr.find(json_data)]
                if matches:
                    json_data = matches
                else:
                    self.logger.warning(f"JSON path '{self.json_path}' returned no results")
                    json_data = []
                    
            # Select a specific root node if specified
            if self.root_node and isinstance(json_data, dict):
                json_data = json_data.get(self.root_node, {})
                
            # Convert to DataFrame
            if isinstance(json_data, list):
                # List of objects - direct normalization if they're flat
                if all(isinstance(item, dict) and not any(isinstance(v, (dict, list)) for v in item.values()) for item in json_data):
                    df = pd.DataFrame(json_data)
                else:
                    # Complex nested structure - normalization required
                    if self.normalize:
                        df = pd.json_normalize(json_data)
                    else:
                        # Store as JSON strings in a single column
                        df = pd.DataFrame({'json_data': [json.dumps(item) for item in json_data]})
            else:
                # Single object
                if self.normalize:
                    df = pd.json_normalize(json_data)
                else:
                    df = pd.DataFrame({'json_data': [json.dumps(json_data)]})
                
            self.logger.info(f"Extracted {len(df)} records from JSON file")
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting data from JSON: {str(e)}")
            raise
```

#### 2. JSONFlattener Class (transformers/json_flattener.py)

Specialized in flattening nested JSON structures:

```python
class JSONFlattener(BaseTransformer):
    def __init__(self, config):
        """Initialize with transformation configuration."""
        super().__init__(config)
        self.flatten_nested = config.get('flatten_nested', [])
        self.explode_arrays = config.get('explode_arrays', [])
        self.max_level = config.get('max_level', None)
        
    def _flatten_nested_dict(self, df, path, prefix, max_level=None, current_level=0):
        """Recursively flatten nested dictionaries in a column."""
        if max_level is not None and current_level >= max_level:
            return df
            
        # Get the original column and check if it exists
        if '.' in path:
            parts = path.split('.')
            current_path = parts[0]
            remaining_path = '.'.join(parts[1:])
            
            if current_path not in df.columns:
                self.logger.warning(f"Column '{current_path}' not found in DataFrame")
                return df
                
            # Process nested paths recursively
            for i, col in enumerate(df[current_path]):
                if isinstance(col, dict):
                    for key, value in col.items():
                        new_col_name = f"{current_path}.{key}"
                        if new_col_name not in df.columns:
                            df[new_col_name] = None
                        df.at[i, new_col_name] = value
                        
            # Continue with remaining path
            return self._flatten_nested_dict(df, remaining_path, prefix, max_level, current_level + 1)
        else:
            # Process the final column
            if path not in df.columns:
                self.logger.warning(f"Column '{path}' not found in DataFrame")
                return df
                
            # Extract dictionary items to new columns
            for i, col in enumerate(df[path]):
                if isinstance(col, dict):
                    for key, value in col.items():
                        new_col_name = f"{prefix}{key}"
                        if new_col_name not in df.columns:
                            df[new_col_name] = None
                        df.at[i, new_col_name] = value
                        
            return df
            
    def _explode_array(self, df, column, max_items=None):
        """Explode an array column into multiple rows."""
        if column not in df.columns:
            self.logger.warning(f"Column '{column}' not found in DataFrame")
            return df
            
        # Check if values are lists
        if not df[column].apply(lambda x: isinstance(x, list)).any():
            self.logger.warning(f"Column '{column}' does not contain lists to explode")
            return df
            
        # Limit array size if specified
        if max_items is not None:
            df[column] = df[column].apply(lambda x: x[:max_items] if isinstance(x, list) else x)
            
        # Explode the column
        return df.explode(column)
        
    def transform(self, df):
        """Apply JSON flattening transformations to the DataFrame."""
        self.logger.info("Flattening nested JSON structures...")
        
        result_df = df.copy()
        
        # Flatten nested dictionaries
        for config in self.flatten_nested:
            path = config.get('path')
            prefix = config.get('prefix', '')
            max_level = config.get('max_level', self.max_level)
            
            if path:
                result_df = self._flatten_nested_dict(result_df, path, prefix, max_level)
                
        # Explode array columns
        for config in self.explode_arrays:
            column = config.get('column')
            max_items = config.get('max_items', None)
            
            if column:
                result_df = self._explode_array(result_df, column, max_items)
                
        self.logger.info(f"Flattening complete, resulting in {len(result_df)} records")
        return result_df
```

#### 3. JSONValidator Class (transformers/json_validator.py)

Handles JSON schema validation:

```python
class JSONValidator(BaseTransformer):
    def __init__(self, config):
        """Initialize with validation configuration."""
        super().__init__(config)
        self.schema_path = config.get('schema_path')
        self.on_error = config.get('on_error', 'filter')  # 'filter', 'fail', or 'continue'
        self._load_schema()
        
    def _load_schema(self):
        """Load the JSON schema from file."""
        try:
            with open(self.schema_path, 'r') as f:
                self.schema = json.load(f)
                
            # Validate schema is a valid JSON Schema
            Draft7Validator.check_schema(self.schema)
            self.validator = Draft7Validator(self.schema)
            
        except FileNotFoundError:
            self.logger.error(f"Schema file not found: {self.schema_path}")
            raise
        except json.JSONDecodeError:
            self.logger.error(f"Invalid schema JSON: {self.schema_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error loading schema: {str(e)}")
            raise
            
    def _validate_record(self, record):
        """Validate a single record against the schema."""
        errors = list(self.validator.iter_errors(record))
        return len(errors) == 0, errors
        
    def transform(self, df):
        """Validate JSON data against the schema."""
        self.logger.info(f"Validating data against schema: {self.schema_path}")
        
        # Convert DataFrame back to dict records for validation
        records = df.to_dict(orient='records')
        valid_records = []
        invalid_count = 0
        
        for i, record in enumerate(records):
            is_valid, errors = self._validate_record(record)
            
            if is_valid:
                valid_records.append(record)
            else:
                invalid_count += 1
                error_details = '; '.join([e.message for e in errors])
                self.logger.warning(f"Record {i} failed validation: {error_details}")
                
                if self.on_error == 'fail':
                    raise ValueError(f"Validation failed for record {i}: {error_details}")
                elif self.on_error == 'filter':
                    continue
                elif self.on_error == 'continue':
                    valid_records.append(record)
                    
        self.logger.info(f"Validation complete. {len(valid_records)} valid records, {invalid_count} invalid records")
        
        if self.on_error == 'filter' or invalid_count > 0:
            return pd.DataFrame(valid_records)
        return df
```

#### 4. JSONLoader Class (loaders/json_loader.py)

Handles writing data to JSON files:

```python
class JSONLoader(BaseLoader):
    def __init__(self, config):
        """Initialize with loader configuration."""
        super().__init__(config)
        self.file_path = config.get('file_path')
        self.orient = config.get('orient', 'records')
        self.date_format = config.get('date_format', 'iso')
        self.compression = config.get('compression', None)
        self.indent = config.get('indent', 2)
        
    def validate_destination(self):
        """Validate the destination path is writable."""
        try:
            # Check if directory exists
            directory = os.path.dirname(self.file_path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory)
                
            # Check if file is writable
            with open(self.file_path, 'a') as f:
                pass
                
            return True
        except Exception as e:
            self.logger.error(f"JSON destination validation failed: {str(e)}")
            raise
            
    def load(self, df):
        """Load the DataFrame to a JSON file."""
        self.logger.info(f"Writing data to JSON file: {self.file_path}")
        
        try:
            # Handle NaN values and complex objects
            def json_serial(obj):
                """JSON serializer for objects not serializable by default json code"""
                if isinstance(obj, (datetime, date)):
                    return obj.isoformat()
                if pd.isna(obj):
                    return None
                raise TypeError(f"Type {type(obj)} not serializable")
                
            # Convert to JSON
            if self.orient == 'table':
                # Use pandas built-in to_json for table format
                df.to_json(
                    self.file_path,
                    orient=self.orient,
                    date_format=self.date_format,
                    compression=self.compression,
                    indent=self.indent
                )
            else:
                # Use custom serialization for other formats
                json_data = df.to_dict(orient=self.orient)
                with open(self.file_path, 'w') as f:
                    json.dump(json_data, f, default=json_serial, indent=self.indent)
                    
            self.logger.info(f"Successfully wrote {len(df)} records to JSON file")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading data to JSON file: {str(e)}")
            raise
```

### Component Interaction and Data Flow

The following sequence diagram shows how the components interact during a JSON ETL process:

```
┌───────────┐          ┌──────────────┐          ┌──────────────┐          ┌──────────────┐          ┌───────────┐
│  main.py  │          │  ETLPipeline │          │JSONExtractor │          │JSONTransform │          │  Loaders  │
└─────┬─────┘          └──────┬───────┘          └──────┬───────┘          └──────┬───────┘          └─────┬─────┘
      │                       │                         │                         │                         │
      │ parse_args()          │                         │                         │                         │
      │───────────────────────>                         │                         │                         │
      │                       │                         │                         │                         │
      │                       │ load_config()           │                         │                         │
      │                       │◄──────────────          │                         │                         │
      │                       │                         │                         │                         │
      │                       │ _initialize_components()│                         │                         │
      │                       │◄──────────────          │                         │                         │
      │                       │                         │                         │                         │
      │                       │                         │                         │                         │
      │                       │ validate_source()       │                         │                         │
      │                       │────────────────────────>│                         │                         │
      │                       │                         │                         │                         │
      │                       │ _run_extraction()       │                         │                         │
      │                       │────────────────────────>│                         │                         │
      │                       │                         │                         │                         │
      │                       │                         │ extract()               │                         │
      │                       │                         │◄──────────              │                         │
      │                       │                         │                         │                         │
      │                       │                         │ json.load()             │                         │
      │                       │                         │◄──────────              │                         │
      │                       │                         │                         │                         │
      │                       │                         │ apply JSON path         │                         │
      │                       │                         │◄──────────              │                         │
      │                       │                         │                         │                         │
      │                       │                         │ pd.json_normalize()     │                         │
      │                       │                         │◄──────────              │                         │
      │                       │                         │                         │                         │
      │                       │ DataFrame               │                         │                         │
      │                       │<────────────────────────│                         │                         │
      │                       │                         │                         │                         │
      │                       │ _run_transformation()   │                         │                         │
      │                       │────────────────────────────────────────────────>  │                         │
      │                       │                         │                         │                         │
      │                       │                         │                         │ flatten_nested_dict()   │
      │                       │                         │                         │◄──────────              │
      │                       │                         │                         │                         │
      │                       │                         │                         │ validate_against_schema()│
      │                       │                         │                         │◄──────────              │
      │                       │                         │                         │                         │
      │                       │ Transformed DataFrame   │                         │                         │
      │                       │<────────────────────────────────────────────────  │                         │
      │                       │                         │                         │                         │
      │                       │ _run_loading()          │                         │                         │
      │                       │──────────────────────────────────────────────────────────────────────────>  │
      │                       │                         │                         │                         │
      │                       │                         │                         │                         │ load()
      │                       │                         │                         │                         │◄──────
      │                       │                         │                         │                         │
      │                       │ Loading Result          │                         │                         │
      │                       │<──────────────────────────────────────────────────────────────────────────  │
      │                       │                         │                         │                         │
      │ run() result          │                         │                         │                         │
      │<──────────────────────│                         │                         │                         │
      │                       │                         │                         │                         │
┌─────┴─────┐          ┌──────┴───────┐          ┌──────┴───────┐          ┌──────┴───────┐          ┌─────┴─────┐
│  main.py  │          │  ETLPipeline │          │JSONExtractor │          │JSONTransform │          │  Loaders  │
└───────────┘          └──────────────┘          └──────────────┘          └──────────────┘          └───────────┘
```

### JSON-Specific Implementation Details

#### Handling Nested JSON Structures

The framework handles nested JSON structures using specialized techniques:

```python
def _unnest_json_column(df, column_name, prefix=''):
    """Unnest a JSON column into multiple columns."""
    # Skip if column doesn't exist
    if column_name not in df.columns:
        return df
    
    # Skip if column is not a nested structure
    if not df[column_name].apply(lambda x: isinstance(x, dict)).any():
        return df
    
    # Create temporary DataFrame with unnested structure
    temp_df = pd.json_normalize(df[column_name].tolist())
    
    # Rename columns with prefix
    temp_df.columns = [f"{prefix}{c}" for c in temp_df.columns]
    
    # Join unnested columns to original DataFrame
    result_df = pd.concat([df.drop(columns=[column_name]), temp_df], axis=1)
    
    return result_df
```

#### Working with JSONPath for Flexible Extraction

The framework implements JSONPath support for targeted data extraction:

```python
def extract_with_jsonpath(json_data, json_path_expr):
    """Extract specific elements from JSON using JSONPath."""
    import jsonpath_ng.ext as jsonpath
    
    try:
        expr = jsonpath.parse(json_path_expr)
        matches = [match.value for match in expr.find(json_data)]
        return matches
    except Exception as e:
        logger.error(f"JSONPath extraction error: {str(e)}")
        raise
```

This detailed technical information provides a comprehensive understanding of how the JSON ETL process is implemented in the framework, from module structure to specific class implementations and the specialized techniques used for working with complex JSON data.
