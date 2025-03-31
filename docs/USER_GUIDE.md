# Data Warehouse ETL Framework - User Guide

## Table of Contents
1. [Introduction](#introduction)
2. [Getting Started](#getting-started)
3. [Installation](#installation)
4. [Running ETL Jobs](#running-etl-jobs)
5. [Configuration](#configuration)
6. [Data Sources](#data-sources)
7. [Transformations](#transformations)
8. [Data Loading](#data-loading)
9. [Monitoring and Logging](#monitoring-and-logging)
10. [Testing](#testing)
11. [Troubleshooting](#troubleshooting)
12. [Best Practices](#best-practices)
13. [Examples](#examples)

## Introduction

The Data Warehouse ETL Framework is a flexible, extensible platform for building Extract-Transform-Load (ETL) pipelines. It provides a standardized approach to moving data between various sources and destinations, with configurable transformations along the way.

Key features:
- Support for multiple data source types (CSV, JSON, SQL databases)
- Configurable transformation pipeline
- Multiple output formats (SQL, CSV, JSON)
- Extensive logging and error handling
- Modular design for easy extension

## Getting Started

### System Requirements
- Python 3.8 or higher
- Adequate disk space for data processing
- Database connectivity (if using database sources/targets)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/your-organization/data-warehouse-etl.git
cd data-warehouse-etl
```

2. Create and activate a virtual environment:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate
```

3. Install the required dependencies:
```powershell
pip install -r requirements.txt
```

## Running ETL Jobs

The framework uses a command-line interface to run ETL jobs. Basic usage:

```powershell
python main.py --config <path_to_config_file>
```

### Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--config` | Path to the YAML configuration file (required) | None |
| `--log-level` | Logging level (DEBUG, INFO, WARNING, ERROR) | INFO |
| `--log-file` | Path to the log file | etl.log |
| `--console-level` | Console logging level (DEBUG, INFO, WARNING, ERROR) | INFO |
| `--output-dir` | Directory for output files | ./output |
| `--dry-run` | Validate the config and set up the pipeline without running | False |
| `--validate-only` | Only validate the configuration | False |
| `--job-id` | Custom job ID | Auto-generated |

### Examples

1. **Basic execution**:
```powershell
python main.py --config config\sample_etl_config.yaml
```

2. **Detailed logging**:
```powershell
python main.py --config config\sample_etl_config.yaml --log-level DEBUG
```

3. **Configuration validation only**:
```powershell
python main.py --config config\sample_etl_config.yaml --validate-only
```

4. **Custom output directory**:
```powershell
python main.py --config config\sample_etl_config.yaml --output-dir custom_output
```

5. **Dry run (setup without execution)**:
```powershell
python main.py --config config\sample_etl_config.yaml --dry-run
```

6. **Custom job ID**:
```powershell
python main.py --config config\sample_etl_config.yaml --job-id my_job_2025_03_24
```

7. **Combined options**:
```powershell
python main.py --config config\sample_etl_config.yaml --log-level DEBUG --output-dir custom_output --job-id test_run_01
```

## Configuration

The ETL framework uses YAML configuration files to define the entire ETL process. A configuration file includes:

- Extractors: Data sources configuration
- Transformers: Data transformation steps
- Loaders: Output destinations configuration

### Sample Configuration

```yaml
job:
  name: sample_etl_job
  description: "Sample ETL job for demonstration"

extractors:
  customer_data:
    type: CSVExtractor
    class: src.extractors.csv_extractor.CSVExtractor
    config:
      file_path: "./data/customers.csv"
      delimiter: ","
      encoding: "utf-8"

  order_data:
    type: JSONExtractor
    class: src.extractors.json_extractor.JSONExtractor
    config:
      file_path: "./data/orders.json"
      encoding: "utf-8"

transformers:
  data_cleaning:
    type: CleaningTransformer
    class: src.transformers.cleaning_transformer.CleaningTransformer
    config:
      remove_duplicates: true
      fill_missing_values:
        strategy: "mean"
        columns: ["age", "salary"]

loaders:
  warehouse_db:
    type: SQLLoader
    class: src.loaders.sql_loader.SQLLoader
    config:
      connection_string: "sqlite:///./output/warehouse.db"
      table_name: "customer_data"
      if_exists: "replace"
      serialize_json: true
```

## Data Sources

The framework supports multiple data source types through its extractors:

1. **CSV Files**
   - Configuration options:
     - `file_path`: Path to the CSV file
     - `delimiter`: Field delimiter (default: ',')
     - `encoding`: File encoding (default: utf-8)
     - `header`: Whether the file has a header row (default: true)

2. **JSON Files**
   - Configuration options:
     - `file_path`: Path to the JSON file
     - `encoding`: File encoding (default: utf-8)
     - `orient`: JSON format orientation (default: records)

3. **SQL Databases**
   - Configuration options:
     - `connection_string`: Database connection URL
     - `query`: SQL query to execute
     - `table`: Table name (alternative to query)
     - `schema`: Schema name (optional)

4. **REST APIs**
   - Configuration options:
     - `base_url`: Base URL of the API
     - `endpoint`: API endpoint path
     - `method`: HTTP method (GET, POST, etc.)
     - `params`: Query parameters
     - `headers`: HTTP headers
     - `body`: Request body (for POST/PUT/PATCH)
     - `auth`: Authentication configuration
     - `pagination`: Pagination configuration
     - `rate_limit`: Rate limiting settings
     - `retry`: Retry configuration for failed requests

### REST API Configuration Details

The API Extractor supports comprehensive configuration options:

#### Authentication

```yaml
auth:
  type: "basic"  # Options: basic, bearer, api_key, oauth2
  username: "your_username"  # For basic auth
  password: "your_password"  # For basic auth
  token: "your_token"  # For bearer auth
  key: "your_api_key"  # For api_key auth
  location: "header"  # For api_key auth: header or query
  param_name: "X-API-Key"  # For api_key auth: name of header or query param
  
  # For OAuth2
  client_id: "your_client_id"
  client_secret: "your_client_secret"
  token_url: "https://auth.example.com/oauth/token"
```

#### Pagination

```yaml
pagination:
  enabled: true
  type: "offset"  # Options: offset, cursor
  params:
    page_param: "page"  # Parameter name for page number
    per_page_param: "per_page"  # Parameter for results per page
    per_page: 100  # Number of results per page
    cursor_param: "next_token"  # For cursor-based pagination
    results_path: "data.items"  # JSONPath to results array
    next_cursor_path: "meta.next_token"  # JSONPath to next cursor
  max_pages: 10  # Maximum pages to retrieve
```

#### Rate Limiting and Retries

```yaml
rate_limit:
  requests_per_minute: 60
  requests_per_day: 5000  # Optional daily limit

retry:
  max_retries: 3
  delay_seconds: 1
  backoff_factor: 2  # Each retry waits backoff_factor times longer
```

### Example API Extraction Configuration

Here's a complete example for extracting data from GitHub's API:

```yaml
extractors:
  - name: github_issues
    type: api
    config:
      base_url: "https://api.github.com"
      endpoint: "repos/organization/repository/issues"
      method: "GET"
      params:
        state: "all"
        sort: "created"
        direction: "desc"
      headers:
        Accept: "application/vnd.github.v3+json"
        User-Agent: "ETL-Framework/1.0"
      auth:
        type: "bearer"
        token: "${GITHUB_TOKEN}"  # Environment variable
      pagination:
        enabled: true
        type: "offset"
        params:
          page_param: "page"
          per_page_param: "per_page"
          per_page: 100
        max_pages: 10
      rate_limit:
        requests_per_minute: 30
      retry:
        max_retries: 3
        delay_seconds: 2
        backoff_factor: 2
```

## Transformations

The framework provides various transformers for data processing:

1. **Cleaning Transformer**
   - Handles data cleaning operations like removing duplicates, handling missing values, etc.
   - Configuration examples in the [Examples](#examples) section

2. **Flattening Transformer**
   - Flattens nested JSON structures into tabular format
   - Configuration options:
     - `flatten_fields`: Fields to flatten, with paths and prefixes
     - `separator`: Character to use when joining field names
     - `drop_original`: Whether to drop original nested fields
     - `max_depth`: Maximum depth to flatten (default: 10)
     - `array_strategy`: How to handle arrays (`first`, `join`, or `expand`)

3. **JSON Transformer**
   - Performs JSON-specific operations
   - Configuration options:
     - `select_fields`: Fields to keep in the output
     - `rename_fields`: Fields to rename (old_name: new_name)
     - `type_casting`: Type conversions to apply (`field: type`)
     - `calculated_fields`: New fields to calculate based on expressions
     - `drop_fields`: Fields to remove from the output

### JSON Transformer Advanced Features

The JSON Transformer provides powerful data transformation capabilities:

#### Type Casting

Converts fields to specified data types:

```yaml
type_casting:
  user_id: "int"
  score: "float"
  is_active: "bool"
  created_at: "datetime:%Y-%m-%d %H:%M:%S"
  birthdate: "date:%Y-%m-%d"
```

Supported types:
- `int`: Integer values
- `float`: Floating-point numbers
- `bool`: Boolean values (handles "yes"/"no", "true"/"false", "1"/"0")
- `date`: Date values (with optional format)
- `datetime`: Date and time values (with optional format)

#### Calculated Fields

Create new fields based on expressions:

```yaml
calculated_fields:
  full_name: "first_name + ' ' + last_name"
  total_amount: "price * quantity"
  discount_price: "price * (1 - discount_rate)"
  is_premium: "subscription_type == 'premium'"
  days_since_join: "(today() - join_date).days"
```

### Flattening Transformer Advanced Features

The Flattening Transformer handles complex nested structures:

#### Array Handling Strategies

```yaml
flatten_fields:
  - path: "order.items"
    prefix: "item_"
    array_strategy: "first"  # Only keep the first item
    
  - path: "tags"
    prefix: "tag_"
    array_strategy: "join"  # Join with delimiter
    delimiter: ", "
    
  - path: "user.addresses"
    prefix: "address_"
    array_strategy: "expand"  # Create numbered columns (address_0_city, address_1_city)
    max_items: 3  # Only expand up to 3 items
```

#### Example Flattening Configuration

```yaml
transformers:
  - name: api_response_flattener
    type: flattening
    config:
      flatten_fields:
        - path: "user"
          prefix: "user_"
        - path: "order.items"
          prefix: "item_"
          array_strategy: "first"
        - path: "location.coordinates"
          prefix: "coord_"
      max_depth: 5
      drop_original: true
```

### Advanced Transformation Example

This example demonstrates a comprehensive transformation pipeline for API data:

```yaml
transformers:
  - name: response_flattener
    type: flattening
    config:
      flatten_fields:
        - path: "user"
          prefix: "user_"
        - path: "metadata"
          prefix: "meta_"
      drop_original: true
  
  - name: data_converter
    type: json
    config:
      select_fields:
        - "id"
        - "user_name"
        - "user_email"
        - "meta_created_at"
        - "meta_updated_at"
        - "status"
      rename_fields:
        user_name: "username"
        user_email: "email"
      type_casting:
        id: "int"
        meta_created_at: "datetime:%Y-%m-%dT%H:%M:%S"
        meta_updated_at: "datetime:%Y-%m-%dT%H:%M:%S"
        status: "bool"
      calculated_fields:
        days_active: "(today() - meta_created_at).days"
        is_recently_updated: "(today() - meta_updated_at).days < 30"
```

## Data Loading

### Supported Destination Types

1. **SQL Databases**
   - Configuration options:
     - `connection_string`: Database connection URL
     - `table_name`: Target table name
     - `schema`: Database schema
     - `if_exists`: Strategy for existing table (replace, append, fail)
     - `chunksize`: Batch size for loading

2. **CSV Files**
   - Configuration options:
     - `file_path`: Output CSV file path
     - `delimiter`: Column delimiter
     - `encoding`: File encoding
     - `include_header`: Whether to include header row
     - `mode`: Write mode (w or a)

3. **JSON Files**
   - Configuration options:
     - `file_path`: Output JSON file path
     - `orient`: JSON format orientation
     - `lines`: Whether to write in JSON Lines format
     - `indent`: Indentation for pretty-printing

## Monitoring and Logging

The ETL framework provides comprehensive logging to help track the progress and diagnose issues with your ETL jobs.

### Log Levels

The framework supports several log levels, which can be set via the `--log-level` command-line option:

- `DEBUG`: Detailed debug information
- `INFO`: General information about job progress (default)
- `WARNING`: Warnings that don't prevent execution
- `ERROR`: Error information when something goes wrong
- `CRITICAL`: Critical issues that cause job failure

### Log Output

Logs are written to both:
- Console (stdout)
- Log file (in the current directory by default)

Example log output:
```
2025-03-24 03:55:14 [INFO] [pipeline:92] ETL pipeline setup complete with 3 extractors, 2 transformers, and 2 loaders
2025-03-24 03:55:14 [INFO] [pipeline:158] Starting data extraction...
2025-03-24 03:55:15 [INFO] [pipeline:173] Extraction complete, records extracted: 1250
2025-03-24 03:55:15 [INFO] [pipeline:182] Starting data transformation...
```

## Testing

### Validating Configurations

To validate your configuration file without executing the pipeline:

```powershell
python main.py --config config\my_etl_config.yaml --validate-only
```

This will check that:
- The configuration file exists and can be parsed
- Required sections (extractors, loaders) are present
- Component configurations have required fields
- The pipeline structure is valid

### Dry Run Mode

To prepare the pipeline without executing it (useful for testing setup):

```powershell
python main.py --config config\my_etl_config.yaml --dry-run
```

This will:
- Validate the configuration
- Initialize all components
- Set up the pipeline
- Report readiness without executing the data processing

### Testing Your ETL Pipeline

If you're developing custom components or modifying the framework, you can run the included test suite:

```powershell
# Run all tests
python run_tests.py

# Run tests for specific components
python run_tests.py api    # Run only API-related tests
python run_tests.py csv    # Run only CSV-related tests
python run_tests.py db     # Run only Database-related tests
python run_tests.py json   # Run only JSON-related tests

# Run tests for other framework components
python run_tests.py transformer  # Run transformer-related tests
python run_tests.py loader       # Run loader-related tests
python run_tests.py extractor    # Run extractor-related tests
python run_tests.py pipeline     # Run pipeline-related tests
python run_tests.py cli          # Run CLI-related tests

# Run specific test files using unittest directly
python -m unittest tests.test_cli_options
```

The test runner provides a comprehensive summary report:

```
======================================================================
TEST SUMMARY
======================================================================
Tests executed: 50
Tests passed: 50
Tests failed: 0
Test errors: 0
Execution time: 1.92 seconds
======================================================================
```

This component-based testing approach allows you to focus on the specific areas of the ETL pipeline you're working with, saving time during development and troubleshooting.

## Troubleshooting

### Common Issues and Solutions

1. **Connection Errors**
   - Check network connectivity
   - Verify connection strings and credentials
   - Ensure firewall and security settings allow connections

2. **Memory Issues**
   - Increase available memory
   - Process data in smaller chunks
   - Optimize transformations to use less memory

3. **Performance Problems**
   - Add appropriate indexes to database tables
   - Use efficient SQL queries
   - Consider partitioning large datasets

4. **Data Type Errors**
   - Verify that source data matches expected schemas
   - Use appropriate type conversions in transformations
   - Check for nulls and special values

### Debug Mode

For detailed troubleshooting, run the ETL job with debug logging:

```powershell
python main.py --config config\sample_etl_config.yaml --log-level DEBUG
```

## Best Practices

1. **Configuration Management**
   - Use separate configuration files for different environments
   - Version control your configuration files
   - Document all configuration parameters

2. **Data Quality**
   - Always include validation in your transformation pipeline
   - Use appropriate data types in your schemas
   - Handle missing or invalid data explicitly

3. **Performance**
   - Process data in chunks for large datasets
   - Configure appropriate batch sizes for database operations
   - Monitor memory usage for large transformations

4. **Error Handling**
   - Implement retry logic for transient errors
   - Set up alerts for critical failures
   - Preserve partial results when possible

## Examples

### Complete ETL Job Example

#### Configuration file: sample_etl_config.yaml
```yaml
job:
  name: customer_order_etl
  description: "Process customer and order data"

extractors:
  customer_data:
    type: CSVExtractor
    class: src.extractors.csv_extractor.CSVExtractor
    config:
      file_path: "./data/customers.csv"
      delimiter: ","
      encoding: "utf-8"

  order_data:
    type: JSONExtractor
    class: src.extractors.json_extractor.JSONExtractor
    config:
      file_path: "./data/orders.json"
      encoding: "utf-8"

  product_data:
    type: SQLExtractor
    class: src.extractors.sql_extractor.SQLExtractor
    config:
      connection_string: "sqlite:///./data/product_db.sqlite"
      query: "SELECT * FROM products"

transformers:
  data_cleaning:
    type: CleaningTransformer
    class: src.transformers.cleaning_transformer.CleaningTransformer
    config:
      remove_duplicates: true
      fill_missing_values:
        strategy: "mean"
        columns: ["price", "quantity"]

  data_normalization:
    type: NormalizationTransformer
    class: src.transformers.normalization_transformer.NormalizationTransformer
    config:
      methods:
        - numeric_scaling:
            columns:
              price:
                method: "min_max"
                feature_range: [0, 1]
              quantity:
                method: "min_max"
                feature_range: [0, 1]
        - date_format:
            columns:
              order_date: "%Y-%m-%d"

loaders:
  warehouse_db:
    type: SQLLoader
    class: src.loaders.sql_loader.SQLLoader
    config:
      connection_string: "sqlite:///./output/warehouse.db"
      table_name: "customer_data"
      if_exists: "replace"
      serialize_json: true

  analytics_csv:
    type: CSVLoader
    class: src.loaders.csv_loader.CSVLoader
    config:
      file_path: "./output/customer_analytics.csv"
      delimiter: ","
      encoding: "utf-8"
      mode: "w"
      include_header: true

  metrics_json:
    type: JSONLoader
    class: src.loaders.json_loader.JSONLoader
    config:
      file_path: "./output/metrics.json"
      orient: "records"
      indent: 2
```

#### Command to run the job:
```powershell
python main.py --config config\sample_etl_config.yaml
```

#### Expected output:
```
2025-03-24 03:26:17 [INFO] [root:91] ETL logging initialized at level: DEBUG
2025-03-24 03:26:17 [INFO] [src.cli:157] Data Warehouse ETL Framework
2025-03-24 03:26:17 [INFO] [src.cli:158] Configuration file: config\sample_etl_config.yaml
...
2025-03-24 03:26:18 [INFO] [src.cli:182] ETL pipeline completed in 1.73 seconds with status: completed_success
```

This completes the User Guide for the Data Warehouse ETL Framework. For more advanced topics and customization options, please refer to the Developer Guide.
