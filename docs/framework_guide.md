# Data Warehouse ETL Framework - Developer & User Guide

## Table of Contents

1. [Introduction](#introduction)
2. [Framework Overview](#framework-overview)
   - [Architecture](#architecture)
   - [Components](#components)
   - [Data Flow](#data-flow)
3. [Installation](#installation)
4. [User Guide](#user-guide)
   - [Configuration Files](#configuration-files)
   - [Running ETL Jobs](#running-etl-jobs)
   - [Configuration Examples](#configuration-examples)
   - [Logging and Monitoring](#logging-and-monitoring)
   - [Troubleshooting](#troubleshooting)
5. [Developer Guide](#developer-guide)
   - [Extension Points](#extension-points)
   - [Building Custom Extractors](#building-custom-extractors)
   - [Building Custom Transformers](#building-custom-transformers)
   - [Building Custom Loaders](#building-custom-loaders)
   - [Testing Components](#testing-components)
6. [Advanced Topics](#advanced-topics)
   - [Error Handling Strategies](#error-handling-strategies)
   - [Performance Optimization](#performance-optimization)
   - [Security Considerations](#security-considerations)
7. [API Reference](#api-reference)
8. [Best Practices](#best-practices)
9. [FAQ](#faq)

## Introduction

The Data Warehouse ETL Framework is a robust, modular system for Extract-Transform-Load (ETL) operations. It provides a flexible and configurable approach to building data pipelines that can read from various sources, apply transformations, and load data to multiple destinations.

This guide serves as a comprehensive resource for both users who want to configure and run ETL jobs, and developers who want to extend the framework with custom components.

## Framework Overview

### Architecture

The framework follows a modular architecture with three main component types:

1. **Extractors**: Read data from sources (CSV, JSON, SQL databases, etc.)
2. **Transformers**: Process and modify data (cleaning, normalization, validation)
3. **Loaders**: Write data to destinations (databases, files, etc.)

These components are orchestrated by the **ETL Pipeline**, which handles the data flow, error management, and metrics collection.

![ETL Framework Architecture](framework_architecture.png)

### Components

#### Core Components

- **Pipeline**: The main orchestrator that coordinates the ETL process
- **ConfigManager**: Handles loading and validating configuration files
- **Logging Utilities**: Provides comprehensive logging capabilities

#### Data Processing Components

- **Extractors**: Base extractor and implementations for different data sources
- **Transformers**: Base transformer and implementations for various transformations
- **Loaders**: Base loader and implementations for different data destinations

### Data Flow

1. **Extraction Phase**: Data is read from one or more sources
2. **Transformation Phase**: Data is processed through a sequence of transformers
3. **Loading Phase**: Processed data is written to one or more destinations

Each phase has validation steps to ensure data quality and integrity. Metrics are collected at each step for monitoring and analysis.

## Installation

### Prerequisites

- Python 3.7 or higher
- pip package manager

### Setup

1. Clone the repository or download the framework
2. Create and activate a virtual environment:
   ```powershell
   # Windows PowerShell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   
   # Linux/macOS
   python -m venv .venv
   source .venv/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## User Guide

### Configuration Files

The framework uses YAML configuration files to define ETL jobs. Each configuration file contains four main sections:

1. **extractors**: Defines data sources
2. **transformers**: Defines data transformations (optional)
3. **loaders**: Defines data destinations
4. **pipeline**: General pipeline settings (optional)

#### Basic Configuration Structure

```yaml
# Sample configuration structure
extractors:
  source_name:
    type: extractor_type
    # extractor-specific configuration

transformers:
  transform_name:
    type: transformer_type
    # transformer-specific configuration

loaders:
  destination_name:
    type: loader_type
    # loader-specific configuration

pipeline:
  name: "Pipeline Name"
  # general pipeline settings
```

### Running ETL Jobs

To run an ETL job, use the command-line interface:

```bash
python main.py --config config/your_config.yaml
```

Additional options:
- `--log-file LOG_FILE`: Specify a custom log file path
- `--log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}`: Set logging level
- `--console-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}`: Set console logging level
- `--validate-only`: Validate configuration without running the pipeline

### Configuration Examples

#### CSV to Database Example

```yaml
extractors:
  customer_data:
    type: csv
    file_path: ./data/customers.csv
    delimiter: ","
    encoding: utf-8
    header: true

transformers:
  data_cleaning:
    type: cleaning
    operations:
      - drop_duplicates:
          subset: ["customer_id"]
      - fill_na:
          columns:
            age: 0

loaders:
  database:
    type: sql
    connection_string: "postgresql://user:password@localhost:5432/warehouse"
    table_name: "customers"
    if_exists: "append"
```

#### Multiple Sources Example

```yaml
extractors:
  customers:
    type: csv
    file_path: ./data/customers.csv
  
  orders:
    type: json
    file_path: ./data/orders.json

transformers:
  # Transformations here

loaders:
  analytics_db:
    type: sql
    # SQL configuration
```

### Logging and Monitoring

The framework provides detailed logging to track the ETL process. Logs include:

- Start and end of each phase
- Number of rows processed
- Errors and warnings
- Performance metrics

Logs are written to both the console and a log file. The default log file is `etl.log` in the current directory, but this can be customized.

### Troubleshooting

Common issues and their solutions:

#### Configuration Errors

- **Invalid YAML syntax**: Check for proper indentation and formatting
- **Missing required fields**: Ensure all required configuration fields are present
- **Invalid component types**: Verify that the specified component types exist

#### Extraction Issues

- **File not found**: Check file paths and permissions
- **Database connection errors**: Verify connection strings and credentials
- **Data format errors**: Ensure source data matches expected format

#### Transformation Issues

- **Data type errors**: Check for incompatible data types in transformations
- **Missing columns**: Verify that required columns exist in the data

#### Loading Issues

- **Database permission errors**: Check database user permissions
- **Schema validation errors**: Ensure data schema matches destination requirements

## Developer Guide

### Extension Points

The framework provides three main extension points:

1. **Custom Extractors**: For reading data from new sources
2. **Custom Transformers**: For implementing new data transformations
3. **Custom Loaders**: For writing data to new destinations

### Building Custom Extractors

To create a custom extractor:

1. Create a new Python file in the `src/extractors` directory
2. Inherit from the `BaseExtractor` class
3. Implement the required methods:
   - `extract()`: Read data from the source
   - `validate_source()`: Validate the data source

#### Example Custom Extractor

```python
from src.extractors.base_extractor import BaseExtractor
import pandas as pd

class MyCustomExtractor(BaseExtractor):
    def __init__(self, config):
        super().__init__(config)
        self.source_path = config.get("source_path")
        # Initialize other parameters
    
    def validate_source(self):
        # Implement source validation
        return True  # or False if validation fails
    
    def extract(self):
        # Implement data extraction
        # Return a pandas DataFrame
        return pd.DataFrame(...)
```

### Building Custom Transformers

To create a custom transformer:

1. Create a new Python file in the `src/transformers` directory
2. Inherit from the `BaseTransformer` class
3. Implement the required methods:
   - `transform()`: Process the input data

#### Example Custom Transformer

```python
from src.transformers.base_transformer import BaseTransformer
import pandas as pd

class MyCustomTransformer(BaseTransformer):
    def __init__(self, config):
        super().__init__(config)
        self.parameters = config.get("parameters", {})
        # Initialize other configuration values
    
    def transform(self, data):
        # Implement data transformation
        # Input: DataFrame or list of DataFrames
        # Output: DataFrame or list of DataFrames
        return processed_data
```

### Building Custom Loaders

To create a custom loader:

1. Create a new Python file in the `src/loaders` directory
2. Inherit from the `BaseLoader` class
3. Implement the required methods:
   - `load()`: Write data to the destination
   - `validate_destination()`: Validate the data destination

#### Example Custom Loader

```python
from src.loaders.base_loader import BaseLoader
import pandas as pd

class MyCustomLoader(BaseLoader):
    def __init__(self, config):
        super().__init__(config)
        self.destination = config.get("destination")
        # Initialize other parameters
    
    def validate_destination(self):
        # Implement destination validation
        return True  # or False if validation fails
    
    def load(self, data):
        # Implement data loading
        # Input: DataFrame or list of DataFrames
        # Return True for success, False for failure
        return True
```

### Testing Components

Each component should have corresponding tests in the `tests` directory. Use pytest for writing and running tests.

#### Testing Extractors

```python
import pytest
from src.extractors.my_custom_extractor import MyCustomExtractor

def test_extractor_initialization():
    config = {"source_path": "test/path"}
    extractor = MyCustomExtractor(config)
    assert extractor.source_path == "test/path"

def test_extraction():
    config = {"source_path": "test/data/sample.dat"}
    extractor = MyCustomExtractor(config)
    data = extractor.extract()
    assert isinstance(data, pd.DataFrame)
    assert len(data) > 0
```

## Advanced Topics

### Error Handling Strategies

The framework supports different error handling strategies:

- **Fail Fast**: Stop pipeline execution on the first error
- **Continue on Error**: Log errors but continue processing
- **Retry**: Attempt to retry failed operations

Configure error handling in the pipeline section:

```yaml
pipeline:
  continue_on_error: true
  retry:
    max_attempts: 3
    delay_seconds: 5
```

### Performance Optimization

Techniques for optimizing ETL performance:

- **Chunked Processing**: Process large datasets in chunks
- **Parallel Extraction**: Extract from multiple sources in parallel
- **Efficient Transformations**: Optimize transformation operations
- **Bulk Loading**: Use bulk loading methods for databases

Configure chunking in extractor and loader components:

```yaml
extractors:
  source_name:
    type: sql
    chunksize: 10000  # Process 10,000 rows at a time

loaders:
  destination_name:
    type: sql
    chunksize: 5000  # Load 5,000 rows at a time
```

### Security Considerations

Best practices for security:

- **Environment Variables**: Store sensitive information in environment variables
- **Connection Pooling**: Use connection pools for database connections
- **Input Validation**: Validate all input data
- **Access Control**: Implement proper access controls for data sources and destinations

## API Reference

### Extractor API

| Method | Description |
|--------|-------------|
| `__init__(config)` | Initialize with configuration |
| `extract()` | Extract data from source |
| `validate_source()` | Validate source configuration |

### Transformer API

| Method | Description |
|--------|-------------|
| `__init__(config)` | Initialize with configuration |
| `transform(data)` | Transform input data |

### Loader API

| Method | Description |
|--------|-------------|
| `__init__(config)` | Initialize with configuration |
| `load(data)` | Load data to destination |
| `validate_destination()` | Validate destination configuration |

### Pipeline API

| Method | Description |
|--------|-------------|
| `__init__(config_path)` | Initialize with configuration |
| `setup()` | Set up pipeline components |
| `run()` | Run the ETL pipeline |
| `get_metrics()` | Get pipeline metrics |

## Best Practices

### Configuration Management

- Use environment variables for sensitive information
- Separate configurations for different environments
- Use descriptive names for components

### Data Validation

- Validate data at source
- Implement data quality checks in transformations
- Validate data before loading

### Error Handling

- Log detailed error information
- Implement appropriate retry mechanisms
- Define fallback strategies for critical operations

### Performance

- Process data in chunks for large datasets
- Use efficient data structures
- Optimize database operations

## FAQ

### General Questions

**Q: Can the framework handle real-time data processing?**
A: The framework is primarily designed for batch processing, but can be adapted for near-real-time processing with scheduled jobs.

**Q: How do I handle dependencies between transformations?**
A: Transformers are executed in the order they appear in the configuration file. Ensure they are listed in the correct dependency order.

### Troubleshooting

**Q: My pipeline fails with memory errors on large datasets. What can I do?**
A: Configure chunked processing in both extractors and loaders to reduce memory usage.

**Q: How do I debug a failing transformation?**
A: Set the log level to DEBUG for detailed information. Add validation transformers to check data at different points in the pipeline.

---

*This guide is a living document and will be updated as the framework evolves.*
