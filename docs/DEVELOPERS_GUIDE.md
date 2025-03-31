# Data Warehouse ETL Framework - Developer's Guide

## Table of Contents
1. [Introduction](#introduction)
2. [Architecture Overview](#architecture-overview)
3. [Code Structure](#code-structure)
4. [Extension Points](#extension-points)
5. [Component Interactions](#component-interactions)
6. [Class Hierarchies](#class-hierarchies)
7. [Creating Custom Components](#creating-custom-components)
8. [Configuration System](#configuration-system)
9. [Logging and Error Handling](#logging-and-error-handling)
10. [Testing](#testing)
11. [Best Practices](#best-practices)
12. [Advanced Examples](#advanced-examples)
13. [Command-Line Interface](#command-line-interface)

## Introduction

This Developer's Guide provides in-depth technical information for extending, enhancing, and maintaining the Data Warehouse ETL Framework. While the User Guide focuses on using the framework, this guide covers the internal architecture, design patterns, and customization approaches.

The framework is designed around three core concepts:
- **Extractors**: Components that obtain data from various sources
- **Transformers**: Components that modify and process data
- **Loaders**: Components that send processed data to destinations

## Architecture Overview

### High-Level Architecture

The ETL framework follows a modular, pipeline-based architecture:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│             │    │             │    │             │
│  Extractors ├───►│ Transformers├───►│   Loaders   │
│             │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘
        │                 │                  │
        │                 │                  │
        ▼                 ▼                  ▼
┌─────────────────────────────────────────────────┐
│                                                 │
│              Configuration Manager              │
│                                                 │
└─────────────────────────────────────────────────┘
                        │
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│                                                 │
│              Logging and Monitoring             │
│                                                 │
└─────────────────────────────────────────────────┘
```

### Key Components

1. **ETL Pipeline**: Orchestrates the entire ETL process
2. **Configuration Manager**: Loads and validates configuration files
3. **Component Registry**: Manages extractor, transformer, and loader components
4. **Logging System**: Provides comprehensive logging and monitoring
5. **Error Handling**: Manages exceptions and error recovery

## Code Structure

```
data_warehouse_etl/
│
├── config/               # Configuration files
│   └── sample_etl_config.yaml
│
├── data/                 # Sample data files
│   ├── customers.csv
│   ├── orders.json
│   └── product_db.sqlite
│
├── docs/                 # Documentation
│   └── framework_guide.md
│
├── output/               # Output directory
│
├── logs/                 # Log files directory
│
├── src/                  # Source code
│   ├── __init__.py
│   ├── cli.py            # Command-line interface
│   ├── pipeline.py       # Main ETL pipeline orchestration
│   │
│   ├── extractors/       # Data extraction components
│   │   ├── __init__.py
│   │   ├── base_extractor.py
│   │   ├── csv_extractor.py
│   │   ├── json_extractor.py
│   │   └── sql_extractor.py
│   │
│   ├── transformers/     # Data transformation components
│   │   ├── __init__.py
│   │   ├── base_transformer.py
│   │   ├── cleaning_transformer.py
│   │   ├── normalization_transformer.py
│   │   └── validation_transformer.py
│   │
│   ├── loaders/          # Data loading components
│   │   ├── __init__.py
│   │   ├── base_loader.py
│   │   ├── csv_loader.py
│   │   ├── json_loader.py
│   │   └── sql_loader.py
│   │
│   └── utils/            # Utility modules
│       ├── __init__.py
│       ├── config_manager.py
│       └── logging_utils.py
│
├── tests/                # Unit and integration tests
│   ├── __init__.py
│   ├── test_cli_options.py       # Command-line interface option tests
│   ├── test_pipeline_options.py  # Pipeline configuration option tests
│   ├── test_extractors/
│   │   ├── __init__.py
│   │   ├── test_csv_extractor.py
│   │   ├── test_json_extractor.py
│   │   └── test_sql_extractor.py
│   ├── test_transformers/
│   │   ├── __init__.py
│   │   └── test_*.py
│   ├── test_loaders/
│   │   ├── __init__.py
│   │   └── test_*.py
│   └── test_integration/
│       ├── __init__.py
│       └── test_full_pipeline.py
├── main.py               # Main entry point
├── requirements.txt      # Dependencies
└── README.md             # Project overview
```

## Extension Points

The framework is designed for extensibility at multiple levels:

### Primary Extension Points

1. **Custom Extractors**: Create specialized components for extracting data from new sources
2. **Custom Transformers**: Implement custom data transformation logic
3. **Custom Loaders**: Build components for loading data to new destinations
4. **Pipeline Customization**: Extend the pipeline orchestration for complex workflows

### Extension Methods

1. **Inheritance**: Extend base classes to create specialized components
2. **Composition**: Combine existing components to create complex workflows
3. **Configuration**: Customize behavior through configuration parameters

## Component Interactions

### ETL Pipeline Flow

1. **Initialization**:
   - Load configuration file
   - Validate configuration
   - Initialize logging
   - Create ETL pipeline instance

2. **Component Setup**:
   - Instantiate extractors, transformers, and loaders
   - Configure components based on configuration
   - Validate component setup

3. **Execution**:
   - Extract data from sources
   - Apply transformations sequentially
   - Load processed data to destinations

4. **Finalization**:
   - Generate summary statistics
   - Log job completion
   - Handle any cleanup tasks

### Data Flow

Data flows through the pipeline as a list of pandas DataFrames. This allows for flexibility in handling multiple datasets while maintaining efficient data manipulation capabilities.

## Class Hierarchies

### Extractor Classes

```
BaseExtractor (Abstract)
├── CSVExtractor
├── JSONExtractor
└── SQLExtractor
```

### Transformer Classes

```
BaseTransformer (Abstract)
├── CleaningTransformer
├── NormalizationTransformer
└── ValidationTransformer
```

### Loader Classes

```
BaseLoader (Abstract)
├── CSVLoader
├── JSONLoader
└── SQLLoader
```

## Creating Custom Components

### Creating a Custom Extractor

1. **Create a new Python file** in the `src/extractors` directory.
2. **Import the base class**:
   ```python
   from src.extractors.base_extractor import BaseExtractor
   ```
3. **Create your extractor class** by extending the base class:
   ```python
   class MyCustomExtractor(BaseExtractor):
       def __init__(self, config):
           super().__init__(config)
           # Initialize your extractor-specific attributes
           self.source_config = config.get("source_config", {})
           self.custom_param = config.get("custom_param", "default_value")
           
       def validate_source(self) -> bool:
           # Implement source validation logic
           # Return True if source is valid, False otherwise
           
       def extract(self):
           # Implement extraction logic
           # Return extracted data as a pandas DataFrame
   ```
4. **Register your extractor** in the configuration file:
   ```yaml
   extractors:
     my_custom_extractor:
       type: MyCustomExtractor
       class: src.extractors.my_custom_extractor.MyCustomExtractor
       config:
         source_config:
           # Source-specific configuration
         custom_param: "custom_value"
   ```

### Creating a Custom Transformer

1. **Create a new Python file** in the `src/transformers` directory.
2. **Import the base class**:
   ```python
   from src.transformers.base_transformer import BaseTransformer
   ```
3. **Create your transformer class** by extending the base class:
   ```python
   class MyCustomTransformer(BaseTransformer):
       def __init__(self, config):
           super().__init__(config)
           # Initialize your transformer-specific attributes
           
       def transform(self, data):
           # Implement transformation logic
           # Data will be a pandas DataFrame or list of DataFrames
           # Return transformed data
   ```
4. **Register your transformer** in the configuration file:
   ```yaml
   transformers:
     my_custom_transformer:
       type: MyCustomTransformer
       class: src.transformers.my_custom_transformer.MyCustomTransformer
       config:
         # Transformer-specific configuration
   ```

### Creating a Custom Loader

1. **Create a new Python file** in the `src/loaders` directory.
2. **Import the base class**:
   ```python
   from src.loaders.base_loader import BaseLoader
   ```
3. **Create your loader class** by extending the base class:
   ```python
   class MyCustomLoader(BaseLoader):
       def __init__(self, config):
           super().__init__(config)
           # Initialize your loader-specific attributes
           
       def validate_destination(self) -> bool:
           # Implement destination validation logic
           # Return True if destination is valid, False otherwise
           
       def load(self, data):
           # Implement loading logic
           # Data will be a pandas DataFrame or list of DataFrames
           # Return True if loading was successful, False otherwise
   ```
4. **Register your loader** in the configuration file:
   ```yaml
   loaders:
     my_custom_loader:
       type: MyCustomLoader
       class: src.loaders.my_custom_loader.MyCustomLoader
       config:
         # Loader-specific configuration
   ```

## API ETL Components

The API ETL components have been enhanced with robust features for handling REST API data:

#### APIExtractor Features

The `APIExtractor` provides these key capabilities:

- **Multiple Authentication Methods**:
  - Basic Auth (username/password)
  - API Key (in header or query parameter)
  - Bearer Token
  - OAuth2 (client credentials flow)

- **Pagination Strategies**:
  - Offset-based (page numbers)
  - Cursor-based (next page token)
  - Link header-based (RFC 5988)

- **Rate Limiting & Error Handling**:
  - Configurable requests per minute/day
  - Exponential backoff for failed requests
  - Automatic retries for transient errors (429, 5xx)

- **Advanced Request Configuration**:
  - Custom headers and query parameters
  - Environment variable resolution
  - Request body customization for POST/PUT/PATCH

Here's an example of configuring the APIExtractor with advanced settings:

```yaml
extractors:
  - name: github_repos
    type: api
    config:
      base_url: "https://api.github.com"
      endpoint: "repos/organization/repository/issues"
      method: "GET"
      auth:
        type: "bearer"
        token: "${GITHUB_TOKEN}"
      rate_limit:
        requests_per_minute: 30
        requests_per_day: 5000
      retry:
        max_retries: 5
        delay_seconds: 2
        backoff_factor: 2
      pagination:
        enabled: true
        type: "offset"
        params:
          page_param: "page"
          per_page_param: "per_page"
          per_page: 100
        max_pages: 10
```

#### JSON Transformation Components

The JSON-related transformers have been enhanced with these capabilities:

1. **FlatteningTransformer**:
   - Configurable flattening of deeply nested structures
   - Multiple array handling strategies:
     - `first`: Extract only the first element
     - `join`: Join array elements with a delimiter
     - `expand`: Create separate columns for array elements
   - Configurable maximum depth with proper serialization of deeply nested objects

2. **JSONTransformer**:
   - Type casting with support for:
     - Integer/float conversion
     - Boolean conversion with smart string handling ("yes"/"no"/"true"/"false")
     - Date and datetime parsing with format specification
   - Field selection, renaming, and dropping
   - Calculated fields with expression evaluation
   - JSONPath extraction for complex data

These components work together to provide a comprehensive solution for processing API data with features comparable to commercial ETL tools.

## Configuration System

### Configuration File Structure

The framework uses YAML configuration files with the following structure:

```yaml
job:
  name: job_name
  description: "Job description"

extractors:
  extractor_name:
    type: ExtractorType
    class: full.path.to.ExtractorClass
    config:
      # Extractor-specific configuration

transformers:
  transformer_name:
    type: TransformerType
    class: full.path.to.TransformerClass
    config:
      # Transformer-specific configuration

loaders:
  loader_name:
    type: LoaderType
    class: full.path.to.LoaderClass
    config:
      # Loader-specific configuration
```

### Configuration Validation

The ConfigManager class in `src/utils/config_manager.py` handles loading and validating configuration files. To extend the validation logic:

1. **Modify the `validate_config` method** to check additional configuration parameters.
2. **Add specialized validation methods** for specific component types.
3. **Implement custom validation logic** in component classes through their initialization.

### Dynamic Component Loading

The framework uses dynamic importing to instantiate components based on class paths in the configuration:

```python
def _load_class(class_path):
    """Load a class given its full path."""
    module_path, class_name = class_path.rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)

def _create_instance(class_path, config):
    """Create an instance of a class with the given configuration."""
    cls = _load_class(class_path)
    return cls(config)
```

## Logging and Error Handling

### Logging System

The logging system is centralized in `src/utils/logging_utils.py` and provides:

- Consistent log formatting
- Context tracking (job ID, component)
- Multiple output destinations
- Configurable log levels

#### Using the Logger in Custom Components

```python
from src.utils.logging_utils import get_etl_logger

class MyCustomComponent:
    def __init__(self, config):
        self.logger = get_etl_logger(__name__, component="MyCustomComponent")
        self.logger.info("Initializing custom component")
        
    def process(self):
        self.logger.debug("Processing data")
        try:
            # Processing logic
            self.logger.info("Processing completed successfully")
        except Exception as e:
            self.logger.error(f"Error during processing: {str(e)}")
            raise
```

### Error Handling Strategy

The framework uses a multi-level error handling approach:

1. **Component-level error handling**: Each component handles its internal errors
2. **Pipeline-level error handling**: The pipeline catches and manages component errors
3. **Job-level error handling**: The CLI handles job-level errors

To customize error handling:

1. **Add try-except blocks** in critical sections of your custom components
2. **Log errors** with appropriate severity levels
3. **Propagate critical errors** that should stop the pipeline
4. **Consider recovery strategies** for non-critical errors

## Testing

### Test Structure

The framework uses a standard Python testing approach with unit tests, integration tests, and end-to-end tests.

```
tests/
├── __init__.py
├── test_cli_options.py       # Command-line interface option tests
├── test_pipeline_options.py  # Pipeline configuration option tests
├── test_extractors/
│   ├── __init__.py
│   ├── test_csv_extractor.py
│   ├── test_json_extractor.py
│   └── test_sql_extractor.py
├── test_transformers/
│   ├── __init__.py
│   └── test_*.py
├── test_loaders/
│   ├── __init__.py
│   └── test_*.py
└── test_integration/
    ├── __init__.py
    └── test_full_pipeline.py
```

### CLI Tests

The `test_cli_options.py` file contains tests for the command-line interface options:

- Testing configuration validation
- Testing the `--validate-only` flag
- Testing the `--dry-run` option
- Testing custom job IDs
- Testing custom output directories

Example of a CLI test:

```python
@patch('src.cli.validate_config')
def test_main_validate_only(self, mock_validate_config):
    """Test main function with validate-only option."""
    mock_validate_config.return_value = True
    
    with patch('sys.argv', ['main.py', '--config', self.test_config, '--validate-only']):
        with patch('src.cli.logger'):  # Suppress logging
            # Execute main function
            main()
            # Verify validate_config was called
            mock_validate_config.assert_called_once_with(self.test_config)
```

### Pipeline Tests

The `test_pipeline_options.py` file contains tests for how the ETL pipeline handles command-line provided options:

- Testing output directory creation
- Testing custom job ID handling
- Testing pipeline setup and execution

### Running Tests

The framework includes a test runner script (`run_tests.py`) that discovers and executes all tests, providing a summary report.

To run all tests with the test runner:

```powershell
python run_tests.py
```

Sample output:
```
======================================================================
TEST SUMMARY
======================================================================
Tests executed: 15
Tests passed: 15
Tests failed: 0
Test errors: 0
Execution time: 0.75 seconds
======================================================================
```

To run a specific test file:

```powershell
python -m unittest tests.test_cli_options
```

To run a specific test method:

```powershell
python -m unittest tests.test_cli_options.TestCLIOptions.test_main_dry_run
```

### Testing Strategies

When writing tests for the framework, consider these strategies:

1. **Use mocking**: Use the `unittest.mock` library to isolate the component you're testing
2. **Test edge cases**: Cover error conditions and boundary values
3. **Test configuration variations**: Test different configuration options
4. **Patch external dependencies**: Use patch decorators to replace external dependencies with mocks
5. **Test the public API**: Focus on testing the public methods of your classes

### Component-based Testing

The ETL framework uses an enhanced test runner (`run_tests.py`) that supports component-based test discovery and execution. This allows developers to focus on specific components during development and testing.

#### Test Organization

Tests can be organized in two ways:
1. **File naming**: Tests organized in files like `test_*_component.py` 
2. **Class/method naming**: Tests with classes or methods containing component names

#### Running Component-specific Tests

The test runner can execute tests for specific components:

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
```

#### Test Discovery Process

The test runner uses a sophisticated discovery mechanism:

1. **Pattern Matching**: Tests are discovered based on component-specific patterns
2. **Class-Based Discovery**: Test classes that contain the component name are included
3. **Method-Based Discovery**: Test methods that reference the component are included

This flexibility allows for:
- Organizing tests by component functionality rather than by file structure
- Running focused test suites for faster feedback during development
- Simplifying maintenance of large test suites

#### Test Runner Implementation

The `run_tests.py` script implements dynamic test discovery through Python's unittest framework:

```python
def discover_component_tests(component_name):
    """
    Discover tests for the specified component using both file patterns and class/method names.
    
    Args:
        component_name: Name of the component to find tests for
        
    Returns:
        unittest.TestSuite: A test suite containing the discovered tests
    """
    # Component-specific patterns and mappings
    component_patterns = {
        'api': ['*api*', '*rest*', '*http*', '*web*'],
        'csv': ['*csv*', '*file*'],
        'db': ['*sql*', '*database*', '*db*'],
        'json': ['*json*'],
        # Add more patterns as needed
    }
    
    # Get patterns for the specified component
    patterns = component_patterns.get(component_name.lower(), [f'*{component_name.lower()}*'])
    
    # Discover tests based on file patterns
    test_suite = unittest.TestSuite()
    for pattern in patterns:
        file_suite = unittest.defaultTestLoader.discover('tests', pattern=f'test_{pattern}.py')
        test_suite.addTest(file_suite)
    
    # Also discover tests based on class and method names
    all_tests = unittest.defaultTestLoader.discover('tests', pattern='test_*.py')
    for test in all_tests:
        for test_case in test:
            if isinstance(test_case, unittest.TestSuite):
                for tc in test_case:
                    # Check if the component name appears in the test class name or method name
                    if component_name.lower() in tc.__class__.__name__.lower() or \
                       component_name.lower() in tc._testMethodName.lower():
                        test_suite.addTest(tc)
    
    return test_suite
```

#### Adding Test Coverage for New Components

When creating a new component, follow these steps to ensure proper test coverage:

1. **Create a dedicated test file**: For example, `test_new_component.py`
2. **Include component name in test classes/methods**: Use naming like `TestNewComponent` or `test_new_component_functionality`
3. **Run component-specific tests**: Use `python run_tests.py new_component` to verify your tests

#### Best Practices for Component Tests

1. **Isolate dependencies**: Use mock objects to isolate the component being tested
2. **Test component interfaces**: Focus on testing the public methods and expected outputs
3. **Include both happy path and error cases**: Test both normal operation and error handling
4. **Test configuration variations**: Verify the component works with different configuration options
5. **Use descriptive test names**: Make test names clearly describe what is being tested

### Testing Custom Components

For each custom component, create a test file in the appropriate directory:

```python
import unittest
import pandas as pd
from src.extractors.my_custom_extractor import MyCustomExtractor

class TestMyCustomExtractor(unittest.TestCase):
    def setUp(self):
        self.config = {
            "source_config": {
                "param1": "value1"
            },
            "custom_param": "test_value"
        }
        self.extractor = MyCustomExtractor(self.config)
    
    def test_initialization(self):
        self.assertEqual(self.extractor.custom_param, "test_value")
    
    def test_validate_source_valid(self):
        # Test with valid source
        self.assertTrue(self.extractor.validate_source())
    
    def test_validate_source_invalid(self):
        # Test with invalid source
        self.extractor.source_config["param1"] = "invalid_value"
        self.assertFalse(self.extractor.validate_source())
    
    def test_extract(self):
        # Test extraction logic
        result = self.extractor.extract()
        self.assertIsInstance(result, pd.DataFrame)
        self.assertGreater(len(result), 0)

if __name__ == '__main__':
    unittest.main()
```

## Best Practices

### Code Style

- Follow PEP 8 guidelines for Python code style
- Use type hints for function arguments and return values
- Write comprehensive docstrings
- Keep functions and methods focused on a single responsibility

### Component Design

- Make components configurable through their constructor
- Validate configuration early
- Use descriptive error messages
- Log all significant events
- Provide sensible defaults for optional parameters
- Use abstract base classes for common interfaces
- Separate business logic from I/O operations

### Error Handling

- Be specific about exceptions you catch
- Provide context in error messages
- Log errors with appropriate severity
- Clean up resources in finally blocks
- Consider recovery strategies when appropriate

### Performance

- Process data in chunks for large datasets
- Use vectorized operations in pandas
- Profile your code to identify bottlenecks
- Minimize unnecessary data copying
- Consider memory usage for large transformations

## Advanced Examples

### Custom CSV Extractor with File Watching

This example shows how to create a custom CSV extractor that watches for file changes and extracts new data incrementally.

```python
import os
import time
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from src.extractors.base_extractor import BaseExtractor
from src.utils.logging_utils import get_etl_logger

class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, callback):
        self.callback = callback
    
    def on_modified(self, event):
        if not event.is_directory:
            self.callback(event.src_path)

class WatchingCSVExtractor(BaseExtractor):
    def __init__(self, config):
        super().__init__(config)
        self.logger = get_etl_logger(__name__, component="WatchingCSVExtractor")
        
        self.file_path = config.get("file_path")
        self.delimiter = config.get("delimiter", ",")
        self.encoding = config.get("encoding", "utf-8")
        self.watch_interval = config.get("watch_interval", 5)  # seconds
        self.last_position = 0
        self.observer = None
        self.event_handler = None
        
    def _on_file_change(self, file_path):
        self.logger.info(f"Detected change in {file_path}")
        # Get new data since last position
        df = self._extract_incremental()
        if not df.empty:
            self.logger.info(f"Extracted {len(df)} new rows")
            # Process the new data - you might want to trigger a callback here
    
    def _extract_incremental(self):
        """Extract only new data since last position."""
        file_size = os.path.getsize(self.file_path)
        if file_size > self.last_position:
            # Open file and seek to last position
            with open(self.file_path, 'r', encoding=self.encoding) as f:
                f.seek(self.last_position)
                # Read only new lines
                new_data = f.read()
                
            # Parse new CSV data
            import io
            df = pd.read_csv(io.StringIO(new_data), delimiter=self.delimiter)
            
            # Update last position
            self.last_position = file_size
            
            return df
        return pd.DataFrame()
    
    def start_watching(self):
        """Start watching for file changes."""
        self.event_handler = FileChangeHandler(self._on_file_change)
        self.observer = Observer()
        self.observer.schedule(self.event_handler, os.path.dirname(self.file_path), recursive=False)
        self.observer.start()
        self.logger.info(f"Started watching {self.file_path} for changes")
    
    def stop_watching(self):
        """Stop watching for file changes."""
        if self.observer:
            self.observer.stop()
            self.observer.join()
            self.logger.info("Stopped file watching")
    
    def validate_source(self) -> bool:
        """Validate that the CSV file source is accessible."""
        if not self.file_path:
            self.logger.error("File path not provided in configuration")
            return False
        
        if not os.path.exists(self.file_path):
            self.logger.error(f"File does not exist: {self.file_path}")
            return False
        
        if not os.access(self.file_path, os.R_OK):
            self.logger.error(f"No read permission for file: {self.file_path}")
            return False
        
        return True
    
    def extract(self):
        """Extract data from the CSV file."""
        if not self.validate_source():
            raise ValueError(f"Invalid source: {self.file_path}")
        
        try:
            df = pd.read_csv(self.file_path, delimiter=self.delimiter, encoding=self.encoding)
            # Update last position to end of file
            self.last_position = os.path.getsize(self.file_path)
            self.logger.info(f"Successfully extracted {len(df)} rows from {self.file_path}")
            return df
        except Exception as e:
            self.logger.error(f"Error extracting data from CSV file: {str(e)}")
            raise
```

### Advanced Transformation Pipeline

This example demonstrates how to create a custom transformer that orchestrates a sequence of sub-transformations:

```python
from typing import Dict, Any, List, Union
import pandas as pd
from src.transformers.base_transformer import BaseTransformer
from src.utils.logging_utils import get_etl_logger

class PipelineTransformer(BaseTransformer):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.logger = get_etl_logger(__name__, component="PipelineTransformer")
        
        # Load sub-transformer configurations
        self.sub_transformers_config = config.get("transformers", [])
        
        # Initialize sub-transformers
        self.sub_transformers = []
        for transformer_config in self.sub_transformers_config:
            transformer_type = transformer_config.get("type")
            transformer_class = transformer_config.get("class")
            transformer_config = transformer_config.get("config", {})
            
            # Import the transformer class dynamically
            import importlib
            module_path, class_name = transformer_class.rsplit('.', 1)
            module = importlib.import_module(module_path)
            transformer_cls = getattr(module, class_name)
            
            # Instantiate the transformer
            transformer = transformer_cls(transformer_config)
            self.sub_transformers.append(transformer)
            
        self.logger.info(f"Initialized pipeline transformer with {len(self.sub_transformers)} sub-transformers")
    
    def transform(self, data: Union[pd.DataFrame, List[pd.DataFrame]]) -> Union[pd.DataFrame, List[pd.DataFrame]]:
        """
        Apply all sub-transformers in sequence.
        
        Args:
            data: Input data
            
        Returns:
            Transformed data
        """
        self.logger.info("Starting transformation pipeline")
        
        # Apply each transformer in sequence
        current_data = data
        for i, transformer in enumerate(self.sub_transformers):
            self.logger.debug(f"Applying transformer {i+1}/{len(self.sub_transformers)}: {transformer.__class__.__name__}")
            try:
                current_data = transformer.transform(current_data)
                self.logger.debug(f"Transformer {transformer.__class__.__name__} completed successfully")
            except Exception as e:
                self.logger.error(f"Error in transformer {transformer.__class__.__name__}: {str(e)}")
                raise
        
        self.logger.info("Transformation pipeline completed successfully")
        return current_data
```

### Custom SQL Loader with Schema Evolution

This example shows a SQL loader that automatically handles schema changes:

```python
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, Column, String, Float, Integer, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from src.loaders.base_loader import BaseLoader
from src.utils.logging_utils import get_etl_logger

class SchemaEvolvingLoader(BaseLoader):
    def __init__(self, config):
        super().__init__(config)
        self.logger = get_etl_logger(__name__, component="SchemaEvolvingLoader")
        
        self.connection_string = config.get("connection_string")
        self.table_name = config.get("table_name")
        self.schema = config.get("schema")
        
        # Schema evolution options
        self.auto_evolve = config.get("auto_evolve", True)
        self.type_map = {
            'object': String(255),
            'int64': Integer,
            'float64': Float,
            'bool': Integer,
            'datetime64[ns]': String(30)
        }
        
        # Initialize engine and session
        self.engine = create_engine(self.connection_string)
        self.metadata = MetaData()
        self.Session = sessionmaker(bind=self.engine)
        
    def _get_table_schema(self):
        """Get the current schema of the database table."""
        inspector = inspect(self.engine)
        columns = {}
        
        if inspector.has_table(self.table_name, schema=self.schema):
            for column in inspector.get_columns(self.table_name, schema=self.schema):
                columns[column['name']] = column['type']
        
        return columns
    
    def _evolve_schema(self, df):
        """Evolve the database table schema based on DataFrame."""
        current_columns = self._get_table_schema()
        df_dtypes = df.dtypes.to_dict()
        
        # Compare current schema with DataFrame
        if not current_columns:
            # Table doesn't exist, create it
            table = Table(self.table_name, self.metadata)
            for column_name, dtype in df_dtypes.items():
                sql_type = self.type_map.get(str(dtype), String(255))
                table.append_column(Column(column_name, sql_type))
            
            self.logger.info(f"Creating new table {self.table_name} with {len(df_dtypes)} columns")
            self.metadata.create_all(self.engine)
        else:
            # Table exists, add any missing columns
            added_columns = 0
            for column_name, dtype in df_dtypes.items():
                if column_name not in current_columns:
                    sql_type = self.type_map.get(str(dtype), String(255))
                    self.engine.execute(
                        f'ALTER TABLE {self.table_name} ADD COLUMN "{column_name}" {sql_type}'
                    )
                    added_columns += 1
            
            if added_columns > 0:
                self.logger.info(f"Added {added_columns} new columns to table {self.table_name}")
    
    def load(self, data):
        """Load data to the database with automatic schema evolution."""
        if isinstance(data, list):
            # Combine multiple DataFrames
            if not data:
                self.logger.warning("Empty data list provided, nothing to load")
                return True
            
            df = pd.concat(data, ignore_index=True)
        else:
            df = data
        
        if df.empty:
            self.logger.warning("Empty DataFrame, nothing to load")
            return True
        
        # Evolve schema if needed
        if self.auto_evolve:
            self._evolve_schema(df)
        
        # Load data to table
        try:
            df.to_sql(
                name=self.table_name,
                con=self.engine,
                schema=self.schema,
                if_exists='append',
                index=False,
                chunksize=1000
            )
            
            self.logger.info(f"Successfully loaded {len(df)} rows to table {self.table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Error loading data to table {self.table_name}: {str(e)}")
            return False
```

## Command-Line Interface

The ETL framework provides a command-line interface (CLI) for running ETL jobs. Understanding the CLI architecture is important for developers extending the framework.

### CLI Architecture

The CLI functionality is implemented in `src/cli.py` and provides a user-friendly interface to:

1. Parse command-line arguments
2. Validate configuration files
3. Set up logging
4. Initialize and run the ETL pipeline

### Command-Line Options

The CLI supports the following options:

| Option | Description | Implementation |
|--------|-------------|----------------|
| `--config` | Path to the YAML configuration file (required) | Used to load the main configuration |
| `--log-level` | Logging level (DEBUG, INFO, WARNING, ERROR) | Passed to the logging configuration |
| `--log-file` | Path to the log file | Defaults to "etl.log" in the current directory |
| `--output-dir` | Directory for output files | Passed to the ETL pipeline and used for file-based loaders |
| `--dry-run` | Validate the config and set up the pipeline without running | Stops execution after pipeline setup |
| `--validate-only` | Only validate the configuration | Exits after validation |
| `--job-id` | Custom job ID | Passed to the ETL pipeline to override the auto-generated job ID |

### Adding New CLI Options

To add new command-line options:

1. Update the `parse_args()` function in `src/cli.py`
2. Add appropriate handler logic in the `main()` function
3. Modify the ETL pipeline as needed to accept the new options

Example of adding a new CLI option:

```python
# In parse_args() function
parser.add_argument(
    "--my-new-option",
    default="default_value",
    help="Description of the new option"
)

# In main() function
if args.my_new_option:
    # Handle the new option
    pipeline_options["my_option"] = args.my_new_option
```

### Best Practices for CLI Development

1. **Consistent Naming**: Use kebab-case for option names (e.g., `--output-dir`)
2. **Helpful Documentation**: Provide clear help text for each option
3. **Parameter Validation**: Validate parameters before passing them to the pipeline
4. **Error Handling**: Provide clear error messages for invalid parameters
5. **Defaults**: Use sensible defaults where appropriate

This completes the Developer's Guide. By following these guidelines and examples, you can extend and customize the Data Warehouse ETL Framework to meet your specific requirements.
