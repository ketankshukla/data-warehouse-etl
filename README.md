# Data Warehouse ETL Framework

A modular Extract-Transform-Load (ETL) framework designed for data warehouse operations. This framework provides a flexible and configurable approach to building ETL pipelines for various data sources and destinations.

## Features

- **Modular Architecture**: Easily extendable components for extraction, transformation, and loading
- **Multiple Data Source Support**: Extract data from CSV, JSON, XML, SQL, and more
- **Comprehensive Transformations**: Clean, normalize, and validate data
- **Flexible Loading Options**: Load data to databases, files, or other destinations
- **Configuration-Based**: Define ETL pipelines using YAML configuration files
- **Robust Logging**: Detailed logging for monitoring and debugging
- **Error Handling**: Graceful error handling and reporting
- **CLI Interface**: Command-line interface for running ETL jobs
- **Complex Data Type Support**: Automatic JSON serialization for complex data types in SQL databases
- **Comprehensive Documentation**: Detailed user and developer guides

## Project Structure

```
data_warehouse_etl/
├── config/               # Configuration files
│   └── sample_etl_config.yaml
├── src/                  # Source code
│   ├── extractors/       # Data extraction components
│   │   ├── base_extractor.py
│   │   ├── csv_extractor.py
│   │   ├── json_extractor.py
│   │   └── sql_extractor.py
│   ├── transformers/     # Data transformation components
│   │   ├── base_transformer.py
│   │   ├── cleaning_transformer.py
│   │   ├── normalization_transformer.py
│   │   └── validation_transformer.py
│   ├── loaders/          # Data loading components
│   │   ├── base_loader.py
│   │   ├── csv_loader.py
│   │   ├── json_loader.py
│   │   └── sql_loader.py
│   ├── utils/            # Utility functions
│   │   ├── config_manager.py
│   │   └── logging_utils.py
│   ├── __init__.py
│   ├── cli.py            # Command-line interface
│   └── pipeline.py       # ETL pipeline orchestrator
├── tests/                # Unit tests
│   ├── test_cli_options.py
│   └── test_pipeline_options.py
├── docs/                 # Documentation
├── .venv/                # Virtual environment (not tracked by git)
├── main.py               # Main entry point
├── requirements.txt      # Project dependencies
└── README.md             # Project documentation
```

## Installation

1. Clone the repository
```powershell
git clone https://github.com/yourusername/data_warehouse_etl.git
cd data_warehouse_etl
```

2. Create and activate a virtual environment:
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate
   ```

3. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```

## Quick Start

1. Activate the virtual environment (if not already activated)
```powershell
.\.venv\Scripts\Activate
```

2. Run a sample ETL job using the provided configuration
```powershell
python main.py --config config\sample_etl_config.yaml
```

## Running Tests

The framework includes a comprehensive test suite to verify functionality:

```powershell
# Run all tests with the test runner (recommended)
python run_tests.py
```

The test runner provides component-based test discovery and execution:

```powershell
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

The test runner provides a summary report with execution statistics:

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

You can also run specific test files directly:

```powershell
# Run only CLI option tests
python -m unittest tests.test_cli_options

# Run only pipeline option tests
python -m unittest tests.test_pipeline_options

# Run a specific test method
python -m unittest tests.test_cli_options.TestCLIOptions.test_main_dry_run
```

### What's Tested

The test suite covers:

1. **Command-Line Interface Options**:
   - Configuration validation
   - Validate-only mode
   - Dry-run mode
   - Custom job IDs
   - Custom output directories

2. **Pipeline Options**:
   - Output directory handling
   - Job ID processing
   - Pipeline setup and execution

3. **Extractor Components**:
   - Database (SQL) extraction
   - CSV file extraction
   - API data extraction
   - JSON file extraction

4. **Transformer Components**:
   - Data cleaning
   - Normalization
   - Validation
   - Type conversion

5. **Loader Components**:
   - Database loading
   - File output loading

For more details on testing, refer to the Testing section in the DEVELOPERS_GUIDE.md.

## Usage

### Running an ETL Job

The framework can be used to run ETL jobs defined in YAML configuration files:

```powershell
python main.py --config config\sample_etl_config.yaml
```

### Additional Options

- `--log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}`: Set the log level (default: INFO)
- `--output-dir OUTPUT_DIR`: Specify the output directory for generated files
- `--dry-run`: Validate and prepare the ETL pipeline without executing it
- `--validate-only`: Only validate the configuration file without running the pipeline
- `--job-id JOB_ID`: Specify a custom job ID for the ETL run

### Example Commands

```powershell
# Run with detailed logging
python main.py --config config\sample_etl_config.yaml --log-level DEBUG

# Validate configuration only
python main.py --config config\sample_etl_config.yaml --validate-only

# Run with custom output directory
python main.py --config config\sample_etl_config.yaml --output-dir custom_output
```

### Configuration Files

ETL jobs are defined using YAML configuration files. Each configuration file defines:

1. **Extractors**: Where to extract data from
2. **Transformers**: How to transform the data (optional)
3. **Loaders**: Where to load the data to
4. **Pipeline**: General pipeline settings

See `config/sample_etl_config.yaml` for a complete example.

## Documentation

The framework comes with comprehensive documentation in the `docs` folder:

- [**User Guide**](docs/USER_GUIDE.md): Detailed guide for users configuring and running ETL jobs
- [**Developers Guide**](docs/DEVELOPERS_GUIDE.md): Technical documentation for developers extending the framework
- [**Framework Guide**](docs/framework_guide.md): Overview of the framework architecture and components

The documentation covers:
- Configuration examples
- Command-line usage
- Best practices
- Troubleshooting tips
- Extension tutorials
- Architecture overview
- Testing strategies

## Extending the Framework

### Adding New Extractors

1. Create a new class that inherits from `BaseExtractor`
2. Implement the required `extract()` and `validate_source()` methods
3. Place the file in the `src/extractors` directory

### Adding New Transformers

1. Create a new class that inherits from `BaseTransformer`
2. Implement the required `transform()` method
3. Place the file in the `src/transformers` directory

### Adding New Loaders

1. Create a new class that inherits from `BaseLoader`
2. Implement the required `load()` and `validate_destination()` methods
3. Place the file in the `src/loaders` directory

## Recent Updates

- Added JSON serialization for complex data types in SQL loaders
- Enhanced normalization transformer with multiple normalization methods
- Unified class naming conventions across the framework
- Created comprehensive user and developer documentation
- Fixed SQL binding parameter issues for complex data types

## License

This project is licensed under the MIT License.

## Dependencies

- pandas
- numpy
- sqlalchemy
- pyyaml
- scikit-learn
- requests
