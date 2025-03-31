# End-to-End ETL Process with CSV Files

This guide walks through the complete process of extracting data from a CSV file, transforming it, and loading it to a destination using the Data Warehouse ETL Framework.

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

Processing CSV files is one of the most common ETL scenarios. This guide demonstrates how data flows through each stage of the ETL pipeline when working with CSV files, from initial extraction to final loading.

The example we'll follow is processing a customer data CSV file, cleaning and transforming the data, then loading it to a SQL database.

## Prerequisites

Before starting, ensure you have:

1. The Data Warehouse ETL Framework installed
2. Python 3.8 or higher
3. Required dependencies installed (`pip install -r requirements.txt`)
4. A CSV file to process
5. Target destination access configured (e.g., database credentials if loading to a database)

## Configuration Setup

### Sample CSV File Structure

Our example uses a CSV file with customer data (`customers.csv`):

```csv
id,first_name,last_name,email,phone,signup_date,status
1,John,Doe,john.doe@example.com,555-123-4567,2025-01-15,active
2,Jane,Smith,jane.smith@example.com,555-987-6543,2025-01-20,active
3,Bob,Johnson,bob.j@example.com,,2025-01-22,inactive
4,Alice,Williams,alice.w@example.com,555-555-5555,2025-01-25,pending
```

### YAML Configuration

Create a YAML configuration file (`csv_etl_config.yaml`) that defines the ETL process:

```yaml
job:
  name: customer_data_processing
  description: "Process customer data from CSV to SQL database"

extractors:
  - name: customer_csv
    type: csv
    config:
      file_path: data/input/customers.csv
      delimiter: ","
      has_header: true
      encoding: utf-8
      # Optional: define column data types
      dtypes:
        id: int
        first_name: str
        last_name: str
        email: str
        phone: str
        signup_date: str
        status: str
      
transformers:
  - name: data_cleaning
    type: cleaning
    config:
      # Fill missing values
      fill_na:
        phone: "UNKNOWN"
      # String transformations
      string_transforms:
        email: "lower"
        first_name: "title"
        last_name: "title"
      # Data type conversions
      type_conversions:
        signup_date: "datetime"

  - name: data_validation
    type: validation
    config:
      rules:
        - field: "email"
          rule: "contains"
          value: "@"
          action: "flag"
        - field: "status"
          rule: "in"
          value: ["active", "inactive", "pending"]
          action: "reject"

loaders:
  - name: sql_database
    type: sql
    config:
      connection_string: "sqlite:///data/output/customer_database.db"
      table_name: "customers"
      if_exists: "replace"  # Options: fail, replace, append
      index: false
```

## Extraction Phase

When the ETL job runs, here's exactly what happens during the extraction phase:

1. **Initialization**:
   - The ETLPipeline loads the configuration file and identifies the `csv` extractor
   - A CSVExtractor instance is created with the specified configuration

2. **Configuration Validation**:
   - The extractor validates that the CSV file exists at the specified path
   - Configuration parameters are validated (delimiter, encoding, etc.)

3. **File Reading Process**:
   - The extractor opens the CSV file using the specified encoding
   - If `has_header` is true, the first row is used as column names
   - The CSV is read using pandas' `read_csv` function with the specified delimiter

4. **Data Type Handling**:
   - If `dtypes` is specified, the extractor applies these data types to columns
   - This prevents automatic type inference that might not align with requirements

5. **Result Preparation**:
   - The extracted data is loaded into a pandas DataFrame
   - The extractor adds metadata (source name, record count, timestamp)
   - The DataFrame is returned to the pipeline for the transformation phase

### CSV-Specific Considerations

- **Large File Handling**: For large CSV files, the extractor uses chunked reading to avoid memory issues
- **Error Handling**: Row-level errors during parsing are logged and optionally skipped
- **Encoding Detection**: If encoding issues occur, the framework attempts to detect the correct encoding

## Transformation Phase

Once the data is extracted, the transformation phase proceeds:

1. **Data Cleaning Transformer**:
   - Receives the DataFrame from the extraction phase
   - Processes the data according to configured cleaning operations:
     - Fill missing values in the `phone` column with "UNKNOWN"
     - Convert email addresses to lowercase
     - Capitalize first and last names (title case)
     - Convert `signup_date` strings to datetime objects

2. **Data Validation Transformer**:
   - Receives the DataFrame from the cleaning transformer
   - Applies validation rules:
     - Checks that email addresses contain "@"
     - Verifies status values are in the allowed list
   - For email validation, invalid records are flagged but kept
   - For status validation, invalid records are rejected (removed)
   - Creates a validation report with counts of valid/invalid records

3. **Transformation Pipeline Flow**:
   - Each transformer operates on the output of the previous transformer
   - The pipeline tracks data lineage and changes at each step
   - Performance metrics are recorded (records in/out, processing time)

## Loading Phase

After transformation, the loading phase begins:

1. **SQL Loader Initialization**:
   - The SQLLoader is initialized with the database connection parameters
   - Connection is established to the SQLite database

2. **Table Preparation**:
   - Based on the `if_exists` setting:
     - `replace`: Any existing table is dropped and recreated
     - `append`: The existing table is kept for appending data
     - `fail`: The job fails if the table already exists
   - Table schema is derived from the DataFrame structure

3. **Data Loading Process**:
   - Data is loaded in batches to optimize performance
   - Complex data types (lists, dictionaries) are automatically serialized to JSON
   - Primary key and index creation is handled if configured

4. **Post-Loading Operations**:
   - The loader commits the transaction (or rolls back if errors occur)
   - Connection resources are properly closed
   - Loading statistics are recorded (records loaded, time taken)

## Execution and Monitoring

During execution, the ETL process provides real-time monitoring:

1. **Command Line Execution**:
   ```powershell
   python main.py --config config/csv_etl_config.yaml --log-level INFO
   ```

2. **Logging Output**:
   The framework logs each step of the process:
   ```
   2025-03-24 08:30:15 [INFO] [pipeline:92] ETL job 'customer_data_processing' started
   2025-03-24 08:30:15 [INFO] [pipeline:103] Initialization phase complete
   2025-03-24 08:30:15 [INFO] [csv_extractor:47] Reading CSV file from data/input/customers.csv
   2025-03-24 08:30:15 [INFO] [csv_extractor:65] Read 4 records from CSV file
   2025-03-24 08:30:15 [INFO] [pipeline:142] Extraction phase complete, extracted 4 records
   2025-03-24 08:30:15 [INFO] [cleaning_transformer:83] Applying cleaning transformations...
   2025-03-24 08:30:15 [INFO] [cleaning_transformer:112] Filled 1 NA values in column 'phone'
   2025-03-24 08:30:15 [INFO] [validation_transformer:94] Validating data against 2 rules
   2025-03-24 08:30:15 [INFO] [validation_transformer:127] Validation complete: 4 valid records, 0 rejected
   2025-03-24 08:30:15 [INFO] [pipeline:178] Transformation phase complete
   2025-03-24 08:30:15 [INFO] [sql_loader:114] Connecting to database
   2025-03-24 08:30:15 [INFO] [sql_loader:143] Creating table 'customers'
   2025-03-24 08:30:15 [INFO] [sql_loader:165] Inserted 4 records into table 'customers'
   2025-03-24 08:30:16 [INFO] [pipeline:214] Loading phase complete
   2025-03-24 08:30:16 [INFO] [pipeline:228] ETL job completed successfully in 0.42 seconds
   ```

3. **Performance Monitoring**:
   - The framework collects metrics at each stage
   - Total records processed/rejected
   - Execution time for each phase
   - Memory usage statistics

4. **Results**:
   - The processed data is now available in the SQLite database
   - Any rejected records are logged for review
   - A job summary report is generated

## Troubleshooting

Common issues when processing CSV files and their solutions:

1. **File Not Found**:
   - Check the file path is correct and accessible
   - Use absolute paths or paths relative to the execution directory

2. **Encoding Issues**:
   - If you see garbled characters, specify the correct encoding
   - Common encodings: utf-8, latin-1, cp1252

3. **Delimiter Problems**:
   - Verify the delimiter is correctly specified (comma, tab, etc.)
   - For tab-delimited files, use `\t` as the delimiter

4. **Data Type Errors**:
   - If conversion errors occur, check the data types in the configuration
   - Consider using string types for initial load, then transform later

5. **Memory Issues with Large Files**:
   - Use the chunking configuration: `chunk_size: 10000`
   - This processes the CSV in batches of the specified size

## Advanced Configuration

For more complex CSV processing needs:

1. **Multi-File Processing**:
   ```yaml
   extractors:
     - name: csv_extractor
       type: csv
       config:
         file_pattern: "data/input/*.csv"
         combine_files: true
   ```

2. **Custom Date Parsing**:
   ```yaml
   transformers:
     - name: date_transformer
       type: cleaning
       config:
         date_formats:
           signup_date: "%Y-%m-%d"
   ```

3. **Conditional Transformations**:
   ```yaml
   transformers:
     - name: conditional_cleaner
       type: cleaning
       config:
         conditional_transforms:
           - field: "status"
             condition: "equals"
             value: "pending"
             action:
               set_field: "priority"
               value: "high"
   ```

4. **Column Renaming and Selection**:
   ```yaml
   transformers:
     - name: column_transformer
       type: cleaning
       config:
         rename_columns:
           first_name: "given_name"
           last_name: "family_name"
         select_columns:
           - "id"
           - "given_name"
           - "family_name"
           - "email"
   ```

This end-to-end guide covers the complete process for handling CSV files in the Data Warehouse ETL Framework, from initial configuration to final data loading.

## Technical Implementation Details

### Module Structure and Program Flow

The ETL Framework processes CSV files through several key modules and classes:

```
src/
├── extractors/
│   ├── base_extractor.py         # BaseExtractor abstract class
│   ├── csv_extractor.py          # CSVExtractor implementation
│   └── extractor_factory.py      # Factory for creating extractors
├── transformers/
│   ├── base_transformer.py       # BaseTransformer abstract class
│   ├── cleaning_transformer.py   # Data cleaning implementation
│   ├── validation_transformer.py # Data validation implementation
│   └── transformer_factory.py    # Factory for creating transformers
├── loaders/
│   ├── base_loader.py            # BaseLoader abstract class
│   ├── csv_loader.py             # CSVLoader implementation
│   ├── sql_loader.py             # SQLLoader implementation
│   └── loader_factory.py         # Factory for creating loaders
├── utils/
│   ├── config_manager.py         # Configuration handling
│   └── logging_utils.py          # Logging utilities
├── cli.py                        # Command-line interface
└── pipeline.py                   # ETLPipeline orchestrator
```

### Process Flow Diagram

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Configuration  │     │    Extraction   │     │ Transformation  │     │     Loading     │
│    Loading      │────►│     Phase       │────►│     Phase       │────►│     Phase       │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
       │                        │                       │                       │
       ▼                        ▼                       ▼                       ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ConfigManager.   │     │CSVExtractor.    │     │CleaningTransf.  │     │SQLLoader.       │
│load_config()    │     │extract()        │     │transform()      │     │load()           │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
                                │                       │                       │
                                ▼                       ▼                       ▼
                        ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
                        │pandas.read_csv()│     │ValidationTransf.│     │CSVLoader.       │
                        │                 │     │transform()      │     │load()           │
                        └─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Key Classes and Methods

#### 1. ETLPipeline Class (pipeline.py)

The central orchestrator for the ETL process:

```python
class ETLPipeline:
    def __init__(self, config_path, log_level='INFO', 
                 validate_only=False, dry_run=False,
                 job_id=None, output_dir='output'):
        """Initialize the ETL pipeline with configuration and options."""
        self.config_path = config_path
        self.log_level = log_level
        self.validate_only = validate_only
        self.dry_run = dry_run
        self.job_id = job_id or f"job_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:6]}"
        self.output_dir = output_dir
        self.logger = self._setup_logging()
        self.config_manager = ConfigManager(config_path)
        self.config = self.config_manager.load_config()
        
    def run(self):
        """Execute the ETL pipeline end-to-end."""
        self.logger.info(f"ETL job '{self.config['job']['name']}' started")
        
        # Setup phase
        self._initialize_components()
        
        if self.validate_only:
            self.logger.info("Configuration validated successfully")
            return True
            
        if self.dry_run:
            self.logger.info("Dry run completed, pipeline is ready for execution")
            return True
        
        # Extract phase
        extracted_data = self._run_extraction()
        
        # Transform phase
        transformed_data = self._run_transformation(extracted_data)
        
        # Load phase
        self._run_loading(transformed_data)
        
        self.logger.info(f"ETL job completed successfully in {self.execution_time:.2f} seconds")
        return True
```

#### 2. CSVExtractor Class (extractors/csv_extractor.py)

Responsible for extracting data from CSV files:

```python
class CSVExtractor(BaseExtractor):
    def __init__(self, config):
        """Initialize with extraction configuration."""
        super().__init__(config)
        self.file_path = config.get('file_path')
        self.delimiter = config.get('delimiter', ',')
        self.has_header = config.get('has_header', True)
        self.encoding = config.get('encoding', 'utf-8')
        self.dtypes = config.get('dtypes', {})
        self.chunk_size = config.get('chunk_size', None)
        
    def validate_source(self):
        """Validate the CSV source exists and is accessible."""
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")
        return True
        
    def extract(self):
        """Extract data from the CSV file into a pandas DataFrame."""
        self.logger.info(f"Reading CSV file from {self.file_path}")
        
        try:
            # Handle large files with chunking if specified
            if self.chunk_size:
                chunks = []
                for chunk in pd.read_csv(
                    self.file_path, 
                    sep=self.delimiter,
                    header=0 if self.has_header else None,
                    encoding=self.encoding,
                    dtype=self.dtypes,
                    chunksize=self.chunk_size
                ):
                    chunks.append(chunk)
                df = pd.concat(chunks)
            else:
                df = pd.read_csv(
                    self.file_path, 
                    sep=self.delimiter,
                    header=0 if self.has_header else None,
                    encoding=self.encoding,
                    dtype=self.dtypes
                )
                
            self.logger.info(f"Read {len(df)} records from CSV file")
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting data from CSV: {str(e)}")
            raise
```

#### 3. CleaningTransformer Class (transformers/cleaning_transformer.py)

Handles data cleaning operations:

```python
class CleaningTransformer(BaseTransformer):
    def __init__(self, config):
        """Initialize with transformation configuration."""
        super().__init__(config)
        self.fill_na = config.get('fill_na', {})
        self.string_transforms = config.get('string_transforms', {})
        self.type_conversions = config.get('type_conversions', {})
        
    def transform(self, df):
        """Apply cleaning transformations to the DataFrame."""
        self.logger.info("Applying cleaning transformations...")
        
        # Fill missing values
        for column, value in self.fill_na.items():
            if column in df.columns:
                missing_count = df[column].isna().sum()
                df[column].fillna(value, inplace=True)
                self.logger.info(f"Filled {missing_count} NA values in column '{column}'")
        
        # Apply string transformations
        for column, transform in self.string_transforms.items():
            if column in df.columns:
                if transform == 'lower':
                    df[column] = df[column].str.lower()
                elif transform == 'upper':
                    df[column] = df[column].str.upper()
                elif transform == 'title':
                    df[column] = df[column].str.title()
                elif transform == 'strip':
                    df[column] = df[column].str.strip()
                    
        # Convert data types
        for column, dtype in self.type_conversions.items():
            if column in df.columns:
                try:
                    if dtype == 'datetime':
                        df[column] = pd.to_datetime(df[column])
                    else:
                        df[column] = df[column].astype(dtype)
                except Exception as e:
                    self.logger.warning(f"Failed to convert column '{column}' to {dtype}: {str(e)}")
        
        return df
```

#### 4. SQLLoader Class (loaders/sql_loader.py)

Handles loading data to SQL databases:

```python
class SQLLoader(BaseLoader):
    def __init__(self, config):
        """Initialize with loader configuration."""
        super().__init__(config)
        self.connection_string = config.get('connection_string')
        self.table_name = config.get('table_name')
        self.if_exists = config.get('if_exists', 'fail')
        self.index = config.get('index', False)
        self.batch_size = config.get('batch_size', 1000)
        
    def validate_destination(self):
        """Validate database connection and permissions."""
        try:
            engine = sa.create_engine(self.connection_string)
            connection = engine.connect()
            connection.close()
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {str(e)}")
            raise
            
    def load(self, df):
        """Load the DataFrame to the SQL database."""
        self.logger.info(f"Connecting to database")
        
        try:
            engine = sa.create_engine(self.connection_string)
            
            # Handle complex data types by converting to JSON
            for col in df.select_dtypes(include=['object']).columns:
                # Check if column contains lists, dicts or other complex types
                if df[col].apply(lambda x: isinstance(x, (list, dict, tuple))).any():
                    df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict, tuple)) else x)
            
            self.logger.info(f"Creating table '{self.table_name}'")
            
            # For large datasets, use batching
            if len(df) > self.batch_size:
                batches = [df[i:i+self.batch_size] for i in range(0, len(df), self.batch_size)]
                for i, batch in enumerate(batches):
                    if i == 0:
                        # First batch creates or replaces the table
                        batch.to_sql(
                            self.table_name,
                            engine,
                            if_exists=self.if_exists,
                            index=self.index
                        )
                    else:
                        # Subsequent batches append
                        batch.to_sql(
                            self.table_name,
                            engine,
                            if_exists='append',
                            index=self.index
                        )
                    self.logger.debug(f"Loaded batch {i+1}/{len(batches)}")
            else:
                df.to_sql(
                    self.table_name,
                    engine,
                    if_exists=self.if_exists,
                    index=self.index
                )
            
            self.logger.info(f"Inserted {len(df)} records into table '{self.table_name}'")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading data to SQL database: {str(e)}")
            raise
```

### Component Interaction and Data Flow

The following sequence diagram shows how the components interact during a CSV ETL process:

```
┌───────────┐          ┌──────────────┐          ┌──────────────┐          ┌──────────────┐          ┌───────────┐
│  main.py  │          │  ETLPipeline │          │ CSVExtractor │          │ Transformers │          │  Loaders  │
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
      │                       │                         │ pandas.read_csv()       │                         │
      │                       │                         │◄──────────              │                         │
      │                       │                         │                         │                         │
      │                       │ DataFrame               │                         │                         │
      │                       │<────────────────────────│                         │                         │
      │                       │                         │                         │                         │
      │                       │ _run_transformation()   │                         │                         │
      │                       │────────────────────────────────────────────────>  │                         │
      │                       │                         │                         │                         │
      │                       │                         │                         │ transform()             │
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
│  main.py  │          │  ETLPipeline │          │ CSVExtractor │          │ Transformers │          │  Loaders  │
└───────────┘          └──────────────┘          └──────────────┘          └──────────────┘          └───────────┘
```

### Implementation Details for Each Phase

#### Configuration Phase

1. The `ConfigManager` class loads and validates the YAML configuration file:
   ```python
   def load_config(self):
       """Load configuration from YAML file."""
       try:
           with open(self.config_path, 'r') as f:
               config = yaml.safe_load(f)
           
           # Validate configuration structure
           self._validate_config_structure(config)
           
           # Resolve environment variables
           config = self._resolve_env_vars(config)
           
           return config
       except Exception as e:
           self.logger.error(f"Error loading configuration: {str(e)}")
           raise
   ```

2. The `ETLPipeline._initialize_components()` method creates the necessary components based on the configuration:
   ```python
   def _initialize_components(self):
       """Initialize extractors, transformers, and loaders."""
       # Initialize extractors
       self.extractors = []
       for extractor_config in self.config['extractors']:
           extractor_type = extractor_config.get('type')
           extractor = ExtractorFactory.create_extractor(
               extractor_type, extractor_config.get('config', {})
           )
           self.extractors.append(extractor)
       
       # Initialize transformers
       self.transformers = []
       for transformer_config in self.config.get('transformers', []):
           transformer_type = transformer_config.get('type')
           transformer = TransformerFactory.create_transformer(
               transformer_type, transformer_config.get('config', {})
           )
           self.transformers.append(transformer)
       
       # Initialize loaders
       self.loaders = []
       for loader_config in self.config['loaders']:
           loader_type = loader_config.get('type')
           loader = LoaderFactory.create_loader(
               loader_type, loader_config.get('config', {})
           )
           self.loaders.append(loader)
   ```

This detailed technical information should provide a clear understanding of how the CSV ETL process is implemented in the framework, from the module structure to the specific class implementations and their interactions.
