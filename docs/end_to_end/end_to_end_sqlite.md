# End-to-End ETL Process with SQLite Databases

This guide walks through the complete process of extracting data from a SQLite database, transforming it, and loading it to a destination using the Data Warehouse ETL Framework.

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

SQLite is a popular file-based database engine often used for embedded applications and prototyping. This guide demonstrates how to extract data from a SQLite database, transform it, and load it to another destination.

The example we'll follow involves extracting order data from a SQLite database, joining it with product information, performing calculations, and loading the results to both a CSV file and another database table.

## Prerequisites

Before starting, ensure you have:

1. The Data Warehouse ETL Framework installed
2. Python 3.8 or higher
3. Required dependencies installed (`pip install -r requirements.txt`)
4. A SQLite database file with source data
5. Target destination access configured (if different from source)

## Configuration Setup

### Sample SQLite Database Schema

Our example uses a SQLite database (`retail.db`) with the following schema:

**orders table:**
```sql
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    order_date TEXT,
    total_amount REAL,
    status TEXT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
```

**order_items table:**
```sql
CREATE TABLE order_items (
    order_item_id INTEGER PRIMARY KEY,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price REAL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);
```

**products table:**
```sql
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    unit_cost REAL,
    retail_price REAL
);
```

### YAML Configuration

Create a YAML configuration file (`sqlite_etl_config.yaml`) that defines the ETL process:

```yaml
job:
  name: order_analysis_etl
  description: "Process order data from SQLite database, calculate metrics, and export results"

extractors:
  - name: orders_extractor
    type: sql
    config:
      connection_string: "sqlite:///data/source/retail.db"
      query: |
        SELECT o.order_id, o.customer_id, o.order_date, o.status,
               oi.order_item_id, oi.product_id, oi.quantity, oi.unit_price,
               p.product_name, p.category
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN products p ON oi.product_id = p.product_id
        WHERE o.order_date >= '2025-01-01'
      
transformers:
  - name: date_transformer
    type: cleaning
    config:
      # Convert string dates to datetime objects
      type_conversions:
        order_date: "datetime"
      
  - name: metrics_calculator
    type: custom
    config:
      # Calculate additional columns
      calculated_fields:
        - name: "item_total"
          expression: "quantity * unit_price"
        - name: "profit_margin"
          expression: "(unit_price - products.unit_cost) / unit_price"
        - name: "days_since_order"
          expression: "(CURRENT_DATE - order_date).days"
      
  - name: aggregator
    type: aggregation
    config:
      # Create summary by product category
      group_by: 
        - "category"
      aggregations:
        - column: "quantity"
          function: "sum"
          new_column: "total_quantity"
        - column: "item_total"
          function: "sum"
          new_column: "total_sales"
      output_dataframe: "category_summary"
      preserve_original: true

loaders:
  - name: order_details_csv
    type: csv
    config:
      file_path: "data/output/order_details.csv"
      delimiter: ","
      include_header: true
      encoding: "utf-8"
      
  - name: category_summary_db
    type: sql
    config:
      connection_string: "sqlite:///data/output/analysis_results.db"
      table_name: "category_sales_summary"
      if_exists: "replace"
      index: false
      source_dataframe: "category_summary"
```

## Extraction Phase

When the ETL job runs, here's exactly what happens during the extraction phase:

1. **Initialization**:
   - The ETLPipeline loads the configuration file and identifies the `sql` extractor
   - An SQLExtractor instance is created with the specified configuration

2. **Configuration Validation**:
   - The extractor validates the connection string format
   - Confirms that the SQL query is provided and syntactically valid
   - Checks for required permissions on the database file

3. **Database Connection**:
   - The extractor establishes a connection to the SQLite database
   - Uses SQLAlchemy as the database interface layer
   - Connection parameters like timeout and isolation level are applied

4. **Query Execution**:
   - The SQL query is executed against the database
   - The query includes JOINs to bring together data from multiple tables
   - For large result sets, the query uses cursor-based fetching to manage memory

5. **Result Processing**:
   - The query results are loaded into a pandas DataFrame
   - Column names and data types are preserved from the database query
   - The extractor adds metadata (source name, query execution time, record count)
   - The DataFrame is returned to the pipeline for the transformation phase

### SQLite-Specific Considerations

- **Query Optimization**: For large tables, the framework adds index hints if available
- **Date Handling**: SQLite stores dates as text, requiring transformation after extraction
- **Connection Management**: SQLite connections are properly closed after extraction
- **Error Handling**: Database errors include specifics about the query for debugging

## Transformation Phase

Once the data is extracted, the transformation phase proceeds:

1. **Date Transformer**:
   - Receives the DataFrame from the extraction phase
   - Converts the string date in `order_date` to Python datetime objects
   - This enables date-based calculations and filtering

2. **Metrics Calculator Transformer**:
   - Calculates additional columns based on the existing data:
     - `item_total`: Quantity multiplied by unit price
     - `profit_margin`: Calculated profit percentage for each item
     - `days_since_order`: Time elapsed since the order was placed

3. **Aggregator Transformer**:
   - Groups the data by product category
   - Computes aggregations like sum of quantity and total sales
   - Creates a new DataFrame named `category_summary` with these aggregated results
   - Preserves the original detailed DataFrame for the detailed export

4. **Transformation Pipeline Flow**:
   - Each transformer operates sequentially on its input
   - The pipeline now contains multiple DataFrames (original detailed data and category summary)
   - All transformations maintain data lineage for tracking changes

## Loading Phase

After transformation, the loading phase begins:

1. **CSV Loader for Detailed Data**:
   - The CSVLoader writes the detailed order data to a CSV file
   - All columns from the transformation phase are included
   - Headers, delimiter, and encoding are applied as configured

2. **SQL Loader for Summary Data**:
   - The SQLLoader targets the `category_summary` DataFrame
   - Establishes a connection to the output SQLite database
   - Creates (or replaces) the `category_sales_summary` table
   - Loads the aggregated data into the table

3. **Loading Process Details**:
   - For the database load, a transaction ensures data consistency
   - Bulk insert operations optimize performance
   - The loader verifies the data was loaded correctly

4. **Loading Completion**:
   - Both loaders report their completion status
   - Record counts and timing information are logged
   - Resources like file handles and database connections are properly closed

## Execution and Monitoring

During execution, the ETL process provides real-time monitoring:

1. **Command Line Execution**:
   ```powershell
   python main.py --config config/sqlite_etl_config.yaml --log-level INFO
   ```

2. **Logging Output**:
   The framework logs each step of the process:
   ```
   2025-03-24 10:12:33 [INFO] [pipeline:92] ETL job 'order_analysis_etl' started
   2025-03-24 10:12:33 [INFO] [pipeline:103] Initialization phase complete
   2025-03-24 10:12:33 [INFO] [sql_extractor:61] Connecting to SQLite database at data/source/retail.db
   2025-03-24 10:12:33 [INFO] [sql_extractor:83] Executing query against database
   2025-03-24 10:12:34 [INFO] [sql_extractor:112] Retrieved 1245 records from database query
   2025-03-24 10:12:34 [INFO] [pipeline:142] Extraction phase complete, extracted 1245 records
   2025-03-24 10:12:34 [INFO] [date_transformer:65] Converting date columns to datetime objects
   2025-03-24 10:12:34 [INFO] [metrics_calculator:77] Calculating derived metrics fields
   2025-03-24 10:12:34 [INFO] [aggregator:93] Performing aggregation by category
   2025-03-24 10:12:34 [INFO] [aggregator:114] Created aggregated dataframe 'category_summary' with 12 records
   2025-03-24 10:12:34 [INFO] [pipeline:178] Transformation phase complete
   2025-03-24 10:12:34 [INFO] [csv_loader:87] Writing data to CSV file data/output/order_details.csv
   2025-03-24 10:12:34 [INFO] [csv_loader:102] Wrote 1245 records to CSV file
   2025-03-24 10:12:34 [INFO] [sql_loader:114] Connecting to database
   2025-03-24 10:12:34 [INFO] [sql_loader:143] Creating table 'category_sales_summary'
   2025-03-24 10:12:34 [INFO] [sql_loader:165] Inserted 12 records into table 'category_sales_summary'
   2025-03-24 10:12:34 [INFO] [pipeline:214] Loading phase complete
   2025-03-24 10:12:34 [INFO] [pipeline:228] ETL job completed successfully in 1.24 seconds
   ```

3. **Performance Monitoring**:
   - The framework collects database-specific metrics:
     - Query execution time
     - Connection establishment time
     - Records processed per second
   - Memory usage for large query results
   - CPU utilization during transformation

4. **Results**:
   - The detailed data is available in the CSV file
   - The aggregated summary is stored in the output database
   - A job summary report details the processing statistics

## Troubleshooting

Common issues when working with SQLite databases and their solutions:

1. **Database Locked Error**:
   - This occurs when another process has locked the database
   - Solution: Configure with `timeout` parameter to wait for locks to be released
   - Alternative: Use `pragma busy_timeout` in the connection

2. **Query Performance Issues**:
   - For slow queries, check if indexes exist on joined columns
   - Use `explain query plan` to analyze query execution
   - Consider extracting only needed columns to reduce data volume

3. **Memory Issues with Large Results**:
   - Use chunked extraction: `chunk_size: 10000`
   - Apply filters in the SQL query, not in transformations
   - Limit date ranges to process data in batches

4. **Data Type Conversion Errors**:
   - SQLite has flexible typing, so specify explicit casts in queries
   - For date/time fields, check the format in the database
   - Use appropriate transformers to convert after extraction

5. **Foreign Key Constraint Errors**:
   - Ensure foreign key constraints are enabled with `PRAGMA foreign_keys = ON`
   - Check referential integrity before ETL execution

## Advanced Configuration

For more complex SQLite processing needs:

1. **Parameterized Queries**:
   ```yaml
   extractors:
     - name: parameterized_extractor
       type: sql
       config:
         connection_string: "sqlite:///data/source/retail.db"
         query: |
           SELECT * FROM orders 
           WHERE order_date >= :start_date 
           AND order_date <= :end_date
         query_params:
           start_date: "2025-01-01"
           end_date: "2025-03-31"
   ```

2. **Multiple Query Extraction**:
   ```yaml
   extractors:
     - name: orders_extractor
       type: sql
       config:
         connection_string: "sqlite:///data/source/retail.db"
         query: "SELECT * FROM orders"
         output_dataframe: "orders"
         
     - name: products_extractor
       type: sql
       config:
         connection_string: "sqlite:///data/source/retail.db"
         query: "SELECT * FROM products"
         output_dataframe: "products"
   ```

3. **Custom SQLite Pragmas**:
   ```yaml
   extractors:
     - name: optimized_extractor
       type: sql
       config:
         connection_string: "sqlite:///data/source/retail.db"
         query: "SELECT * FROM large_table"
         pragmas:
           - "cache_size = 10000"
           - "temp_store = MEMORY"
           - "mmap_size = 30000000000"
   ```

4. **Incremental Extraction**:
   ```yaml
   extractors:
     - name: incremental_extractor
       type: sql
       config:
         connection_string: "sqlite:///data/source/retail.db"
         query: |
           SELECT * FROM orders 
           WHERE last_modified > (
             SELECT COALESCE(MAX(last_extracted), '2000-01-01') 
             FROM _etl_watermarks WHERE table_name = 'orders'
           )
         watermark:
           enable: true
           table: "_etl_watermarks"
           field: "last_modified"
   ```

This end-to-end guide covers the complete process for working with SQLite databases in the Data Warehouse ETL Framework, from initial extraction to transformation and loading of results.

## Technical Implementation Details

### Module Structure and Program Flow

The ETL Framework processes SQLite databases through several key modules and classes:

```
src/
├── extractors/
│   ├── base_extractor.py         # BaseExtractor abstract class
│   ├── sql_extractor.py          # SQLExtractor implementation
│   └── extractor_factory.py      # Factory for creating extractors
├── transformers/
│   ├── base_transformer.py       # BaseTransformer abstract class
│   ├── cleaning_transformer.py   # Data cleaning implementation
│   ├── aggregation_transformer.py # Aggregation implementation
│   └── transformer_factory.py    # Factory for creating transformers
├── loaders/
│   ├── base_loader.py            # BaseLoader abstract class
│   ├── sql_loader.py             # SQLLoader implementation
│   ├── csv_loader.py             # CSVLoader implementation
│   └── loader_factory.py         # Factory for creating loaders
├── utils/
│   ├── config_manager.py         # Configuration handling
│   ├── sql_utils.py              # SQL utilities
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
│ConfigManager.   │     │SQLExtractor.    │     │CleaningTransf.  │     │SQLLoader.       │
│load_config()    │     │extract()        │     │transform()      │     │load()           │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
                                │                       │                       │
                                ▼                       ▼                       ▼
                        ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
                        │SQLAlchemy       │     │AggregationTransf│     │CSVLoader.       │
                        │execute_query()  │     │transform()      │     │load()           │
                        └─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Key Classes and Methods

#### 1. SQLExtractor Class (extractors/sql_extractor.py)

Responsible for extracting data from SQLite databases:

```python
class SQLExtractor(BaseExtractor):
    def __init__(self, config):
        """Initialize with extraction configuration."""
        super().__init__(config)
        self.connection_string = config.get('connection_string')
        self.query = config.get('query')
        self.query_params = config.get('query_params', {})
        self.chunk_size = config.get('chunk_size', None)
        self.max_rows = config.get('max_rows', None)
        self.engine = None
        self.connection = None
        self.pragmas = config.get('pragmas', [])
        self.output_dataframe = config.get('output_dataframe', None)
        
    def validate_source(self):
        """Validate the SQLite source exists and is accessible."""
        try:
            # Check connection can be established
            engine = sa.create_engine(self.connection_string)
            conn = engine.connect()
            conn.close()
            
            # Validate query syntax
            if self.query:
                # Simply check if the query is valid SQL syntax
                # Use SQLite's compile feature without executing
                import sqlite3
                sqlite3.complete_statement(self.query)
                
            return True
        except Exception as e:
            self.logger.error(f"SQL source validation failed: {str(e)}")
            raise
        
    def _apply_pragmas(self, connection):
        """Apply SQLite pragmas to the connection."""
        if not self.pragmas:
            return
            
        self.logger.debug(f"Applying {len(self.pragmas)} SQLite pragmas")
        cursor = connection.cursor()
        for pragma in self.pragmas:
            cursor.execute(f"PRAGMA {pragma}")
        
    def extract(self):
        """Extract data from the SQL database into a pandas DataFrame."""
        self.logger.info(f"Connecting to SQLite database")
        
        try:
            # Create engine and connection
            self.engine = sa.create_engine(self.connection_string)
            self.connection = self.engine.connect()
            
            # Apply SQLite pragmas if specified
            if self.pragmas and 'sqlite' in self.connection_string.lower():
                self._apply_pragmas(self.connection.connection)
            
            self.logger.info(f"Executing query against database")
            
            start_time = time.time()
            
            # Execute the query with parameters
            if self.chunk_size:
                # For large result sets, use chunking
                chunks = []
                for chunk_df in pd.read_sql_query(
                    self.query,
                    self.connection,
                    params=self.query_params,
                    chunksize=self.chunk_size
                ):
                    chunks.append(chunk_df)
                    if self.max_rows and len(chunks) * self.chunk_size >= self.max_rows:
                        break
                
                df = pd.concat(chunks)
                if self.max_rows:
                    df = df.head(self.max_rows)
            else:
                # Standard execution
                df = pd.read_sql_query(
                    self.query,
                    self.connection,
                    params=self.query_params
                )
                if self.max_rows:
                    df = df.head(self.max_rows)
            
            query_time = time.time() - start_time
            
            self.logger.info(f"Retrieved {len(df)} records from database query in {query_time:.2f} seconds")
            
            # Set name for multi-dataframe pipelines
            if self.output_dataframe:
                df.attrs['dataframe_name'] = self.output_dataframe
                
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting data from SQL: {str(e)}")
            raise
            
        finally:
            # Close connection
            if self.connection:
                self.connection.close()
                self.connection = None
```

#### 2. AggregationTransformer Class (transformers/aggregation_transformer.py)

Handles data aggregation operations:

```python
class AggregationTransformer(BaseTransformer):
    def __init__(self, config):
        """Initialize with aggregation configuration."""
        super().__init__(config)
        self.group_by = config.get('group_by', [])
        self.aggregations = config.get('aggregations', [])
        self.output_dataframe = config.get('output_dataframe', None)
        self.preserve_original = config.get('preserve_original', False)
        
    def transform(self, df):
        """Apply aggregation transformations to the DataFrame."""
        self.logger.info(f"Performing aggregation by {', '.join(self.group_by)}")
        
        if not self.group_by or not self.aggregations:
            self.logger.warning("No group_by or aggregation functions specified")
            return df
            
        try:
            # Build aggregation dictionary
            agg_dict = {}
            for agg in self.aggregations:
                column = agg.get('column')
                function = agg.get('function')
                new_column = agg.get('new_column', f"{column}_{function}")
                
                if column and function:
                    agg_dict[new_column] = pd.NamedAgg(column=column, aggfunc=function)
            
            # Perform aggregation
            result_df = df.groupby(self.group_by).agg(**agg_dict).reset_index()
            
            if self.output_dataframe:
                # Store the aggregated result with a name
                result_df.attrs['dataframe_name'] = self.output_dataframe
                
                if self.preserve_original:
                    # Return both dataframes in a dictionary
                    self.logger.info(f"Created aggregated dataframe '{self.output_dataframe}' with {len(result_df)} records")
                    return {
                        'original': df,
                        self.output_dataframe: result_df
                    }
                
            self.logger.info(f"Aggregation complete, resulting in {len(result_df)} records")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error during aggregation: {str(e)}")
            raise
```

#### 3. SQLLoader Class (loaders/sql_loader.py)

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
        self.source_dataframe = config.get('source_dataframe', None)
        self.engine = None
        
    def validate_destination(self):
        """Validate the database connection and write permissions."""
        try:
            self.engine = sa.create_engine(self.connection_string)
            connection = self.engine.connect()
            
            # Check if we can create a temporary table
            connection.execute(sa.text("CREATE TEMPORARY TABLE etl_framework_test (id INTEGER)"))
            connection.execute(sa.text("DROP TABLE etl_framework_test"))
            
            connection.close()
            return True
        except Exception as e:
            self.logger.error(f"SQL destination validation failed: {str(e)}")
            raise
            
    def _serialize_complex_types(self, df):
        """Convert complex data types to JSON strings for SQLite compatibility."""
        df_copy = df.copy()
        
        for col in df_copy.select_dtypes(include=['object']).columns:
            # Check if column contains lists, dicts or other complex types
            if df_copy[col].apply(lambda x: isinstance(x, (list, dict, tuple))).any():
                df_copy[col] = df_copy[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict, tuple)) else x)
                
        return df_copy
            
    def load(self, df_input):
        """Load the DataFrame to the SQL database."""
        self.logger.info(f"Connecting to database")
        
        try:
            # Handle multiple dataframes case
            if isinstance(df_input, dict):
                if self.source_dataframe:
                    if self.source_dataframe in df_input:
                        df = df_input[self.source_dataframe]
                        self.logger.info(f"Using source dataframe '{self.source_dataframe}'")
                    else:
                        raise KeyError(f"Source dataframe '{self.source_dataframe}' not found in input")
                else:
                    # Default to the first dataframe
                    df = list(df_input.values())[0]
            else:
                df = df_input
                
            # Create engine
            if self.engine is None:
                self.engine = sa.create_engine(self.connection_string)
                
            # Handle complex data types for SQLite
            df = self._serialize_complex_types(df)
            
            self.logger.info(f"Creating table '{self.table_name}'")
            
            # For large datasets, use batching
            if len(df) > self.batch_size:
                batches = [df[i:i+self.batch_size] for i in range(0, len(df), self.batch_size)]
                
                for i, batch in enumerate(batches):
                    batch_if_exists = self.if_exists if i == 0 else 'append'
                    
                    batch.to_sql(
                        self.table_name,
                        self.engine,
                        if_exists=batch_if_exists,
                        index=self.index
                    )
                    self.logger.debug(f"Loaded batch {i+1}/{len(batches)}")
            else:
                df.to_sql(
                    self.table_name,
                    self.engine,
                    if_exists=self.if_exists,
                    index=self.index
                )
                
            self.logger.info(f"Inserted {len(df)} records into table '{self.table_name}'")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading data to SQL database: {str(e)}")
            raise
            
        finally:
            # Close engine
            if self.engine:
                self.engine.dispose()
                self.engine = None
```

### Component Interaction and Data Flow

The following sequence diagram shows how the components interact during a SQLite ETL process:

```
┌───────────┐          ┌──────────────┐          ┌──────────────┐          ┌──────────────┐          ┌───────────┐
│  main.py  │          │  ETLPipeline │          │ SQLExtractor │          │ Transformers │          │  Loaders  │
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
      │                       │                         │ create_engine()         │                         │
      │                       │                         │◄──────────              │                         │
      │                       │                         │                         │                         │
      │                       │                         │ apply_pragmas()         │                         │
      │                       │                         │◄──────────              │                         │
      │                       │                         │                         │                         │
      │                       │                         │ execute_query()         │                         │
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
      │                       │                         │                         │                         │ create_engine()
      │                       │                         │                         │                         │◄──────
      │                       │                         │                         │                         │
      │                       │                         │                         │                         │ pandas.to_sql()
      │                       │                         │                         │                         │◄──────
      │                       │                         │                         │                         │
      │                       │ Loading Result          │                         │                         │
      │                       │<──────────────────────────────────────────────────────────────────────────  │
      │                       │                         │                         │                         │
      │ run() result          │                         │                         │                         │
      │<──────────────────────│                         │                         │                         │
      │                       │                         │                         │                         │
┌─────┴─────┐          ┌──────┴───────┐          ┌──────┴───────┐          ┌──────┴───────┐          ┌─────┴─────┐
│  main.py  │          │  ETLPipeline │          │ SQLExtractor │          │ Transformers │          │  Loaders  │
└───────────┘          └──────────────┘          └──────────────┘          └──────────────┘          └───────────┘
```

### SQLite-Specific Implementation Details

#### Multi-database Query Execution

For complex ETL processes involving multiple databases, the framework uses a specialized multi-query approach:

```python
def execute_multi_db_query(primary_conn_string, query, params=None, secondary_conn_strings=None):
    """Execute a query that might reference attached databases."""
    import sqlite3
    
    # Connect to main database
    conn = sqlite3.connect(primary_conn_string.replace('sqlite:///', ''))
    
    try:
        # Attach secondary databases if provided
        if secondary_conn_strings:
            for i, db_conn in enumerate(secondary_conn_strings):
                db_path = db_conn.replace('sqlite:///', '')
                db_name = f"db{i}"
                conn.execute(f"ATTACH DATABASE '{db_path}' AS {db_name}")
        
        # Execute the query with parameters
        cursor = conn.cursor()
        if params:
            result = cursor.execute(query, params)
        else:
            result = cursor.execute(query)
            
        # Fetch results
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        
        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=columns)
        return df
        
    finally:
        # Detach databases and close connection
        if secondary_conn_strings:
            for i in range(len(secondary_conn_strings)):
                db_name = f"db{i}"
                conn.execute(f"DETACH DATABASE {db_name}")
        conn.close()
```

#### Optimized Batch Loading

For large dataset loading to SQLite, the framework uses a transaction-based approach for better performance:

```python
def batch_load_sqlite(df, connection_string, table_name, batch_size=1000, if_exists='replace'):
    """Load data to SQLite in batches with optimized transaction handling."""
    import sqlite3
    
    # Strip sqlite:/// prefix if present
    db_path = connection_string.replace('sqlite:///', '')
    
    # Connect to the database
    conn = sqlite3.connect(db_path)
    
    try:
        # Get column names and types
        column_specs = []
        for col, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                col_type = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                col_type = "REAL"
            elif pd.api.types.is_datetime64_dtype(dtype):
                col_type = "TIMESTAMP"
            else:
                col_type = "TEXT"
            column_specs.append(f"`{col}` {col_type}")
        
        # Create table if needed
        if if_exists == 'replace':
            conn.execute(f"DROP TABLE IF EXISTS `{table_name}`")
            
        create_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(column_specs)})"
        conn.execute(create_sql)
        
        # Prepare for batch insertion
        placeholders = ", ".join(["?" for _ in range(len(df.columns))])
        insert_sql = f"INSERT INTO `{table_name}` VALUES ({placeholders})"
        
        # Process in batches within a transaction
        with conn:
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                
                # Convert timestamps to ISO format strings for SQLite
                for col in batch.select_dtypes(include=['datetime64']).columns:
                    batch[col] = batch[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # Insert batch
                conn.executemany(insert_sql, batch.values.tolist())
                
        return True
        
    finally:
        conn.close()
```

#### Custom SQLite Function Registration

The framework supports registering custom functions for use in SQLite queries:

```python
def register_sqlite_functions(connection):
    """Register custom functions for use in SQLite queries."""
    import math
    import re
    
    # Mathematical functions
    connection.create_function("POW", 2, math.pow)
    connection.create_function("SQRT", 1, math.sqrt)
    connection.create_function("LOG", 1, math.log)
    
    # String functions
    connection.create_function("REGEXP", 2, 
                              lambda pattern, string: bool(re.search(pattern, string)) if string else False)
    
    # Date functions
    connection.create_function("DATE_PART", 2, 
                              lambda part, date_str: date_str.split('-')[0] if part.lower() == 'year' else
                                                   date_str.split('-')[1] if part.lower() == 'month' else
                                                   date_str.split('-')[2] if part.lower() == 'day' else None)
```

These detailed technical explanations provide a comprehensive understanding of how the framework implements SQLite database processing, including extraction using SQLAlchemy, transformation with pandas, and optimized loading back to SQLite.
