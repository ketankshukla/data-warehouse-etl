# End-to-End ETL Process with REST APIs

This guide walks through the complete process of extracting data from a REST API, transforming it, and loading it to a destination using the Data Warehouse ETL Framework.

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
10. [Technical Implementation Details](#technical-implementation-details)

## Overview

REST APIs are a common source of data in modern applications. This guide demonstrates how to extract data from REST APIs, process it, and load it to a destination using the Data Warehouse ETL Framework.

The example we'll follow involves extracting weather data from a public API, transforming it for analysis, and loading it into both a database and a CSV file for reporting.

## Prerequisites

Before starting, ensure you have:

1. The Data Warehouse ETL Framework installed
2. Python 3.8 or higher
3. Required dependencies installed (`pip install -r requirements.txt`)
4. API access credentials (if required)
5. Internet connectivity to access the API
6. Target destination systems configured

## Configuration Setup

### Sample API Request

Our example uses a weather API that provides current weather data for cities:

**API Endpoint:** `https://api.weatherservice.com/v1/current`

**Sample Response:**
```json
{
  "location": {
    "name": "Seattle",
    "region": "Washington",
    "country": "USA",
    "lat": 47.61,
    "lon": -122.33,
    "tz_id": "America/Los_Angeles"
  },
  "current": {
    "last_updated": "2025-03-24 09:30",
    "temp_c": 12.0,
    "temp_f": 53.6,
    "condition": {
      "text": "Partly cloudy",
      "icon": "//cdn.weatherservice.com/images/64x64/day/partly-cloudy.png",
      "code": 1003
    },
    "wind_mph": 6.9,
    "wind_kph": 11.2,
    "wind_degree": 270,
    "wind_dir": "W",
    "pressure_mb": 1015.0,
    "pressure_in": 30.0,
    "precip_mm": 0.0,
    "precip_in": 0.0,
    "humidity": 78,
    "cloud": 25,
    "feelslike_c": 11.0,
    "feelslike_f": 51.8,
    "uv": 3.0
  }
}
```

### YAML Configuration

Create a YAML configuration file (`api_etl_config.yaml`) that defines the ETL process:

```yml
job:
  name: weather_data_processing
  description: "Extract weather data from API, transform, and load to database and CSV"

extractors:
  - name: weather_api
    type: api
    config:
      base_url: "https://api.weatherservice.com/v1"
      endpoint: "current"
      method: "GET"
      params:
        key: "${WEATHER_API_KEY}"  # Using environment variable for API key
        q: "Seattle,New York,Chicago,Miami,Denver"  # Multiple cities
        aqi: "no"
      headers:
        Accept: "application/json"
        User-Agent: "ETL-Framework/1.0"
      rate_limit:
        requests_per_minute: 5  # Respect API rate limits
      pagination:
        enabled: false  # This API doesn't use pagination for this endpoint
      
transformers:
  - name: weather_normalizer
    type: custom
    config:
      # Flatten the nested structure
      flatten_fields:
        - path: "location"
          prefix: "loc_"
        - path: "current"
          prefix: ""
        - path: "current.condition"
          prefix: "condition_"
      # Extract only needed fields
      select_fields:
        - "loc_name"
        - "loc_region"
        - "loc_country"
        - "loc_lat"
        - "loc_lon"
        - "last_updated"
        - "temp_c"
        - "temp_f"
        - "condition_text"
        - "wind_kph"
        - "pressure_mb"
        - "humidity"
        - "cloud"
        - "uv"
      
  - name: data_cleaner
    type: cleaning
    config:
      # Date transformation
      date_formats:
        last_updated: "%Y-%m-%d %H:%M"
      # Data typing
      type_conversions:
        temp_c: "float"
        temp_f: "float"
        humidity: "int"
        
  - name: weather_classifier
    type: custom
    config:
      # Add derived fields based on conditions
      conditional_fields:
        - name: "temperature_category"
          conditions:
            - condition: "temp_c < 0"
              value: "Freezing"
            - condition: "temp_c >= 0 and temp_c < 10"
              value: "Cold"
            - condition: "temp_c >= 10 and temp_c < 20"
              value: "Mild"
            - condition: "temp_c >= 20 and temp_c < 30"
              value: "Warm"
            - condition: "temp_c >= 30"
              value: "Hot"
              
loaders:
  - name: weather_db
    type: sql
    config:
      connection_string: "sqlite:///data/output/weather_data.db"
      table_name: "current_weather"
      if_exists: "replace"
      index: false
      
  - name: weather_csv
    type: csv
    config:
      file_path: "data/output/weather_report.csv"
      delimiter: ","
      include_header: true
      encoding: "utf-8"
```

## Extraction Phase

When the ETL job runs, here's exactly what happens during the extraction phase:

1. **Initialization**:
   - The ETLPipeline loads the configuration file and identifies the `api` extractor
   - An APIExtractor instance is created with the specified configuration
   - Environment variables like `WEATHER_API_KEY` are resolved

2. **Configuration Validation**:
   - The extractor validates the base URL format
   - Checks that required parameters are present
   - Verifies authentication configuration (if needed)

3. **Request Preparation**:
   - The full API URL is constructed by combining the base URL and endpoint
   - Query parameters are prepared (including the API key)
   - Headers are added to the request

4. **API Request Execution**:
   - The HTTP request is made to the API endpoint
   - Rate limiting is applied to respect API constraints
   - For multiple queries (like multiple cities), requests are made sequentially
   - Retries are automatically applied for transient errors (network issues, rate limiting)

5. **Response Processing**:
   - API responses are validated for success (HTTP 200-299 status codes)
   - JSON responses are parsed into Python dictionaries
   - For multiple requests, results are combined into a list
   - Error responses are handled gracefully with detailed error information

6. **Result Preparation**:
   - The extracted data is converted into a pandas DataFrame
   - For requests with multiple items (like multiple cities), each item becomes a row
   - The extractor adds metadata (source details, timestamp, request count)
   - The DataFrame is returned to the pipeline for the transformation phase

### API-Specific Considerations

- **Authentication**: The framework handles various authentication methods (API keys, OAuth, etc.)
- **Rate Limiting**: Automatic throttling prevents hitting API limits
- **Pagination**: For APIs with paginated results, the extractor automatically handles pagination
- **Error Handling**: Robust error handling for network issues, authentication failures, and API errors
- **Caching**: Optional response caching to reduce API calls during development and testing

## Transformation Phase

Once the data is extracted, the transformation phase proceeds:

1. **Weather Normalizer Transformer**:
   - Receives the DataFrame with nested API response structure
   - Flattens the JSON structure for easier processing:
     - `location.name` becomes `loc_name`
     - `current.temp_c` becomes `temp_c`
     - `current.condition.text` becomes `condition_text`
   - Selects only the specified fields, removing unneeded data

2. **Data Cleaner Transformer**:
   - Processes the structured data:
     - Converts the `last_updated` string to a datetime object
     - Ensures temperature values are floating-point numbers
     - Converts humidity to an integer

3. **Weather Classifier Transformer**:
   - Adds the derived `temperature_category` field based on temperature ranges
   - Classifies each weather record into categories (Freezing, Cold, Mild, Warm, Hot)

4. **Transformation Pipeline Flow**:
   - Each transformer operates sequentially
   - The data becomes progressively more refined and analysis-ready
   - The pipeline maintains the single DataFrame throughout transformation

## Loading Phase

After transformation, the loading phase begins:

1. **SQL Loader**:
   - The SQLLoader establishes a connection to the SQLite database
   - Creates (or replaces) the `current_weather` table based on the DataFrame schema
   - Loads all weather data records into the table
   - Complex data that wasn't flattened is automatically serialized to JSON

2. **CSV Loader**:
   - The CSVLoader writes the same transformed data to a CSV file
   - Headers are included based on the configuration
   - All fields are written with the specified delimiter and encoding

3. **Loading Process Details**:
   - Loading operations happen sequentially
   - Each loader reports its progress and completion status
   - Database connections and file handles are properly closed

## Execution and Monitoring

During execution, the ETL process provides real-time monitoring:

1. **Command Line Execution**:
   ```powershell
   python main.py --config config/api_etl_config.yaml --log-level INFO
   ```

2. **Logging Output**:
   The framework logs each step of the process:
   ```
   2025-03-24 10:45:08 [INFO] [pipeline:92] ETL job 'weather_data_processing' started
   2025-03-24 10:45:08 [INFO] [pipeline:103] Initialization phase complete
   2025-03-24 10:45:08 [INFO] [api_extractor:61] Making API requests to https://api.weatherservice.com/v1/current
   2025-03-24 10:45:08 [INFO] [api_extractor:83] Requesting data for city: Seattle
   2025-03-24 10:45:09 [INFO] [api_extractor:98] Received response with status code 200
   2025-03-24 10:45:09 [INFO] [api_extractor:83] Requesting data for city: New York
   2025-03-24 10:45:10 [INFO] [api_extractor:98] Received response with status code 200
   2025-03-24 10:45:10 [INFO] [api_extractor:83] Requesting data for city: Chicago
   2025-03-24 10:45:11 [INFO] [api_extractor:98] Received response with status code 200
   2025-03-24 10:45:11 [INFO] [api_extractor:83] Requesting data for city: Miami
   2025-03-24 10:45:12 [INFO] [api_extractor:98] Received response with status code 200
   2025-03-24 10:45:12 [INFO] [api_extractor:83] Requesting data for city: Denver
   2025-03-24 10:45:13 [INFO] [api_extractor:98] Received response with status code 200
   2025-03-24 10:45:13 [INFO] [api_extractor:112] Successfully processed 5 API responses
   2025-03-24 10:45:13 [INFO] [pipeline:142] Extraction phase complete, extracted 5 records
   2025-03-24 10:45:13 [INFO] [weather_normalizer:65] Flattening nested API response structure
   2025-03-24 10:45:13 [INFO] [data_cleaner:92] Applying cleaning transformations...
   2025-03-24 10:45:13 [INFO] [weather_classifier:77] Classifying weather records
   2025-03-24 10:45:13 [INFO] [pipeline:178] Transformation phase complete
   2025-03-24 10:45:13 [INFO] [sql_loader:114] Connecting to database
   2025-03-24 10:45:13 [INFO] [sql_loader:143] Creating table 'current_weather'
   2025-03-24 10:45:13 [INFO] [sql_loader:165] Inserted 5 records into table 'current_weather'
   2025-03-24 10:45:13 [INFO] [csv_loader:87] Writing data to CSV file data/output/weather_report.csv
   2025-03-24 10:45:13 [INFO] [csv_loader:102] Wrote 5 records to CSV file
   2025-03-24 10:45:13 [INFO] [pipeline:214] Loading phase complete
   2025-03-24 10:45:13 [INFO] [pipeline:228] ETL job completed successfully in 5.67 seconds
   ```

3. **Performance Monitoring**:
   - The framework collects API-specific metrics:
     - Request count and success rate
     - Response times for each API call
     - Rate limiting status
   - General processing metrics are also tracked

4. **Results**:
   - The processed weather data is available in:
     - The SQLite database table `current_weather`
     - The CSV file `weather_report.csv`
   - A job summary report provides execution statistics

## Troubleshooting

Common issues when working with API sources and their solutions:

1. **Authentication Failures**:
   - Verify API key or credentials are correct
   - Check if the API key has necessary permissions
   - Ensure environment variables are properly set

2. **Rate Limiting Issues**:
   - Configure appropriate rate limits in the `rate_limit` section
   - Implement exponential backoff for retries
   - Consider caching responses for development

3. **API Response Changes**:
   - If the API structure changes, update the transformer configuration
   - Add validation to check for expected fields
   - Use schema validation to ensure compatibility

4. **Network Connectivity Problems**:
   - Configure longer timeouts for unreliable connections
   - Implement retry logic for transient failures
   - Consider fallback data sources for critical processes

5. **Large Response Handling**:
   - For APIs with large responses, use pagination
   - Process data in chunks to manage memory usage
   - Consider filtering at the API level if supported

## Advanced Configuration

For more complex API ETL needs:

1. **OAuth Authentication**:
   ```yml
   extractors:
     - name: oauth_api_extractor
       type: api
       config:
         base_url: "https://api.example.com/v1"
         endpoint: "data"
         auth:
           type: "oauth2"
           token_url: "https://api.example.com/oauth/token"
           client_id: "${CLIENT_ID}"
           client_secret: "${CLIENT_SECRET}"
           scope: "read_data"
   ```

2. **Pagination Handling**:
   ```yml
   extractors:
     - name: paginated_api
       type: api
       config:
         base_url: "https://api.example.com/v1"
         endpoint: "records"
         pagination:
           enabled: true
           strategy: "offset"
           limit_param: "limit"
           offset_param: "offset"
           page_size: 100
           max_pages: 10
   ```

3. **Dynamic Request Parameters**:
   ```yml
   extractors:
     - name: dynamic_api
       type: api
       config:
         base_url: "https://api.example.com/v1"
         endpoint: "data/${YEAR}/${MONTH}"
         params:
           from_date: "${START_DATE}"
           to_date: "${END_DATE}"
         dynamic_params:
           YEAR: "2025"
           MONTH: "03"
           START_DATE: "2025-03-01"
           END_DATE: "2025-03-31"
   ```

4. **Response Transformation with JSONPath**:
   ```yml
   transformers:
     - name: jsonpath_transformer
       type: custom
       config:
         jsonpath_extractions:
           temperatures: "$.data[*].temp"
           cities: "$.data[*].location.name"
           dates: "$.data[*].date"
   ```

## Technical Implementation Details

The Data Warehouse ETL Framework employs sophisticated components to handle API data extraction and transformation:

### Key API ETL Classes

#### 1. APIExtractor Class (extractors/api_extractor.py)

The `APIExtractor` is responsible for fetching data from REST APIs with the following capabilities:
```python
class APIExtractor(BaseExtractor):
    def __init__(self, config):
        """Initialize with API extraction configuration."""
        super().__init__(config)
        self.base_url = config.get('base_url')
        self.endpoint = config.get('endpoint', '')
        self.method = config.get('method', 'GET').upper()
        self.params = config.get('params', {})
        self.headers = config.get('headers', {})
        self.body = config.get('body', {})
        self.timeout = config.get('timeout', 30)
        self.verify_ssl = config.get('verify_ssl', True)
        
        # Authentication setup
        auth_config = config.get('auth', {})
        self.auth_type = auth_config.get('type', None)
        self.auth = self._setup_authentication(auth_config)
        
        # Pagination configuration
        pagination_config = config.get('pagination', {})
        self.pagination_enabled = pagination_config.get('enabled', False)
        self.pagination_type = pagination_config.get('type', 'offset')  # 'offset' or 'cursor'
        self.pagination_params = pagination_config.get('params', {})
        
        # Rate limiting
        rate_limit_config = config.get('rate_limit', {})
        requests_per_minute = rate_limit_config.get('requests_per_minute', 60)
        requests_per_day = rate_limit_config.get('requests_per_day', None)
        self.rate_limiter = RateLimiter(requests_per_minute, requests_per_day)
        
        # Retry configuration
        retry_config = config.get('retry', {})
        self.max_retries = retry_config.get('max_retries', 3)
        self.retry_delay = retry_config.get('delay_seconds', 1)
        self.retry_backoff = retry_config.get('backoff_factor', 2)
```

**Key Features:**
- Multiple authentication methods: Basic Auth, API Key, Bearer Token, and OAuth2
- Robust pagination handling (offset and cursor-based)
- Built-in rate limiting to avoid API throttling
- Retry logic with exponential backoff for transient errors
- Request parameter customization and header management

#### 2. FlatteningTransformer Class (transformers/flattening_transformer.py)

The `FlatteningTransformer` handles nested JSON structures, making them easier to analyze:
```python
class FlatteningTransformer(BaseTransformer):
    def __init__(self, config):
        """Initialize with configuration parameters."""
        super().__init__(config)
        
        # Fields to flatten with their configurations
        self.flatten_fields = config.get("flatten_fields", [])
        if not self.flatten_fields:
            logger.warning("No fields specified for flattening")
            
        self.max_depth = config.get("max_depth", 10)
        logger.info(f"Initialized flattening transformer with {len(self.flatten_fields)} fields to flatten")
```

**Key Features:**
- Handles complex nested JSON structures from API responses
- Configurable depth control for nested objects
- Special handling for arrays with multiple strategies:
  - `first`: Take only the first item in the array
  - `join`: Combine all items with a delimiter
  - `expand`: Create new columns for each array item
- Preserves original fields when needed
- Proper JSON serialization for complex objects at depth limits
- Robust error handling for malformed JSON

#### 3. JSONTransformer Class (transformers/json_transformer.py)

The `JSONTransformer` specializes in processing JSON data with versatile options:
```python
class JSONTransformer(BaseTransformer):
    def __init__(self, config):
        """Initialize with JSON transformation configuration."""
        super().__init__(config)
        
        # Field selection
        self.select_fields = config.get("select_fields", [])
        
        # Field renaming
        self.rename_fields = config.get("rename_fields", {})
        
        # Type casting configuration
        self.type_casting = config.get("type_casting", {})
        
        # Expressions for calculated fields
        self.calculated_fields = config.get("calculated_fields", {})
        
        # Fields to drop
        self.drop_fields = config.get("drop_fields", [])
```

**Key Features:**
- Field selection for focusing on relevant data
- Field renaming for consistent naming conventions
- Comprehensive type casting with support for:
  - Numeric types (int, float)
  - Boolean values with intelligent string conversion (yes/no, true/false)
  - Date and datetime handling
- Dynamic calculated fields using expressions
- Field dropping for removing unnecessary data
- Intelligent string concatenation for creating composite fields

### How These Components Work Together

The API ETL process typically follows this sequence:

1. **Extraction Phase**: The `APIExtractor` fetches data from the API, handling authentication, pagination, and rate limiting.

2. **Initial Transformation**: If the API returns nested JSON data, the `FlatteningTransformer` processes it to create a more accessible tabular structure.

3. **Advanced Transformation**: The `JSONTransformer` then performs additional operations like field selection, renaming, type casting, and creating calculated fields.

4. **Loading Phase**: The processed data is passed to appropriate loaders for storage in databases, files, or other destinations.

### Error Handling and Resilience

The API ETL components include robust error handling:

- The `APIExtractor` implements retry logic for transient network issues
- Rate limiting prevents API quota exhaustion
- The transformers gracefully handle malformed or unexpected data structures
- Comprehensive logging at each stage helps diagnose issues

This design ensures the ETL process is resilient to common API integration challenges and can process even complex, nested JSON responses from various API providers.

### Recent Enhancements

The ETL framework has recently been enhanced with the following improvements:

#### 1. Improved Boolean Type Casting

The JSONTransformer now has more robust boolean type handling that can intelligently convert various string representations to proper boolean values:

```python
def _cast_to_bool(self, value):
    """Cast a value to boolean with extended string support."""
    if isinstance(value, bool):
        return value
    
    if isinstance(value, (int, float)):
        return bool(value)
    
    if isinstance(value, str):
        # Handle case-insensitive string representations
        value = value.lower().strip()
        if value in ('true', 'yes', 'y', '1', 't'):
            return True
        if value in ('false', 'no', 'n', '0', 'f'):
            return False
    
    return bool(value)  # Default Python behavior
```

This enhanced logic allows the transformer to properly handle various boolean representations commonly found in API responses, such as:
- Text values: "yes"/"no", "true"/"false"
- Numeric values: 1/0
- Single character indicators: "Y"/"N", "T"/"F"

#### 2. Enhanced Expression Evaluation

The expression evaluation capabilities have been improved to support more complex operations:

- **String concatenation**: `first_name + " " + last_name`
- **Mathematical operations**: `price * (1 - discount_rate)`
- **Conditional expressions**: `case when status == 'active' then 1 else 0 end`
- **Date arithmetic**: `(today() - created_at).days`

This allows for sophisticated calculated fields that can derive new insights from the API data without requiring additional post-processing.

#### 3. Array Handling in Nested Structures

The FlatteningTransformer now provides multiple strategies for handling arrays within nested JSON:

```yaml
flatten_fields:
  - path: "comments"
    prefix: "comment_"
    array_strategy: "first"  # Only take the first comment
  
  - path: "tags"
    prefix: "tags_"
    array_strategy: "join"   # Join all tags with a delimiter
    delimiter: ", "
    
  - path: "user_roles"
    prefix: "role_"
    array_strategy: "expand" # Create separate columns for each array item
    max_items: 5             # Limit to first 5 roles
```

These improvements make the API ETL components more powerful and flexible, capable of handling a wide variety of API data formats and transformation requirements.
