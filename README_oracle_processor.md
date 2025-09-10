# Oracle Data Processor

This module provides a comprehensive solution for processing Oracle database ticketing data with CBR (Caller Can Be Reached) information. The original script has been refactored into well-structured, reusable functions.

## Features

- **Modular Design**: The original monolithic script has been broken down into logical, reusable functions
- **Parallel Processing**: Utilizes ThreadPoolExecutor for efficient parallel database operations
- **Data Filtering**: Automatically filters out hardcoded numbers and special words
- **Error Handling**: Robust error handling with logging
- **Type Hints**: Full type annotations for better code documentation and IDE support

## Dependencies

```bash
pip install pandas numpy oracledb
```

Note: `oracledb` is required for actual database operations. The module gracefully handles cases where it's not available for testing purposes.

## Main Functions

### Core Processing Function

```python
process_ticket_data(user, password, dsn, start_date, end_date, instant_client_path=None, max_workers=8)
```

This is the main orchestrator function that executes the entire data processing pipeline.

**Parameters:**
- `user`: Database username
- `password`: Database password  
- `dsn`: Data Source Name
- `start_date`: Start date in 'YYYY-MM-DD' format
- `end_date`: End date in 'YYYY-MM-DD' format
- `instant_client_path`: Path to Oracle instant client (optional)
- `max_workers`: Maximum number of parallel workers (default: 8)

**Returns:** DataFrame with processed ticket data including CBR information

### Database Functions

- `create_oracle_connection()`: Creates Oracle database connection
- `fetch_main_data()`: Fetches main ticketing data with customer product information
- `parallel_fetch_ticketing_jobs()`: Fetches TICKETING_JOB_AI data in parallel
- `parallel_fetch_cbr_data()`: Fetches CBR data in parallel from multiple tables
- `parallel_fetch_missing_cbrs()`: Fetches missing CBR data from all tables

### Data Processing Functions

- `group_by_session()`: Groups data by RXPS_SESSION_AI
- `process_column()`: Filters out hardcoded numbers and special words
- `aggregate_cbrs()`: Aggregates CBR values ordered by timestamp
- `batch_list()`: Splits lists into batches for parallel processing

## Usage Example

```python
from oracle_data_processor import process_ticket_data

# Connection parameters
instant_client_path = r'C:\path\to\instantclient'
user = 'your_username'
password = 'your_password'
dsn = 'your_dsn'
start_date = '2024-08-13'
end_date = '2025-08-13'

# Process the data
try:
    result_df = process_ticket_data(
        user=user,
        password=password,
        dsn=dsn,
        start_date=start_date,
        end_date=end_date,
        instant_client_path=instant_client_path,
        max_workers=8
    )
    
    print(f"Processing completed. Result has {len(result_df)} records")
    print(result_df.head())
    
except Exception as e:
    print(f"Error: {e}")
```

## Key Improvements Over Original Script

1. **Modularity**: Broken down into logical, reusable functions
2. **Error Handling**: Comprehensive error handling with proper logging
3. **Documentation**: Full docstrings and type hints
4. **Testability**: Functions can be tested independently
5. **Flexibility**: Configurable parameters for different environments
6. **Maintainability**: Clean, readable code structure

## Data Processing Pipeline

The module implements a 8-step data processing pipeline:

1. **Database Connection**: Establishes connection to Oracle database
2. **Main Data Fetch**: Retrieves core ticketing data with joins
3. **Data Grouping**: Groups data by RXPS_SESSION_AI
4. **Parallel Job Fetch**: Fetches TICKETING_JOB_AI data in parallel
5. **Parallel CBR Fetch**: Retrieves CBR data from multiple tables
6. **Data Aggregation**: Merges and aggregates CBR information
7. **Data Filtering**: Removes hardcoded numbers and special words
8. **Missing Data Recovery**: Fetches missing CBR data from additional sources

## Testing

Run the test suite to validate the function structure:

```bash
python test_oracle_processor.py
```

Note: Database connection functions require actual Oracle database access to test fully.

## Hardcoded Numbers Filter

The module automatically filters out 58+ hardcoded phone numbers commonly found in test data, including:
- 9999999999, 1111111111, 0000000000
- Various test numbers and patterns
- Special words like "none", "i dont have one"

## Performance Considerations

- Uses ThreadPoolExecutor with configurable worker count
- Implements batching (default 900 records per batch) to optimize database queries
- Parallel processing across multiple database tables
- Efficient DataFrame operations with pandas

## Error Handling

- Graceful handling of missing dependencies
- Database connection error handling
- Comprehensive logging throughout the pipeline
- Proper resource cleanup (database connections)