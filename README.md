# Animal ETL Pipeline

A robust Python ETL (Extract, Transform, Load) system that fetches animal data from a paginated API, transforms it according to business requirements, and loads it to a destination endpoint with built-in resilience for network failures and server errors.

## Features

- **Resilient API Client**: Handles random server pauses and HTTP errors (500, 502, 503, 504) with exponential backoff retry mechanism
- **Efficient Data Processing**: Fetches paginated animal data and processes individual animal details
- **Data Transformation**: Converts comma-delimited friends strings to arrays and ISO8601 timestamp formatting for born_at fields
- **Batch Processing**: Sends transformed data in optimized batches of up to 100 animals
- **Comprehensive Logging**: Detailed logging for monitoring and debugging
- **Modular Architecture**: Clean separation of concerns with reusable components

## Steps to Solve

### 1. Environment Setup
- Set up Python virtual environment
- Install required dependencies
- Configure Docker container for the API server
- Set up logging and configuration management

### 2. API Client Development
- Implement paginated data fetching from `/animals/v1/animals`
- Build individual animal detail retrieval from `/animals/v1/animals/<id>`
- Add resilient error handling with retry logic for server failures
- Implement exponential backoff for random pauses and HTTP errors

### 3. Data Transformation
- Parse comma-delimited friends strings into arrays
- Convert born_at timestamps to ISO8601 UTC format
- Validate transformed data structure

### 4. Batch Processing & Loading
- Implement batching logic (max 100 animals per batch)
- POST transformed data to `/animals/v1/home` endpoint
- Handle batch-level failures and retries

### 5. Testing & Validation
- Unit tests for transformation logic
- Integration tests for API client
- End-to-end pipeline testing

## Folder Structure

```
├── Common/
│   ├── __pycache__/           # Python bytecode cache
│   ├── configs.py             # Configuration management (API URLs, retry settings)
│   ├── logger.py              # Centralized logging setup
│   ├── models.py              # Data models and schemas (Animal, etc.)
│   └── utils.py               # Utility functions and helpers
├── README.md                  # Project documentation
├── app.log                    # Application log file
├── application/
│   ├── __pycache__/           # Python bytecode cache
│   ├── api_client.py          # HTTP client with retry logic and API interactions
│   ├── main.py                # Main ETL pipeline orchestrator
│   └── transformer.py        # Data transformation logic
├── requirements.txt           # Python dependencies
└── tests/
    ├── __pycache__/           # Python bytecode cache
    └── test.py                # Unit and integration tests
```

## Sample Environment Configuration

Create a `.env` file in the project root:

```env
BASE_URL = "http://localhost:3123"
BATCH_SIZE = 100
MAX_ATTEMPTS = 5
RETRY_BASE_DELAY = 1  # in seconds
MAX_RETRY_DELAY = 120  # in seconds
BACKOFF_FACTOR = 2
```

## Prerequisites

Before running the application, ensure you have:

### 1. Docker Setup
```bash
# Download the Docker image
wget https://storage.googleapis.com/lp-dev-hiring/images/lp-programming-challenge-1-1625758668.tar.gz

# Load the container
docker load -i lp-programming-challenge-1-1625758668.tar.gz

# Run the container (expose port 3123)
docker run --rm -p 3123:3123 -ti lp-programming-challenge-1
```

### 2. Github Clone
```bash
git clone https://github.com/Zarar-Azwar/animals_api.git
```

### 3. Python Environment
```bash
# Create virtual environment
python -m venv animal-etl-env

# Activate virtual environment
# On Windows:
animal-etl-env\Scripts\activate
# On macOS/Linux:
source animal-etl-env/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 4. Verify API Access
Open [http://localhost:3123/](http://localhost:3123/) in your browser to confirm the API is running.
Check the API documentation at [http://localhost:3123/docs](http://localhost:3123/docs).

## How to Run

### 1. Start the Docker Container
```bash
docker run --rm -p 3123:3123 -ti lp-programming-challenge-1
```
Keep this running in a separate terminal.

### 2. Run Tests (Recommended First)
```bash
cd animals_api
# Run all tests to validate setup
python -m tests.test
```

### 3. Execute the ETL Pipeline
```bash
# Run the main ETL process
python -m application.main
```

### 4. Monitor Progress
- Check console output for real-time progress
- View detailed logs in `app.log`
- Monitor API responses and retry attempts

## Expected Output

The application will:
1. Fetch all animals from the paginated API
2. Retrieve detailed information for each animal
3. Transform the data (friends array, ISO8601 timestamps)
4. Send data in batches to the home endpoint
5. Provide success/failure statistics

## Error Handling

The system handles:
- **Network timeouts**: Automatic retry with exponential backoff
- **Server errors (500, 502, 503, 504)**: Retry logic with configurable attempts
- **Random pauses**: Patient waiting with timeout handling
- **Rate limiting**: Built-in delays between requests
- **Data validation errors**: Logging and graceful error handling

## Performance Considerations

- **Concurrent processing**: Configurable concurrent request handling
- **Batch optimization**: Efficient 100-animal batches for optimal throughput
- **Memory management**: Streaming processing for large datasets
- **Rate limiting**: Respectful API usage with configurable delays




