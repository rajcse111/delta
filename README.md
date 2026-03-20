# Delta Fetching Application

A production-ready Python application that periodically retrieves incremental data from a PostgreSQL database.

## Features
- **Incremental Data Fetching**: Uses a delta strategy (e.g., `updated_at` column) to fetch only new or updated records.
- **State Management**: Persists the last processed offset in a file to avoid duplicate processing.
- **Periodic Scheduling**: Uses `APScheduler` for robust periodic execution.
- **Reliability**: Implements structured logging (Loguru) and exponential backoff retries.
- **Configuration**: Managed via environment variables and Pydantic Settings.
- **Database Support**: PostgreSQL with SQLAlchemy connection pooling.

## Prerequisites
- Python 3.9+
- MySQL database

## Installation
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration
1. Update the `.env` file with your database credentials and settings.
   ```env
   DB_HOST=localhost
   DB_PORT=3306
   DB_USER=root
   DB_PASSWORD=
   DB_NAME=test
   DB_TABLE_NAME=users
   FETCH_INTERVAL_MINUTES=1
   DELTA_COLUMN=registration_date
   LAST_PROCESSED_OFFSET_FILE=last_processed_offset.txt
   LOG_LEVEL=INFO
   ```

## Running the Application
### Unix (Linux/macOS)
```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run application
python main.py
```

### Windows
```powershell
# Create virtual environment
python -m venv venv

# Activate virtual environment
.\venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run application
python main.py
```

## Docker Support
1. Build the image:
   ```bash
   docker build -t delta-fetcher .
   ```
2. Run the container:
   ```bash
   docker run --env-file .env delta-fetcher
   ```

## Design Overview
- **`main.py`**: Entry point and scheduler.
- **`database.py`**: PostgreSQL connection and fetching logic.
- **`state.py`**: Persistence of the last processed offset.
- **`config.py`**: Application settings.
