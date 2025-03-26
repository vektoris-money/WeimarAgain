# Token Data Fetch Script

This script fetches cryptocurrency token data from the CoinGecko API (https://docs.coingecko.com/reference/introduction), processes it, and saves the results as Apache Parquet files. It is designed to efficiently retrieve detailed coin information, market charts, and OHLC (Open, High, Low, Close) data over a defined period using concurrent processing and robust error handling.

## Overview

- **Data Sources:**  
  - **Coin Details:** Retrieves comprehensive details for each token (e.g., market data, community metrics, developer statistics).
  - **Market Chart:** Gathers historical price, volume, and market cap data for each token.
  - **OHLC Data:** Collects open, high, low, and close prices over the selected time period.

- **Time Range:**  
  - The script processes data for the last 180 days (calculated from the current UTC date).

- **Output:**  
  - Processed data is saved as Apache Parquet files in dedicated subdirectories (`coins`, `market_chart`, and `ohlc`) within a specified data directory.
  - Logs and metadata are maintained to track the execution and handle any errors.

## Features

- **Batch Processing & Concurrency:**  
  Processes tokens in batches (default size: 500) using a thread pool (default: 5 workers) for efficient API requests.

- **Error Handling & Logging:**  
  Implements exponential backoff with retries for failed requests and logs errors. Tokens that fail to fetch are recorded in a `failed_tokens.json` file.

- **Data Transformation:**  
  - Flattens nested JSON structures from the API.
  - Converts specified fields to numeric data types to prevent conversion issues during Parquet file creation.

- **Progress Tracking:**  
  Uses progress bars via `tqdm` to display the status of concurrent processing.

## Requirements

- **Python Version:** Python 3.x
- **Required Libraries:**  
  - `os`
  - `json`
  - `time`
  - `logging`
  - `requests`
  - `pandas`
  - `pyarrow`
  - `glob`
  - `concurrent.futures`
  - `datetime`
  - `tqdm`
- **API Key:**  
  A valid CoinGecko API key (replace the default key in the script or load from environment variables).

## Installation

1. **Clone or Download the Script:**  
   Save the script to your local machine.

2. **Install Dependencies:**  
   Use pip to install the required packages:
   ```bash
   pip install requests pandas pyarrow tqdm
   ```

## Configuration

Before running the script, review and adjust the following configuration variables as needed:

- **API Settings:**
  - `BASE_URL`: Base URL for the CoinGecko API.
  - `API_KEY`: Your CoinGecko API key.
  - `HEADERS`: HTTP headers including your API key.

- **Directories & Files:**
  - `DATA_DIR`: Path to the directory where data, logs, and metadata will be stored.
  - Subdirectories (`coins`, `market_chart`, `ohlc`) are automatically created under `DATA_DIR`.
  - Log and metadata files include `data_fetch.log`, `batches.json`, `metadata.json`, and `failed_tokens.json`.

- **Request & Retry Settings:**
  - `THROTTLE_DELAY`: Delay between API requests to respect rate limits.
  - `MAX_RETRIES`: Maximum number of retry attempts for failed requests.
  - `BACKOFF_FACTOR`: Factor used for exponential backoff.

- **Concurrency & Batching:**
  - `MAX_WORKERS`: Number of concurrent threads.
  - `BATCH_SIZE`: Number of tokens processed in each batch.

## Input

The script does not require any external input files or command-line arguments. It operates as follows:

- **API Endpoints:**
  - `/coins/list`: Fetches the complete list of tokens.
  - `/coins/{token_id}`: Retrieves detailed data for a given token.
  - `/coins/{token_id}/market_chart/range`: Fetches historical market chart data (prices, volumes, market caps).
  - `/coins/{token_id}/ohlc/range`: Retrieves OHLC data for the token.

- **Date Range:**  
  Automatically calculates the start and end dates (last 180 days) based on the current UTC date.

## Output

After execution, the following outputs will be generated:

- **Parquet Files:**  
  - **Coins Data:** Saved in the `coins` subdirectory.
  - **Market Chart Data:** Saved in the `market_chart` subdirectory.
  - **OHLC Data:** Saved in the `ohlc` subdirectory.
  
  Each file is named using a batch index and a timestamp (e.g., `batch_0_20250326123456.parquet`).

- **Logs:**  
  A log file (`data_fetch.log`) in the data directory contains detailed execution logs and error messages.

- **Metadata Files:**  
  - `metadata.json`: Contains information about the last processed batch and timestamp.
  - `failed_tokens.json`: Records details of any tokens for which data could not be fetched.

## Script Workflow

1. **Initialization:**  
   - Prints a start-up message.
   - Sets up configuration variables and creates necessary directories.

2. **Logging Setup:**  
   Configures logging to output to both the console and a log file.

3. **Token List Retrieval:**  
   Fetches the list of tokens from the CoinGecko API.

4. **Batch Processing:**  
   Processes tokens in batches:
   - Uses a thread pool to concurrently fetch data (coin details, market chart, and OHLC) for each token.
   - Processes and flattens JSON data.
   - Converts specified columns to numeric types.
   - Saves processed data as Parquet files in the appropriate subdirectories.
   - Records any tokens with failed data fetches.

5. **Completion:**  
   Logs the completion of all batches.

## Execution

To run the script, execute the following command in your terminal:
```bash
python script_name.py
```
Replace `script_name.py` with the actual file name.

## License

MIT License

## Contributing

Contributions are welcome. Please follow standard guidelines for pull requests and code reviews if you wish to contribute.

## Contact

For any questions or support, please contact contact@vektoris.com


