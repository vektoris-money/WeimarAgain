import os
import json
import time
import logging
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import random
import glob
import concurrent.futures
from datetime import datetime, timezone, timedelta
from typing import List

from tqdm import tqdm  # For progress bars

print("Script execution has started!")

# === General Configuration ===
BASE_URL = "https://pro-api.coingecko.com/api/v3"
API_KEY = "YOUR_API_KEY"  # Replace with your API key or fetch from environment variables
HEADERS = {"Accept": "application/json", "x-cg-pro-api-key": API_KEY}

DATA_DIR = "DATA_DIR"  # Change to your data directory
LOG_FILE = os.path.join(DATA_DIR, "data_fetch.log")
BATCHES_FILE = os.path.join(DATA_DIR, "batches.json")
METADATA_FILE = os.path.join(DATA_DIR, "metadata.json")
FAILED_TOKENS_FILE = os.path.join(DATA_DIR, "failed_tokens.json")

THROTTLE_DELAY = 10  # Base delay between token requests
MAX_RETRIES = 5
BACKOFF_FACTOR = 2

# Concurrency and batching configuration
MAX_WORKERS = 5  # Number of concurrent threads
BATCH_SIZE = 500   # Adjust batch size based on available resources and rate limits

# Calculate timestamps for the last 180 days minus one day
today = datetime.now(timezone.utc)
end_date = today
start_date = today - timedelta(days=180)  # 180 days ago

START_DATE = start_date.strftime("%Y-%m-%d")
END_DATE = end_date.strftime("%Y-%m-%d")

# Setup directories
os.makedirs(DATA_DIR, exist_ok=True)
for subdir in ["coins", "market_chart", "ohlc"]:
    os.makedirs(os.path.join(DATA_DIR, subdir), exist_ok=True)

# === Logging Configuration ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# Columns that should be numeric (we'll coerce them to numeric to avoid PyArrow conversion issues).
NUMERIC_COLUMNS = {
    # From coin flatten
    "price_usd",
    "price_btc",
    "mcap_to_tvl_ratio",
    "fdv_to_tvl_ratio",
    "roi",
    "ath_usd",
    "ath_change_percentage_usd",
    "market_cap_usd",
    "fully_diluted_valuation_usd",
    "market_cap_fdv_ratio",
    "total_volume_usd",
    "price_change_percentage_7d",
    "price_change_percentage_14d",
    "price_change_percentage_30d",
    "price_change_percentage_60d",
    "price_change_percentage_200d",
    "price_change_percentage_1y",
    "total_supply",
    "max_supply",
    "circulating_supply",
    "facebook_likes",
    "twitter_followers",
    "reddit_average_posts_48h",
    "reddit_average_comments_48h",
    "reddit_subscribers",
    "reddit_accounts_active_48h",
    "telegram_channel_user_count",
    "forks",
    "stars",
    "subscribers",
    "total_issues",
    "closed_issues",
    "pull_requests_merged",
    "pull_request_contributors",
    "commit_count_4_weeks",

    # From market_chart
    "price",
    "total_volume",
    "market_cap",

    # From OHLC
    "open",
    "high",
    "low",
    "close"
}

def load_metadata():
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            return json.load(f)
    return {"last_processed_batch": -1, "last_processed_timestamp": None, "schema_version": 1}

def update_metadata(metadata):
    with open(METADATA_FILE, "w") as f:
        json.dump(metadata, f, indent=4)

def save_failed_tokens(failed_tokens):
    with open(FAILED_TOKENS_FILE, "w") as f:
        json.dump(failed_tokens, f, indent=4)

def load_failed_tokens():
    if os.path.exists(FAILED_TOKENS_FILE):
        with open(FAILED_TOKENS_FILE, "r") as f:
            return json.load(f)
    return {}

def log_failed_token(token_id, reason="Unknown"):
    failed_tokens = load_failed_tokens()
    failed_records = failed_tokens.get("failed_tokens", [])
    failed_records.append({
        "token_id": token_id,
        "reason": reason,
        "timestamp": datetime.now(timezone.utc).isoformat()
    })
    failed_tokens["failed_tokens"] = failed_records
    save_failed_tokens(failed_tokens)

def exponential_backoff(attempt):
    return BACKOFF_FACTOR ** attempt

def flatten_coin_data(coin):
    flat = {}
    top_level_fields = [
        "id", "symbol", "name", "asset_platform_id", "block_time_in_minutes",
        "hashing_algorithm", "country_origin", "genesis_date", 
        "sentiment_votes_up_percentage", "sentiment_votes_down_percentage", 
        "watchlist_portfolio_users", "market_cap_rank"
    ]
    for field in top_level_fields:
        flat[field] = coin.get(field)

    # A separate reference to the coin's slug if needed
    flat["web_slug"] = coin.get("id")

    market_data = coin.get("market_data", {})
    nested_market_fields = {
        "price_usd": ["current_price", "usd"],
        "price_btc": ["current_price", "btc"],
        "mcap_to_tvl_ratio": ["mcap_to_tvl_ratio"],
        "fdv_to_tvl_ratio": ["fdv_to_tvl_ratio"],
        "roi": ["roi"],
        "ath_usd": ["ath", "usd"],
        "ath_change_percentage_usd": ["ath_change_percentage", "usd"],
        "ath_date_usd": ["ath_date", "usd"],
        "atl_usd": ["atl", "usd"],
        "atl_change_percentage_usd": ["atl_change_percentage", "usd"],
        "atl_date_usd": ["atl_date", "usd"],
        "market_cap_usd": ["market_cap", "usd"],
        "market_cap_rank": ["market_cap_rank"],
        "fully_diluted_valuation_usd": ["fully_diluted_valuation", "usd"],
        "market_cap_fdv_ratio": ["market_cap_/_fully_diluted_valuation"],  # This field may or may not exist
        "total_volume_usd": ["total_volume", "usd"],
        "price_change_percentage_7d": ["price_change_percentage_7d"],
        "price_change_percentage_14d": ["price_change_percentage_14d"],
        "price_change_percentage_30d": ["price_change_percentage_30d"],
        "price_change_percentage_60d": ["price_change_percentage_60d"],
        "price_change_percentage_200d": ["price_change_percentage_200d"],
        "price_change_percentage_1y": ["price_change_percentage_1y"],
        "total_supply": ["total_supply"],
        "max_supply": ["max_supply"],
        "circulating_supply": ["circulating_supply"],
        "last_updated": ["last_updated"]
    }
    for new_field, path in nested_market_fields.items():
        value = market_data
        for key in path:
            if value is None:
                break
            value = value.get(key)
        # Ensure that invalid placeholders like '-' or None are handled
        if value == '-':
            value = None
        flat[new_field] = value

    # total_value_locked can sometimes be an object like {"usd": x, "btc": y}, or None
    total_value_locked = market_data.get("total_value_locked")
    if isinstance(total_value_locked, dict):
        flat["total_value_locked_usd"] = total_value_locked.get("usd")
        flat["total_value_locked_btc"] = total_value_locked.get("btc")
    else:
        # If it's not a dict or is None, store None
        flat["total_value_locked_usd"] = None
        flat["total_value_locked_btc"] = None

    community_data = coin.get("community_data", {})
    nested_community_fields = [
        "facebook_likes", "twitter_followers", "reddit_average_posts_48h",
        "reddit_average_comments_48h", "reddit_subscribers", 
        "reddit_accounts_active_48h", "telegram_channel_user_count"
    ]
    for field in nested_community_fields:
        value = community_data.get(field)
        if value == '-':
            value = None
        flat[field] = value

    developer_data = coin.get("developer_data", {})
    nested_developer_fields = [
        "forks", "stars", "subscribers", "total_issues", "closed_issues",
        "pull_requests_merged", "pull_request_contributors", "commit_count_4_weeks"
    ]
    for field in nested_developer_fields:
        value = developer_data.get(field)
        if value == '-':
            value = None
        flat[field] = value

    return flat

def preprocess_coin_data(coins):
    return [flatten_coin_data(record) for record in coins]

def validate_market_data_structure(data):
    required_keys = {"prices", "market_caps", "total_volumes"}
    actual_keys = set(data.keys())
    missing = required_keys - actual_keys
    if missing:
        logging.warning(f"Market chart data missing required keys: {missing}")

def validate_ohlc_data_structure(data):
    if not isinstance(data, list):
        logging.warning("OHLC data is not a list.")
        return
    for entry in data:
        if not (isinstance(entry, list) and len(entry) == 5):
            logging.warning(f"OHLC entry has invalid structure: {entry}")

def convert_columns_to_numeric(df: pd.DataFrame):
    """
    Safely convert the known numeric columns to numeric dtypes, ignoring errors.
    """
    for col in df.columns:
        if col in NUMERIC_COLUMNS:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def save_parquet(data, endpoint, batch_index):
    endpoint_dir = os.path.join(DATA_DIR, endpoint)
    os.makedirs(endpoint_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    file_path = os.path.join(endpoint_dir, f"batch_{batch_index}_{timestamp}.parquet")

    if data:
        # For the coins endpoint, flatten and then convert
        if endpoint == "coins":
            data = preprocess_coin_data(data)
        df = pd.DataFrame(data)

        # Convert columns that should be numeric
        df = convert_columns_to_numeric(df)

        # Fill NaNs in certain columns for safety
        fill_values = {
            "price": float("nan"),
            "open": float("nan"),
            "high": float("nan"),
            "low": float("nan"),
            "close": float("nan"),
            "token_id": ""
        }
        for col, val in fill_values.items():
            if col in df.columns:
                df[col] = df[col].fillna(val)

        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path)
        logging.info(f"Saved {endpoint} data for batch {batch_index} to {file_path}")
    else:
        logging.warning(f"No data to save for {endpoint} in batch {batch_index}")

def fetch_endpoint(url, params=None):
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            response = requests.get(url, headers=HEADERS, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            # If OHLC endpoint returns 422, skip further retries for OHLC
            if response.status_code == 422 and "ohlc" in url:
                logging.warning(f"OHLC data not available for {url}. Skipping.")
                return None
            logging.warning(f"Error fetching {url}: {e}. Retry {attempt + 1}/{MAX_RETRIES}")
            time.sleep(exponential_backoff(attempt))
            attempt += 1
        except requests.exceptions.RequestException as e:
            logging.warning(f"Error fetching {url}: {e}. Retry {attempt + 1}/{MAX_RETRIES}")
            time.sleep(exponential_backoff(attempt))
            attempt += 1
    logging.error(f"Exceeded max retries for {url}")
    return None

def fetch_data_for_token(token_id, start_timestamp, end_timestamp):
    data = {}
    coin_params = {
        "localization": "false",
        "tickers": "false",
        "market_data": "true",
        "community_data": "true",
        "developer_data": "true",
        "sparkline": "false"
    }
    common_params = {
        "vs_currency": "usd",
        "from": start_timestamp,
        "to": end_timestamp,
        "interval": "daily",
        "precision": "8"
    }
    endpoints = {
        "coins": f"{BASE_URL}/coins/{token_id}",
        "market_chart": f"{BASE_URL}/coins/{token_id}/market_chart/range",
        "ohlc": f"{BASE_URL}/coins/{token_id}/ohlc/range"
    }

    # Fetch coin data
    coin_data = fetch_endpoint(endpoints["coins"], params=coin_params)
    if coin_data:
        data["coin"] = coin_data
    else:
        log_failed_token(token_id, reason="Coin data could not be fetched.")

    # Fetch market chart data
    market_data = fetch_endpoint(endpoints["market_chart"], params=common_params)
    if market_data:
        validate_market_data_structure(market_data)
        data["market_chart"] = market_data
    else:
        log_failed_token(token_id, reason="Market chart data could not be fetched.")

    # Fetch OHLC data
    ohlc_params = {
        "vs_currency": "usd",
        "from": start_timestamp,
        "to": end_timestamp,
        "interval": "daily"
    }
    ohlc_data = fetch_endpoint(endpoints["ohlc"], params=ohlc_params)
    if ohlc_data is not None:
        validate_ohlc_data_structure(ohlc_data)
        data["ohlc"] = ohlc_data
    else:
        log_failed_token(token_id, reason="OHLC data could not be fetched or not available.")

    # Adjust sleep based on concurrency to respect rate limits
    time.sleep(THROTTLE_DELAY / MAX_WORKERS)
    return data

def process_token(token_id, start_timestamp, end_timestamp):
    return token_id, fetch_data_for_token(token_id, start_timestamp, end_timestamp)

def process_tokens_concurrently(token_ids, start_timestamp, end_timestamp):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_token = {
            executor.submit(process_token, token, start_timestamp, end_timestamp): token 
            for token in token_ids
        }
        for future in tqdm(concurrent.futures.as_completed(future_to_token), 
                           total=len(future_to_token), 
                           desc="Concurrent Processing"):
            token = future_to_token[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as exc:
                logging.error(f"Token {token} generated an exception: {exc}")
                log_failed_token(token, reason=str(exc))
    return results

def main():
    coin_list_url = f"{BASE_URL}/coins/list"
    all_coins = fetch_endpoint(coin_list_url)
    if not all_coins:
        logging.error("Could not retrieve the full list of coins.")
        return

    token_ids = [coin["id"] for coin in all_coins]
    logging.info(f"Total tokens to process: {len(token_ids)}")

    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())
    logging.info(f"Fetching data for the last 180 days: start={START_DATE}, end={END_DATE}")

    # Process tokens in batches
    for batch_index, i in enumerate(range(0, len(token_ids), BATCH_SIZE)):
        batch_tokens = token_ids[i:i + BATCH_SIZE]
        logging.info(f"Processing batch {batch_index}: tokens {i} to {i + len(batch_tokens)}")

        batch_results = process_tokens_concurrently(batch_tokens, start_timestamp, end_timestamp)
        
        coins_data, market_chart_data, ohlc_data = [], [], []
        for token, data in batch_results:
            if not data:
                continue
            # Process coin data
            if "coin" in data:
                coin_record = data["coin"]
                coin_record["token_id"] = token
                coins_data.append(coin_record)

            # Process market chart data
            if "market_chart" in data and "prices" in data["market_chart"]:
                mc = data["market_chart"]
                prices = mc.get("prices", [])
                volumes = mc.get("total_volumes", [])
                mcaps = mc.get("market_caps", [])
                for idx, price_record in enumerate(prices):
                    volume = (
                        volumes[idx][1] 
                        if (idx < len(volumes) and len(volumes[idx]) > 1) 
                        else None
                    )
                    mcap = (
                        mcaps[idx][1] 
                        if (idx < len(mcaps) and len(mcaps[idx]) > 1) 
                        else None
                    )
                    market_chart_data.append({
                        "token_id": token,
                        "timestamp": price_record[0],
                        "price": price_record[1],
                        "total_volume": volume,
                        "market_cap": mcap
                    })

            # Process OHLC data
            if "ohlc" in data and data["ohlc"]:
                for record in data["ohlc"]:
                    if isinstance(record, list) and len(record) == 5:
                        ohlc_data.append({
                            "token_id": token,
                            "timestamp": record[0],
                            "open": record[1],
                            "high": record[2],
                            "low": record[3],
                            "close": record[4]
                        })

        save_parquet(coins_data, "coins", batch_index)
        save_parquet(market_chart_data, "market_chart", batch_index)
        save_parquet(ohlc_data, "ohlc", batch_index)
        logging.info(f"Completed saving batch {batch_index}")

    logging.info("Completed processing all tokens.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(f"Unhandled exception: {e}")
