import os
import sys
import requests
import time
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
import json

# **DEBUG MODE VARIABLE**
DEBUG_MODE = False  # Set to True to enable detailed logging, False to disable

# Load environment variables from .env file
load_dotenv()

# Fetch environment variables
RPC_URL = os.getenv("ANKR_RPC_URL")
RPC_API_KEY = os.getenv("")
SUPABASE_URL = os.getenv("SUPABASE_PRODUCTION_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_PRODUCTION_SERVICE_KEY")

# Set up logging
log_level = logging.DEBUG if DEBUG_MODE else logging.INFO
logging.basicConfig(
    filename='./logs/block_hash.log',
    level=log_level,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def get_highest_block_height():
    try:
        response = supabase.rpc('get_highest_block_height_from_block_hashes').execute()
        result = response.data
        return result if result is not None else 0
    except Exception as e:
        logging.error(f"Error getting highest block height: {e}")
        raise

def get_block_count(rpc_url, auth_token=None):
    headers = {
        "Content-Type": "application/json",
    }

    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    payload = {
        "jsonrpc": "2.0",
        "id": "1",
        "method": "getblockcount",
        "params": []
    }

    try:
        if DEBUG_MODE:
            logging.debug(f"Attempting to fetch current block height from {rpc_url}")
            logging.debug(f"Request payload: {json.dumps(payload, indent=2)}")

        response = requests.post(rpc_url, headers=headers, json=payload)

        if DEBUG_MODE:
            logging.debug(f"Response status code: {response.status_code}")
            logging.debug(f"Response headers: {json.dumps(dict(response.headers), indent=2)}")
            logging.debug(f"Response body: {json.dumps(response.json(), indent=2)}")

        response.raise_for_status()

        result = response.json().get('result')
        logging.info(f"Current block height is {result}.")
        return result
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch current block height: {e}")
        raise

def store_block_hash(block_height, block_hash):
    try:
        response = supabase.rpc('insert_block_hash', {
            'block_height': block_height,
            'block_hash': block_hash
        }).execute()

        if response.error is None:
            logging.info(f"Block hash for height {block_height} stored in the database.")
            print(f"Block {block_height} committed to the database", flush=True)
        else:
            logging.error(f"Error storing block hash for height {block_height}: {response.error}")
            raise Exception(f"Supabase error: {response.error}")

    except Exception as e:
        logging.error(f"Error storing block hash for height {block_height}: {e}")
        raise

def fetch_block_hash(block_height, rpc_url, auth_token=None):
    headers = {
        "Content-Type": "application/json",
    }

    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    payload = {
        "jsonrpc": "2.0",
        "id": "1",
        "method": "getblockhash",
        "params": [block_height]
    }

    retry_delay = 60  # Set delay to wait before retrying (60 seconds)

    while True:  # Retry loop
        try:
            start_time = time.time()

            if DEBUG_MODE:
                logging.debug(f"Sending getblockhash request to {rpc_url} for block height {block_height}")
                logging.debug(f"Request payload: {json.dumps(payload, indent=2)}")

            response = requests.post(rpc_url, headers=headers, json=payload)

            if DEBUG_MODE:
                logging.debug(f"Response status code: {response.status_code}")
                logging.debug(f"Response headers: {json.dumps(dict(response.headers), indent=2)}")
                logging.debug(f"Response body: {json.dumps(response.json(), indent=2)}")

            response.raise_for_status()

            result = response.json().get('result')
            query_time = time.time() - start_time
            logging.info(f"Block hash for height {block_height} fetched in {query_time:.2f} sec.")
            return result

        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch block hash for height {block_height}: {e}")
            if DEBUG_MODE and e.response is not None:
                logging.debug(f"Response status code: {e.response.status_code}")
                logging.debug(f"Response headers: {json.dumps(dict(e.response.headers), indent=2)}")
                logging.debug(f"Response body: {e.response.text}")
            logging.error(f"Retrying in {retry_delay} sec...")
            time.sleep(retry_delay)

def collect_block_hashes(rpc_url, auth_token=None):
    try:
        start_block = get_highest_block_height() + 1
        logging.info(f"Starting block hash fetch from {start_block}")

        end_block = get_block_count(rpc_url, auth_token)
        logging.info(f"Current block height is {end_block}. Will fetch {end_block - start_block + 1} blocks.")

        if start_block > end_block:
            logging.warning(f"Start block ({start_block}) is greater than end block ({end_block}). No new blocks to fetch.")
            return

        for block_height in range(start_block, end_block + 1):
            logging.info(f"Attempting to fetch block hash for height {block_height}")
            try:
                block_hash = fetch_block_hash(block_height, rpc_url, auth_token)
                if block_hash:
                    logging.info(f"Attempting to store block hash for height {block_height}")
                    store_block_hash(block_height, block_hash)
                    logging.info(f"Block hash for height {block_height} successfully processed and stored.")
                else:
                    logging.error(f"Failed to fetch block hash for height {block_height}: Received empty result")
            except Exception as e:
                logging.error(f"Error processing block {block_height}: {e}")
                # Optionally, you might want to add a retry mechanism here

    except KeyboardInterrupt:
        logging.warning("Script interrupted by user.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        logging.info("Script execution completed.")

if __name__ == "__main__":
    rpc_url = RPC_URL
    auth_token = RPC_API_KEY if RPC_API_KEY else None

    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)

    collect_block_hashes(rpc_url, auth_token)
