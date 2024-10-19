import os
import sys
import requests
import time
import logging
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables from .env file
load_dotenv()

# Fetch environment variables
RPC_URL = os.getenv("ANKR_RPC_URL")
RPC_API_KEY = os.getenv("ANKR_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# Set up logging
logging.basicConfig(
    filename='./logs/block_hashes.log',
    level=logging.INFO,  # Set to DEBUG level for detailed logs
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

    if auth_token:  # Add Authorization header if token exists
        headers["Authorization"] = f"Bearer {auth_token}"

    payload = {
        "jsonrpc": "2.0",
        "id": "1",
        "method": "getblockcount",
        "params": []
    }

    try:
        logging.debug(f"Sending request to {rpc_url} with headers: {headers}")
        logging.debug(f"Request payload: {payload}")

        response = requests.post(rpc_url, headers=headers, json=payload)
        response.raise_for_status()

        logging.debug(f"Response status code: {response.status_code}")
        logging.debug(f"Response body: {response.text}")

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

        if response.data:
            logging.info(f"Block hash for height {block_height} stored in the database.")
            print(f"Block {block_height} committed to the database", end='\r', flush=True)
        else:
            logging.warning(f"No data returned when inserting block hash for height {block_height}.")

    except Exception as e:
        logging.error(f"Error storing block hash for height {block_height}: {e}")
        raise

def fetch_block_hash(block_height, rpc_url, auth_token=None):
    headers = {
        "Content-Type": "application/json",
    }

    if auth_token:  # Add Authorization header if token exists
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

            # Log request details
            logging.debug(f"Sending request to {rpc_url} with headers: {headers}")
            logging.debug(f"Request payload: {payload}")

            response = requests.post(rpc_url, headers=headers, json=payload)

            # Log response details
            logging.debug(f"Response status code: {response.status_code}")
            logging.debug(f"Response body: {response.text}")

            if response.status_code == 200:
                result = response.json().get('result')
                query_time = time.time() - start_time
                logging.info(f"Block hash for height {block_height} fetched in {query_time:.2f} sec.")
                return result
            else:
                logging.error(f"Non-200 response code {response.status_code}. Retrying in {retry_delay} sec...")
                time.sleep(retry_delay)

        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch block hash for height {block_height}: {e}. Retrying in {retry_delay} sec...")
            time.sleep(retry_delay)

def collect_block_hashes(rpc_url, auth_token=None):
    try:
        start_block = get_highest_block_height() + 1
        logging.info(f"Starting block hash fetch from {start_block}")

        end_block = get_block_count(rpc_url, auth_token)

        for block_height in range(start_block, end_block + 1):
            block_hash = fetch_block_hash(block_height, rpc_url, auth_token)
            if block_hash:
                store_block_hash(block_height, block_hash)
                logging.info(f"Block hash for height {block_height} committed to the database.")

    except KeyboardInterrupt:
        logging.warning("Script interrupted by user.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        logging.info("Script execution completed.")

if __name__ == "__main__":
    rpc_url = RPC_URL
    auth_token = RPC_API_KEY if RPC_API_KEY else None  # Use None if no auth token is required

    # Set stdout to unbuffered
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)

    collect_block_hashes(rpc_url, auth_token)
