import os
import sys
import requests
import time
import logging
from datetime import datetime
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
    filename='./logs/block_headers.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def get_highest_block_height():
    try:
        response = supabase.rpc('get_highest_block_height_from_block_headers').execute()
        result = response.data
        return result if result is not None else -1  # Start from -1 if no records
    except Exception as e:
        logging.error(f"Error getting highest block height: {e}")
        raise

def get_block_count(rpc_url, auth_token=None):
    headers = {"Content-Type": "application/json"}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    payload = {
        "jsonrpc": "2.0",
        "id": "1",
        "method": "getblockcount",
        "params": []
    }

    try:
        response = requests.post(rpc_url, headers=headers, json=payload)
        response.raise_for_status()
        result = response.json().get('result')
        logging.info(f"Current block height is {result}.")
        return result
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch current block height: {e}")
        raise

def fetch_block_header(block_height, rpc_url, auth_token=None):
    headers = {"Content-Type": "application/json"}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    payload = {
        "jsonrpc": "2.0",
        "id": "getblockheader",
        "method": "getblockheader",
        "params": [block_height, True]  # True to get verbose output
    }

    retry_delay = 60

    while True:
        try:
            start_time = time.time()
            response = requests.post(rpc_url, headers=headers, json=payload)

            if response.status_code == 200:
                result = response.json().get('result')
                query_time = time.time() - start_time
                logging.info(f"Block header for height {block_height} fetched in {query_time:.2f} sec.")
                return result
            else:
                logging.error(f"Non-200 response code {response.status_code}. Retrying in {retry_delay} sec...")
                time.sleep(retry_delay)

        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch block header for height {block_height}: {e}. Retrying in {retry_delay} sec...")
            time.sleep(retry_delay)

def store_block_header(block_header):
    try:
        response = supabase.rpc('insert_block_header', {
            'block_height': block_header['height'],
            'block_hash': block_header['hash'],
            'confirmations': block_header['confirmations'],
            'version': block_header['version'],
            'version_hex': block_header['versionHex'],
            'merkleroot': block_header['merkleroot'],
            'block_time': block_header['time'],
            'mediantime': block_header['mediantime'],
            'nonce': block_header['nonce'],
            'bits': block_header['bits'],
            'difficulty': block_header['difficulty'],
            'chainwork': block_header['chainwork'],
            'n_tx': block_header['nTx'],
            'previous_block_hash': block_header.get('previousblockhash', ''),
            'next_block_hash': block_header.get('nextblockhash', '')
        }).execute()

        if response.data:
            logging.info(f"Block header for height {block_header['height']} stored in the database.")
            print(f"Block {block_header['height']} committed to the database", end='\r', flush=True)
        else:
            logging.warning(f"No data returned when inserting block header for height {block_header['height']}.")

    except Exception as e:
        logging.error(f"Error storing block header for height {block_header['height']}: {e}")
        raise

def collect_block_headers(rpc_url, auth_token=None):
    try:
        start_block = get_highest_block_height() + 1
        logging.info(f"Starting block header fetch from {start_block}")

        end_block = get_block_count(rpc_url, auth_token)

        for block_height in range(start_block, end_block + 1):
            block_header = fetch_block_header(block_height, rpc_url, auth_token)
            if block_header:
                store_block_header(block_header)
                logging.info(f"Block {block_height} header committed to the database.")

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

    collect_block_headers(rpc_url, auth_token)
