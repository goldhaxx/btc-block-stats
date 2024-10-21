import os
import sys
import requests
import time
import logging
from datetime import datetime
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
    filename='./logs/block_headers.log',
    level=log_level,
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

def fetch_block_header(block_height, rpc_url, auth_token=None):
    # First, try to get the block hash from Supabase
    block_hash = get_block_hash_from_supabase(block_height)
    
    # If not found in Supabase, try BTC RPC
    if not block_hash:
        block_hash = get_block_hash_from_btc_rpc(block_height, rpc_url, auth_token)
    
    if not block_hash:
        logging.error(f"Failed to get block hash for height {block_height}")
        return None

    headers = {"Content-Type": "application/json"}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    payload = {
        "jsonrpc": "2.0",
        "id": "getblockheader",
        "method": "getblockheader",
        "params": [block_hash, True]  # True to get verbose output
    }

    retry_delay = 60

    while True:
        try:
            start_time = time.time()
            if DEBUG_MODE:
                logging.debug(f"Sending request to BTC RPC for block hash {block_hash}")
                logging.debug(f"Request payload: {json.dumps(payload, indent=2)}")
            
            response = requests.post(rpc_url, headers=headers, json=payload)
            
            if DEBUG_MODE:
                logging.debug(f"Response status code: {response.status_code}")
                logging.debug(f"Response headers: {json.dumps(dict(response.headers), indent=2)}")
                logging.debug(f"Response body: {json.dumps(response.json(), indent=2)}")
            
            response.raise_for_status()

            response_json = response.json()
            result = response_json.get('result')
            if result:
                query_time = time.time() - start_time
                logging.info(f"Block header for height {block_height} fetched successfully in {query_time:.2f} sec.")
                return result
            else:
                logging.warning(f"Empty result received for block height {block_height}. Response: {json.dumps(response_json, indent=2)}")
                logging.warning(f"Retrying in {retry_delay} sec...")
                time.sleep(retry_delay)

        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch block header for height {block_height}: {e}")
            if DEBUG_MODE and e.response is not None:
                logging.debug(f"Response status code: {e.response.status_code}")
                logging.debug(f"Response headers: {json.dumps(dict(e.response.headers), indent=2)}")
                logging.debug(f"Response body: {e.response.text}")
            logging.error(f"Retrying in {retry_delay} sec...")
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
            'block_time': block_header['time'],  # This matches the SQL function parameter name
            'mediantime': block_header['mediantime'],
            'nonce': block_header['nonce'],
            'bits': block_header['bits'],
            'difficulty': block_header['difficulty'],
            'chainwork': block_header['chainwork'],
            'n_tx': block_header['nTx'],
            'previous_block_hash': block_header.get('previousblockhash', ''),
            'next_block_hash': block_header.get('nextblockhash', '')
        }).execute()

        if response.error is None:
            logging.info(f"Block header for height {block_header['height']} stored in the database.")
            print(f"Block {block_header['height']} committed to the database", flush=True)
        else:
            logging.error(f"Error storing block header for height {block_header['height']}: {response.error}")
            raise Exception(f"Supabase error: {response.error}")

    except Exception as e:
        logging.error(f"Error storing block header for height {block_header['height']}: {e}")
        raise

def collect_block_headers(rpc_url, auth_token=None):
    try:
        start_block = get_highest_block_height() + 1
        logging.info(f"Starting block header fetch from {start_block}")

        end_block = get_block_count(rpc_url, auth_token)
        logging.info(f"Current block height is {end_block}. Will fetch {end_block - start_block + 1} blocks.")

        if start_block > end_block:
            logging.warning(f"Start block ({start_block}) is greater than end block ({end_block}). No new blocks to fetch.")
            return

        for block_height in range(start_block, end_block + 1):
            logging.info(f"Attempting to fetch block header for height {block_height}")
            try:
                block_header = fetch_block_header(block_height, rpc_url, auth_token)
                if block_header:
                    logging.info(f"Attempting to store block header for height {block_height}")
                    store_block_header(block_header)
                    logging.info(f"Block {block_height} header successfully processed and stored.")
                else:
                    logging.error(f"Failed to fetch block header for height {block_height}: Received empty result")
            except Exception as e:
                logging.error(f"Error processing block {block_height}: {e}")
                # Optionally, you might want to add a retry mechanism here

    except KeyboardInterrupt:
        logging.warning("Script interrupted by user.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        logging.info("Script execution completed.")

def get_block_hash_from_supabase(block_height):
    try:
        response = supabase.rpc('get_block_hash', {'in_block_height': str(block_height)}).execute()
        if DEBUG_MODE:
            logging.debug(f"Supabase get_block_hash response: {response}")
        if response.data:
            return response.data
        return None
    except Exception as e:
        logging.error(f"Error getting block hash from Supabase: {e}")
        return None

def get_block_hash_from_btc_rpc(block_height, rpc_url, auth_token):
    headers = {"Content-Type": "application/json"}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    payload = {
        "jsonrpc": "2.0",
        "id": "1",
        "method": "getblockhash",
        "params": [block_height]
    }

    try:
        if DEBUG_MODE:
            logging.debug(f"Sending getblockhash request to RPC for block height {block_height}")
            logging.debug(f"Request payload: {json.dumps(payload, indent=2)}")
        
        response = requests.post(rpc_url, headers=headers, json=payload)
        
        if DEBUG_MODE:
            logging.debug(f"Response status code: {response.status_code}")
            logging.debug(f"Response headers: {json.dumps(dict(response.headers), indent=2)}")
            logging.debug(f"Response body: {json.dumps(response.json(), indent=2)}")
        
        response.raise_for_status()
        result = response.json().get('result')
        logging.info(f"Block hash for height {block_height} is {result}")
        return result
    except Exception as e:
        logging.error(f"Error getting block hash from BTC RPC: {e}")
        return None

if __name__ == "__main__":
    rpc_url = RPC_URL
    auth_token = RPC_API_KEY if RPC_API_KEY else None

    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)

    collect_block_headers(rpc_url, auth_token)
