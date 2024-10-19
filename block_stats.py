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
    filename='./logs/block_stats.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# Function to get the highest block height currently in the database
def get_highest_block_height():
    try:
        response = supabase.rpc('get_highest_block_height_from_block_stats').execute()
        result = response.data
        return result if result is not None else 0  # Start from 0 if no records
    except Exception as e:
        logging.error(f"Error getting highest block height: {e}")
        raise

# Function to get the current BTC block height
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

# Function to insert data into the database using Supabase RPC
def store_block_stats(block_stats):
    try:
        response = supabase.rpc('insert_block_stat', {
            'block_height': block_stats['block_height'],
            'total_fee': block_stats['total_fee'],
            'avg_fee_rate': block_stats['avg_fee_rate'],
            'txs': block_stats['txs'],
            'block_timestamp': datetime.fromtimestamp(block_stats['timestamp']).isoformat(),
            'ins': block_stats['ins'],
            'maxfee': block_stats['maxfee'],
            'maxfeerate': block_stats['maxfeerate'],
            'maxtxsize': block_stats['maxtxsize'],
            'medianfee': block_stats['medianfee'],
            'mediantime': block_stats['mediantime'],
            'mediantxsize': block_stats['mediantxsize'],
            'minfee': block_stats['minfee'],
            'minfeerate': block_stats['minfeerate'],
            'mintxsize': block_stats['mintxsize'],
            'outs': block_stats['outs'],
            'subsidy': block_stats['subsidy'],
            'swtotal_size': block_stats['swtotal_size'],
            'swtotal_weight': block_stats['swtotal_weight'],
            'swtxs': block_stats['swtxs'],
            'total_out': block_stats['total_out'],
            'total_size': block_stats['total_size'],
            'total_weight': block_stats['total_weight'],
            'utxo_increase': block_stats['utxo_increase'],
            'utxo_size_inc': block_stats['utxo_size_inc'],
            'utxo_increase_actual': block_stats['utxo_increase_actual'],
            'utxo_size_inc_actual': block_stats['utxo_size_inc_actual']
        }).execute()

        if response.data:
            logging.info(f"Block {block_stats['block_height']} stored in the database.")
            print(f"Block {block_stats['block_height']} committed to the database", end='\r', flush=True)
        else:
            logging.warning(f"No data returned when inserting block {block_stats['block_height']}.")

    except Exception as e:
        logging.error(f"Error storing block {block_stats['block_height']} in database: {e}")
        raise

# API interaction with retry logic and advanced logging
def fetch_block_stats(block_height, rpc_url, auth_token=None):
    headers = {"Content-Type": "application/json"}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    payload = {
        "jsonrpc": "2.0",
        "id": "getblockstats",
        "method": "getblockstats",
        "params": [block_height]
    }

    retry_delay = 60  # Set delay to wait before retrying (60 seconds)

    while True:  # Retry loop
        try:
            start_time = time.time()
            response = requests.post(rpc_url, headers=headers, json=payload)

            if response.status_code == 200:
                result = response.json().get('result')
                query_time = time.time() - start_time
                logging.info(f"Block {block_height} fetched in {query_time:.2f} sec.")
                return {
                    'block_height': block_height,
                    'total_fee': result.get('totalfee'),
                    'avg_fee_rate': result.get('avgfeerate'),
                    'txs': result.get('txs'),
                    'timestamp': result.get('time'),
                    'ins': result.get('ins'),
                    'maxfee': result.get('maxfee'),
                    'maxfeerate': result.get('maxfeerate'),
                    'maxtxsize': result.get('maxtxsize'),
                    'medianfee': result.get('medianfee'),
                    'mediantime': result.get('mediantime'),
                    'mediantxsize': result.get('mediantxsize'),
                    'minfee': result.get('minfee'),
                    'minfeerate': result.get('minfeerate'),
                    'mintxsize': result.get('mintxsize'),
                    'outs': result.get('outs'),
                    'subsidy': result.get('subsidy'),
                    'swtotal_size': result.get('swtotal_size'),
                    'swtotal_weight': result.get('swtotal_weight'),
                    'swtxs': result.get('swtxs'),
                    'total_out': result.get('total_out'),
                    'total_size': result.get('total_size'),
                    'total_weight': result.get('total_weight'),
                    'utxo_increase': result.get('utxo_increase'),
                    'utxo_size_inc': result.get('utxo_size_inc'),
                    'utxo_increase_actual': result.get('utxo_increase_actual'),
                    'utxo_size_inc_actual': result.get('utxo_size_inc_actual')
                }
            else:
                logging.error(f"Non-200 response code {response.status_code}. Retrying in {retry_delay} sec...")
                time.sleep(retry_delay)

        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch block {block_height}: {e}. Retrying in {retry_delay} sec...")
            time.sleep(retry_delay)

# Main function to pull data from the highest block in the database to the latest block
def collect_block_data(rpc_url, auth_token=None):
    try:
        # Get the highest block height in the database
        start_block = get_highest_block_height() + 1
        logging.info(f"Starting block fetch from {start_block}")

        # Get the latest block height to use as the ceiling
        end_block = get_block_count(rpc_url, auth_token)

        for block_height in range(start_block, end_block + 1):
            block_stats = fetch_block_stats(block_height, rpc_url, auth_token)
            if block_stats:
                store_block_stats(block_stats)
                logging.info(f"Block {block_height} committed to the database.")

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

    collect_block_data(rpc_url, auth_token)
