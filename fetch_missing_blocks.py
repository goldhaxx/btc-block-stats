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
    filename='./logs/fetch_missing_blocks.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def get_missing_blocks():
    try:
        response = supabase.rpc('audit_missing_block_heights').execute()
        if response.data:
            return response.data
        else:
            logging.warning("No missing blocks found or empty response from audit_missing_block_heights")
            return []
    except Exception as e:
        logging.error(f"Error fetching missing blocks: {e}")
        raise

def fetch_block_hash(block_height, rpc_url, auth_token=None):
    headers = {"Content-Type": "application/json"}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    payload = {
        "jsonrpc": "2.0",
        "id": "1",
        "method": "getblockhash",
        "params": [block_height]
    }

    retry_delay = 60

    while True:
        try:
            start_time = time.time()
            response = requests.post(rpc_url, headers=headers, json=payload)

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

def fetch_block_header(block_height, rpc_url, auth_token=None):
    headers = {"Content-Type": "application/json"}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    payload = {
        "jsonrpc": "2.0",
        "id": "getblockheader",
        "method": "getblockhash",
        "params": [block_height]
    }

    retry_delay = 60

    while True:
        try:
            start_time = time.time()
            response = requests.post(rpc_url, headers=headers, json=payload)

            if response.status_code == 200:
                block_hash = response.json().get('result')
                
                # Now fetch the block header using the hash
                header_payload = {
                    "jsonrpc": "2.0",
                    "id": "getblockheader",
                    "method": "getblockheader",
                    "params": [block_hash, True]
                }
                header_response = requests.post(rpc_url, headers=headers, json=header_payload)
                
                if header_response.status_code == 200:
                    result = header_response.json().get('result')
                    query_time = time.time() - start_time
                    logging.info(f"Block header for height {block_height} fetched in {query_time:.2f} sec.")
                    return result
                else:
                    logging.error(f"Non-200 response code {header_response.status_code} for getblockheader. Retrying in {retry_delay} sec...")
                    time.sleep(retry_delay)
            else:
                logging.error(f"Non-200 response code {response.status_code} for getblockhash. Retrying in {retry_delay} sec...")
                time.sleep(retry_delay)

        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch block header for height {block_height}: {e}. Retrying in {retry_delay} sec...")
            time.sleep(retry_delay)

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

    retry_delay = 60

    while True:
        try:
            start_time = time.time()
            response = requests.post(rpc_url, headers=headers, json=payload)

            if response.status_code == 200:
                result = response.json().get('result')
                query_time = time.time() - start_time
                logging.info(f"Block stats for height {block_height} fetched in {query_time:.2f} sec.")
                return result
            else:
                logging.error(f"Non-200 response code {response.status_code}. Retrying in {retry_delay} sec...")
                time.sleep(retry_delay)

        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch block stats for height {block_height}: {e}. Retrying in {retry_delay} sec...")
            time.sleep(retry_delay)

def store_block_hash(block_height, block_hash):
    try:
        response = supabase.rpc('insert_block_hash', {
            'block_height': block_height,
            'block_hash': block_hash
        }).execute()

        if response.data is not None:
            logging.info(f"Block hash for height {block_height} stored in the database.")
            print(f"Block hash {block_height} committed to the database", end='\r', flush=True)
        else:
            logging.warning(f"No data returned when inserting block hash for height {block_height}.")

    except Exception as e:
        logging.error(f"Error storing block hash for height {block_height}: {e}")
        raise

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

        if response.data is not None:
            logging.info(f"Block header for height {block_header['height']} stored in the database.")
            print(f"Block header {block_header['height']} committed to the database", end='\r', flush=True)
        else:
            logging.warning(f"No data returned when inserting block header for height {block_header['height']}.")

    except Exception as e:
        logging.error(f"Error storing block header for height {block_header['height']}: {e}")
        raise

def store_block_stats(block_stats):
    try:
        response = supabase.rpc('insert_block_stat', {
            'block_height': block_stats['height'],
            'total_fee': block_stats['totalfee'],
            'avg_fee_rate': block_stats['avgfeerate'],
            'txs': block_stats['txs'],
            'block_timestamp': datetime.fromtimestamp(block_stats['time']).isoformat(),
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
            'utxo_increase_actual': block_stats.get('utxo_increase_actual', 0),
            'utxo_size_inc_actual': block_stats.get('utxo_size_inc_actual', 0)
        }).execute()

        if response.data is not None:
            logging.info(f"Block stats for height {block_stats['height']} stored in the database.")
            print(f"Block stats {block_stats['height']} committed to the database", end='\r', flush=True)
        else:
            logging.warning(f"No data returned when inserting block stats for height {block_stats['height']}.")

    except Exception as e:
        logging.error(f"Error storing block stats for height {block_stats['height']}: {e}")
        raise

def process_missing_blocks(rpc_url, auth_token=None):
    try:
        missing_blocks = get_missing_blocks()
        
        for table_info in missing_blocks:
            table_name = table_info['table_name']
            missing_block_heights = table_info['missing_block_heights']
            
            if missing_block_heights:
                block_heights = [int(h.strip()) for h in missing_block_heights.split(',')]
                total_missing = len(block_heights)
                logging.info(f"Found {total_missing} missing blocks for {table_name}.")
                print(f"Found {total_missing} missing blocks for {table_name}.")
                
                for i, height in enumerate(block_heights, 1):
                    if table_name == 'block_hashes':
                        block_hash = fetch_block_hash(height, rpc_url, auth_token)
                        if block_hash:
                            store_block_hash(height, block_hash)
                    elif table_name == 'block_headers':
                        block_header = fetch_block_header(height, rpc_url, auth_token)
                        if block_header:
                            store_block_header(block_header)
                    elif table_name == 'block_stats':
                        block_stats = fetch_block_stats(height, rpc_url, auth_token)
                        if block_stats:
                            store_block_stats(block_stats)
                    else:
                        logging.warning(f"Unknown table name: {table_name}")
                    
                    logging.info(f"Processed and stored {table_name} {i}/{total_missing} - Block {height}")
                    print(f"Processed {i}/{total_missing} - Block {height}", end='\r', flush=True)
                
                print(f"\nAll missing {table_name} processed.")
            else:
                logging.info(f"No missing blocks for {table_name}")
        
        print("\nAll missing blocks processed.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    rpc_url = RPC_URL
    auth_token = RPC_API_KEY if RPC_API_KEY else None

    # Set stdout to unbuffered
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)

    process_missing_blocks(rpc_url, auth_token)
