import os
import sys
import requests
import time
import logging
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv()

# Fetch environment variables
RPC_URL = os.getenv("ANKR_RPC_URL")
RPC_API_KEY = os.getenv("ANKR_API_KEY")
SUPABASE_URL = "https://nzeegmjntzmihrgsdvqm.supabase.co"
SUPABASE_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im56ZWVnbWpudHptaWhyZ3NkdnFtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Mjg0NDM5NTYsImV4cCI6MjA0NDAxOTk1Nn0.J9o9efFs81RdHxeUkrWaFsY32Rrp7hLM-DyqZXzI1f8"

# Set up logging
logging.basicConfig(
    filename='./logs/audit_and_update_block_headers.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)

def get_missing_block_headers():
    try:
        response = supabase.rpc('audit_missing_block_heights').execute()
        if response.data:
            for item in response.data:
                if item['table_name'] == 'block_headers':
                    return [int(h.strip()) for h in item['missing_block_heights'].split(',')]
        return []
    except Exception as e:
        logging.error(f"Error fetching missing block headers: {e}")
        raise

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
            print(f"Block {block_header['height']} committed to the database", end='\r', flush=True)
        else:
            logging.warning(f"No data returned when inserting block header for height {block_header['height']}.")

    except Exception as e:
        logging.error(f"Error storing block header for height {block_header['height']}: {e}")
        raise

def audit_and_update_block_headers(rpc_url, auth_token=None):
    try:
        missing_heights = get_missing_block_headers()
        total_missing = len(missing_heights)
        logging.info(f"Found {total_missing} missing block headers.")
        print(f"Found {total_missing} missing block headers.")

        for i, height in enumerate(missing_heights, 1):
            block_header = fetch_block_header(height, rpc_url, auth_token)
            if block_header:
                store_block_header(block_header)
                logging.info(f"Processed and stored block header {i}/{total_missing} - Block {height}")
                print(f"Processed {i}/{total_missing} - Block {height}", end='\r', flush=True)

        print("\nAll missing block headers processed.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    rpc_url = RPC_URL
    auth_token = RPC_API_KEY if RPC_API_KEY else None

    # Set stdout to unbuffered
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)

    audit_and_update_block_headers(rpc_url, auth_token)
