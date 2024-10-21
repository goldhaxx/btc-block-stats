import subprocess
import sys
import os
import requests
from dotenv import load_dotenv
from tqdm import tqdm
from supabase import create_client, Client
import logging

# **DEBUG MODE VARIABLE**
DEBUG_MODE = False  # Set to True to enable detailed logging, False to disable

# Load environment variables
load_dotenv()

RPC_URL = os.getenv("ANKR_RPC_URL")
RPC_API_KEY = os.getenv("")
SUPABASE_URL = os.getenv("SUPABASE_PRODUCTION_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_PRODUCTION_SERVICE_KEY")

# Set up logging
log_level = logging.DEBUG if DEBUG_MODE else logging.INFO
logging.basicConfig(
    filename='./logs/run_all_scripts.log',
    level=log_level,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def get_current_block_height():
    headers = {"Content-Type": "application/json"}
    if RPC_API_KEY:
        headers["Authorization"] = f"Bearer {RPC_API_KEY}"

    payload = {
        "jsonrpc": "2.0",
        "id": "1",
        "method": "getblockcount",
        "params": []
    }

    try:
        if DEBUG_MODE:
            logging.debug(f"Attempting to fetch current block height from {RPC_URL}")
            logging.debug(f"Request payload: {payload}")
        
        response = requests.post(RPC_URL, headers=headers, json=payload)
        
        if DEBUG_MODE:
            logging.debug(f"Response status code: {response.status_code}")
            logging.debug(f"Response body: {response.text}")
        
        response.raise_for_status()
        result = response.json().get('result')
        logging.info(f"Current BTC network block height is {result}.")
        return result
    except Exception as e:
        logging.error(f"Failed to get current block height: {e}")
        raise

def get_db_block_height(table_name):
    rpc_function = f'get_highest_block_height_from_{table_name}'
    try:
        response = supabase.rpc(rpc_function).execute()
        result = response.data if response.data is not None else 0
        logging.info(f"Database block height for {table_name} is {result}.")
        return result
    except Exception as e:
        logging.error(f"Error getting database block height for {table_name}: {e}")
        raise

def run_script(script_name, current_height, db_height):
    print(f"\n--- Running {script_name} ---")
    blocks_to_process = current_height - db_height

    if blocks_to_process <= 0:
        print(f"{script_name} is up to date. No blocks to process.")
        return 0

    process = subprocess.Popen([sys.executable, script_name], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, universal_newlines=True)

    with tqdm(total=blocks_to_process, desc=f"{script_name} progress", unit="block", ncols=100) as pbar:
        last_block = db_height
        for line in iter(process.stdout.readline, ''):
            if line.startswith("PROGRESS:"):
                try:
                    current_block = int(line.split(":")[1])
                    blocks_processed = current_block - last_block
                    if blocks_processed > 0:
                        pbar.update(blocks_processed)
                        last_block = current_block
                except ValueError:
                    pass
            else:
                if DEBUG_MODE:
                    logging.debug(line.strip())
                print(line.strip(), end='\r')  # Print other output on the same line

    process.wait()
    print()  # Add a newline after the progress bar
    return process.returncode

def main():
    scripts = [
        ('block_stats.py', 'block_stats'),
        ('block_hash.py', 'block_hashes'),
        ('block_headers.py', 'block_headers')
    ]

    try:
        current_height = get_current_block_height()
        print(f"Current BTC network block height: {current_height}")

        for script, table in scripts:
            db_height = get_db_block_height(table)
            blocks_behind = current_height - db_height
            print(f"\n{table} is {blocks_behind} blocks behind the current network height.")

            exit_code = run_script(script, current_height, db_height)
            if exit_code != 0:
                print(f"\nError: {script} exited with code {exit_code}")
                break

        print("\nAll scripts executed.")
    except Exception as e:
        logging.error(f"An error occurred in run_all_scripts.py: {e}")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
