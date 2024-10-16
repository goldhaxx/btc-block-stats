import subprocess
import sys
import os
import requests
import psycopg2
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

RPC_URL = os.getenv("ANKR_RPC_URL")
RPC_API_KEY = os.getenv("ANKR_API_KEY")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

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

    response = requests.post(RPC_URL, headers=headers, json=payload)
    if response.status_code == 200:
        return response.json().get('result')
    else:
        raise Exception(f"Failed to get current block height: {response.text}")

def get_db_block_height(table_name):
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)
    cursor = conn.cursor()
    cursor.execute(f'SELECT MAX(block_height) FROM {table_name}')
    result = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return result if result is not None else 0

def run_script(script_name, current_height, db_height):
    print(f"\n--- Running {script_name} ---")
    blocks_to_process = current_height - db_height
    
    process = subprocess.Popen([sys.executable, script_name], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, universal_newlines=True)
    
    with tqdm(total=blocks_to_process, desc=f"{script_name} progress", unit="block", ncols=100) as pbar:
        last_block = db_height
        for line in iter(process.stdout.readline, ''):
            if "Block" in line and "committed to the database" in line:
                try:
                    current_block = int(line.split("Block")[1].split("committed")[0].strip())
                    blocks_processed = current_block - last_block
                    pbar.update(blocks_processed)
                    last_block = current_block
                except ValueError:
                    pass
            else:
                print(line.strip(), end='\r')
    
    process.wait()
    print()  # Add a newline after the progress bar
    return process.returncode

def main():
    scripts = [
        ('block_stats.py', 'block_stats'),
        ('block_hash.py', 'block_hashes'),
        ('block_headers.py', 'block_headers')
    ]
    
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

if __name__ == "__main__":
    main()
