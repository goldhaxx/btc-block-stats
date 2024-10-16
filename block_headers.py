import os
import sys
import requests
import psycopg2
import time
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Fetch environment variables
RPC_URL = os.getenv("ANKR_RPC_URL")  # Can be changed to any RPC_URL in .env file
AUTH_TOKEN = os.getenv("ANKR_API_KEY", None)  # Get the auth token (if any)
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Set up logging
logging.basicConfig(
    filename='./logs/block_headers.log',
    level=logging.INFO,  # Set to DEBUG level for detailed logs
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Database connection (PostgreSQL)
def create_connection():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port="5432"
        )
        logging.info("Database connection established.")
        return conn
    except psycopg2.DatabaseError as e:
        logging.error(f"Error connecting to the database: {e}")
        raise

# Function to get the highest block height currently in the block_headers table
def get_highest_block_height(cursor):
    cursor.execute('SELECT MAX(block_height) FROM block_headers')
    result = cursor.fetchone()
    return result[0] if result[0] is not None else 0  # Start from 0 if no records

# Function to get the block hash from the block_hashes table
def get_block_hash(cursor, block_height):
    cursor.execute('SELECT block_hash FROM block_hashes WHERE block_height = %s', (block_height,))
    result = cursor.fetchone()
    return result[0] if result else None

# Function to insert data into the block_headers table
def store_block_headers(cursor, block_headers):
    try:
        cursor.execute('''
            INSERT INTO block_headers (
                block_height, block_hash, confirmations, version, version_hex, merkleroot, 
                time, mediantime, nonce, bits, difficulty, chainwork, n_tx, 
                previous_block_hash, next_block_hash
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (block_height) DO NOTHING
        ''', (
            block_headers['block_height'],
            block_headers['block_hash'],
            block_headers['confirmations'],
            block_headers['version'],
            block_headers['version_hex'],
            block_headers['merkleroot'],
            block_headers['time'],         # Store as BIGINT (no TO_TIMESTAMP)
            block_headers['mediantime'],   # Store as BIGINT (no TO_TIMESTAMP)
            block_headers['nonce'],
            block_headers['bits'],
            block_headers['difficulty'],
            block_headers['chainwork'],
            block_headers['n_tx'],
            block_headers['previousblockhash'],
            block_headers['nextblockhash']
        ))
        logging.info(f"Block {block_headers['block_height']} header stored in the database.")
        print(f"Block {block_headers['block_height']} committed to the database", end='\r', flush=True)
    except psycopg2.DatabaseError as e:
        logging.error(f"Error storing block {block_headers['block_height']} in database: {e}")
        raise

# API interaction with advanced logging for request and response
def fetch_block_header(block_hash, rpc_url, auth_token=None):
    headers = {
        "Content-Type": "application/json",
    }

    if auth_token:  # Add Authorization header if token exists
        headers["Authorization"] = f"Bearer {auth_token}"

    payload = {
        "jsonrpc": "2.0",
        "id": "getblockheader",
        "method": "getblockheader",
        "params": [block_hash]
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
                logging.info(f"Block header for hash {block_hash} fetched in {query_time:.2f} sec.")
                return {
                    'block_hash': block_hash,
                    'confirmations': result.get('confirmations'),
                    'version': result.get('version'),
                    'version_hex': result.get('versionHex'),
                    'merkleroot': result.get('merkleroot'),
                    'time': result.get('time'),
                    'mediantime': result.get('mediantime'),
                    'nonce': result.get('nonce'),
                    'bits': result.get('bits'),
                    'difficulty': result.get('difficulty'),
                    'chainwork': result.get('chainwork'),
                    'n_tx': result.get('nTx'),
                    'previousblockhash': result.get('previousblockhash'),
                    'nextblockhash': result.get('nextblockhash')
                }
            else:
                logging.error(f"Non-200 response code {response.status_code}. Retrying in {retry_delay} sec...")
                time.sleep(retry_delay)

        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch block header for hash {block_hash}: {e}. Retrying in {retry_delay} sec...")
            time.sleep(retry_delay)

# Main function to pull block headers from the highest block in the database to the latest block
def collect_block_headers(rpc_url, auth_token=None):
    conn = create_connection()
    cursor = conn.cursor()

    try:
        # Get the highest block height in the block_headers table
        start_block = get_highest_block_height(cursor) + 1
        logging.info(f"Starting block header fetch from {start_block}")

        while True:
            # Fetch the block hash from block_hashes table for the current block height
            block_hash = get_block_hash(cursor, start_block)
            if block_hash:
                block_header = fetch_block_header(block_hash, rpc_url, auth_token)
                if block_header:
                    block_header['block_height'] = start_block  # Add block_height to the header data
                    store_block_headers(cursor, block_header)
                    conn.commit()
                    logging.info(f"Block {start_block} header committed to the database.")
                start_block += 1
            else:
                logging.info(f"No block hash found for block height {start_block}. Stopping process.")
                break  # Stop the loop if no more block hashes are available

            # Sleep to avoid rate limiting or overloading the server
            # time.sleep(0.1)

    except KeyboardInterrupt:
        logging.warning("Script interrupted by user.")
    finally:
        cursor.close()
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    rpc_url = RPC_URL
    auth_token = AUTH_TOKEN if AUTH_TOKEN else None  # Use None if no auth token is required

    # Set stdout to unbuffered
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)

    collect_block_headers(rpc_url, auth_token)
