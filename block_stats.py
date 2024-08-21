import os
import requests
import psycopg2
import time
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Fetch environment variables
RPC_URL_1 = os.getenv("RPC_URL_1")
RPC_URL_1_API_KEY = os.getenv("RPC_URL_1_API_KEY")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Set up logging
logging.basicConfig(
    filename='./logs/block_stats.log',
    level=logging.INFO,  # Set to DEBUG for advanced logging
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

# Function to get the highest block height currently in the database
def get_highest_block_height(cursor):
    cursor.execute('SELECT MAX(block_height) FROM block_stats')
    result = cursor.fetchone()
    return result[0] if result[0] is not None else 0  # Start from 0 if no records

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

# Function to insert data into the PostgreSQL database
def store_block_stats(cursor, block_stats):
    try:
        cursor.execute('''
            INSERT INTO block_stats (
                block_height, total_fee, avg_fee_rate, txs, timestamp, ins, maxfee, maxfeerate, maxtxsize, 
                medianfee, mediantime, mediantxsize, minfee, minfeerate, mintxsize, outs, subsidy, 
                swtotal_size, swtotal_weight, swtxs, total_out, total_size, total_weight, 
                utxo_increase, utxo_size_inc, utxo_increase_actual, utxo_size_inc_actual
            ) VALUES (%s, %s, %s, %s, TO_TIMESTAMP(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (block_height) DO NOTHING
        ''', (
            block_stats['block_height'],
            block_stats['total_fee'],
            block_stats['avg_fee_rate'],
            block_stats['txs'],
            block_stats['timestamp'],  # Store timestamp as TO_TIMESTAMP (TIMESTAMP)
            block_stats['ins'],
            block_stats['maxfee'],
            block_stats['maxfeerate'],
            block_stats['maxtxsize'],
            block_stats['medianfee'],
            block_stats['mediantime'],  # Store mediantime as UNIX timestamp (BIGINT)
            block_stats['mediantxsize'],
            block_stats['minfee'],
            block_stats['minfeerate'],
            block_stats['mintxsize'],
            block_stats['outs'],
            block_stats['subsidy'],
            block_stats['swtotal_size'],
            block_stats['swtotal_weight'],
            block_stats['swtxs'],
            block_stats['total_out'],
            block_stats['total_size'],
            block_stats['total_weight'],
            block_stats['utxo_increase'],
            block_stats['utxo_size_inc'],
            block_stats['utxo_increase_actual'],
            block_stats['utxo_size_inc_actual']
        ))
        logging.info(f"Block {block_stats['block_height']} stored in the database.")
    except psycopg2.DatabaseError as e:
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

            logging.debug(f"Sending request to {rpc_url} with headers: {headers}")
            logging.debug(f"Request payload: {payload}")

            response = requests.post(rpc_url, headers=headers, json=payload)

            logging.debug(f"Response status code: {response.status_code}")
            logging.debug(f"Response body: {response.text}")

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
    conn = create_connection()
    cursor = conn.cursor()

    try:
        # Get the highest block height in the database
        start_block = get_highest_block_height(cursor) + 1
        logging.info(f"Starting block fetch from {start_block}")

        # Get the latest block height to use as the ceiling
        end_block = get_block_count(rpc_url, auth_token)

        for block_height in range(start_block, end_block + 1):
            block_stats = fetch_block_stats(block_height, rpc_url, auth_token)
            if block_stats:
                store_block_stats(cursor, block_stats)
                conn.commit()
                logging.info(f"Block {block_height} committed to the database.")

            # Sleep to avoid rate limiting or overloading the server
            time.sleep(0.1)

    except KeyboardInterrupt:
        logging.warning("Script interrupted by user.")
    finally:
        cursor.close()
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    rpc_url = RPC_URL_1
    auth_token = RPC_URL_1_API_KEY if RPC_URL_1_API_KEY else None  # Use None if no auth token is required

    collect_block_data(rpc_url, auth_token)
