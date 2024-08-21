import os
import requests
import psycopg2
import time
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Fetch environment variables
RPC_URL_2 = os.getenv("RPC_URL_2")
RPC_URL_2_API_KEY = os.getenv("RPC_URL_2_API_KEY")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Set up logging
logging.basicConfig(
    filename='./logs/block_hashes.log',
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

# Function to store block hashes in the PostgreSQL database
def store_block_hash(cursor, block_height, block_hash):
    try:
        cursor.execute('''
            INSERT INTO block_hashes (block_height, block_hash)
            VALUES (%s, %s)
            ON CONFLICT (block_height) DO NOTHING
        ''', (block_height, block_hash))
        logging.info(f"Block hash for height {block_height} stored in the database.")
    except psycopg2.DatabaseError as e:
        logging.error(f"Error storing block hash for height {block_height}: {e}")
        raise

# Function to fetch block hashes from the Bitcoin RPC API with advanced logging
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

# Function to get the highest block height currently in the block_hashes table
def get_highest_block_height(cursor):
    cursor.execute('SELECT MAX(block_height) FROM block_hashes')
    result = cursor.fetchone()
    return result[0] if result[0] is not None else 0  # Start from 0 if no records

# Function to get the current BTC block height with advanced logging
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

# Main function to pull block hashes and store them
def collect_block_hashes(rpc_url, auth_token=None):
    conn = create_connection()
    cursor = conn.cursor()

    try:
        # Get the highest block height in the database to start from the next block
        start_block = get_highest_block_height(cursor) + 1
        logging.info(f"Starting block hash fetch from {start_block}")

        # Get the latest block height to use as the ceiling
        end_block = get_block_count(rpc_url, auth_token)

        for block_height in range(start_block, end_block + 1):
            block_hash = fetch_block_hash(block_height, rpc_url, auth_token)
            if block_hash:
                store_block_hash(cursor, block_height, block_hash)
                conn.commit()
                logging.info(f"Block hash for height {block_height} committed to the database.")

            # Sleep to avoid rate limiting or overloading the server
            time.sleep(0.1)

    except KeyboardInterrupt:
        logging.warning("Script interrupted by user.")
    finally:
        cursor.close()
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    rpc_url = RPC_URL_2
    auth_token = RPC_URL_2_API_KEY if RPC_URL_2_API_KEY else None  # Use None if no auth token is required

    collect_block_hashes(rpc_url, auth_token)
