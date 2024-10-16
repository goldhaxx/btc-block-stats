# Bitcoin Block Data Analysis

This project is designed to collect, store, and analyze Bitcoin blockchain data, focusing on block statistics and headers. It leverages Python scripts to interact with the Bitcoin network via RPC calls, stores data in a PostgreSQL database, and provides tools for data visualization using Jupyter Notebooks.

## Table of Contents
- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Database Schema](#database-schema)
- [Scripts Description](#scripts-description)
- [Data Visualization](#data-visualization)
- [Docker Configuration](#docker-configuration)
- [Contributing](#contributing)
- [License](#license)

## Project Overview
The goal of this project is to collect detailed block statistics from the Bitcoin blockchain, store them in a PostgreSQL database, and perform analysis to gain insights into the network's behavior over time.

## Architecture
The project consists of the following components:
- **Data Collection Scripts**: Python scripts that interact with the Bitcoin RPC API to fetch block data.
- **Database**: A PostgreSQL database to store the collected data.
- **Data Visualization**: Jupyter Notebooks for analyzing and visualizing the data.
- **Docker**: Docker Compose configuration to set up the database and other services.

## High-Level Architecture
```sql
+-------------------+        +------------------------+
|                   |        |                        |
|  Bitcoin Network  +-------->  Data Collection Scripts|
|                   |        |                        |
+-------------------+        +-----------+------------+
                                   |
                                   |
                                   v
                        +-----------+------------+
                        |                        |
                        |      PostgreSQL        |
                        |       Database         |
                        |                        |
                        +-----------+------------+
                                   |
                                   |
                                   v
                        +------------------------+
                        |                        |
                        |   Data Visualization   |
                        |   (Jupyter Notebook)   |
                        |                        |
                        +------------------------+
```
## Prerequisites
- **Docker** and **Docker Compose**
- **Python 3.8+**
- **Bitcoin RPC API Access** (e.g., via Ankr or other providers)
- **Jupyter Notebook**
## Installation
1. **Clone the repository**:
```bash
git clone https://github.com/yourusername/bitcoin-block-analysis.git
cd bitcoin-block-analysis
```

2. **Set up environment variables**:
Create a `.env` file in the project root directory and configure the required environment variables:
```env
RPC_URL=<Your Bitcoin RPC URL>
RPC_API_KEY=<Your RPC API Key>
DB_HOST=postgres
DB_USER=user
DB_PASSWORD=password
DB_NAME=btc
```

3. **Install Python dependencies**:
```bash
pip install -r requirements.txt
```

4. **Start Docker services**:
```bash
docker-compose up -d
```
## Usage
### Data Collection
Run the data collection scripts to fetch block data and store it in the database.
- **Collect Block Headers**:
```bash
python block_headers.py
```
- **Collect Block Statistics**:
```bash
python block_stats.py
```
- **Collect Block Hashes**:
```bash
python block_hash.py
```
### Data Analysis
Open the Jupyter Notebook to analyze and visualize the data:

```bash
jupyter notebook block_stuff.ipynb
```
## Database Schema
The PostgreSQL database stores detailed block information in several tables.
### Entity Relationship Diagram (ERD)

```sql
+-----------------+       +------------------+       +------------------+
|                 |       |                  |       |                  |
|  block_headers  +------->   block_stats     <-------+   block_hashes   |
|                 |       |                  |       |                  |
+--------+--------+       +---------+--------+       +------------------+
         |                          ^
         |                          |
         v                          |
   (Foreign Key)                    |
         |                          |
+--------+--------+                 |
|                 |                 |
| previous_block  |-----------------+
|                 |
+-----------------+
```
 
- **block_headers**: Stores basic header information for each block.
- **block_stats**: Stores detailed statistics for each block.
- **block_hashes**: Stores the hash for each block.
### Table Structures
#### block_headers
- `block_height` (Primary Key)
- `block_hash`
- `confirmations`
- `version`
- `version_hex`
- `merkleroot`
- `time`
- `mediantime`
- `nonce`
- `bits`
- `difficulty`
- `chainwork`
- `n_tx`
- `previous_block_hash`
- `next_block_hash`
#### block_stats
- `block_height` (Primary Key)
- `total_fee`
- `avg_fee_rate`
- `txs`
- `timestamp`
- `ins`
- `maxfee`
- `maxfeerate`
- `maxtxsize`
- `medianfee`
- `mediantime`
- `mediantxsize`
- `minfee`
- `minfeerate`
- `mintxsize`
- `outs`
- `subsidy`
- `swtotal_size`
- `swtotal_weight`
- `swtxs`
- `total_out`
- `total_size`
- `total_weight`
- `utxo_increase`
- `utxo_size_inc`
- `utxo_increase_actual`
- `utxo_size_inc_actual`
#### block_hashes
- `block_height` (Primary Key)
- `block_hash`
## Scripts Description
### `block_headers.py`
Fetches block header information from the Bitcoin network and stores it in the `block_headers` table.

### `block_stats.py`
Retrieves detailed block statistics and stores them in the `block_stats` table.

### `block_hash.py`
Collects block hashes and stores them in the `block_hashes` table.

### `run_all_scripts.py`
A helper script to run all data collection scripts sequentially.

### `block_stuff.ipynb`
A Jupyter Notebook containing data analysis and visualization of the collected data.

## Data Visualization
The Jupyter Notebook includes several plots:
- **Total Fees Over Time**
- **Transactions Per Block Over Time**
- **Block Size Over Time**
- **Network Difficulty Over Time**
- **Hash Rate Over Time**
- **Bitcoin Reward per TH/s per Day**
### Example Plot

```sql
Total Fees and 144-Block EMA per Block Over Time
┌──────────────────────────────────────────────────┐
│           *      *     *     *    *     *        │
│          * *    * *   * *   * *  * *   * *       │
│         *   *  *   * *   * *   **   * *   *      │
│        *     **     **     **       *     *      │
│       *                                      *   │
│                                                  │
│--------------------------------------------------│
│ Time                                             │
└──────────────────────────────────────────────────┘ 
```

## Docker Configuration
The `docker-compose.yml` file sets up the required services:
- **Postgres**: Database service.
- **Adminer**: Database management tool.
- **Grafana**: Visualization and monitoring platform.
### Docker Compose Services

```yaml
services:
  postgres:
    image: postgres:latest
    container_name: btc-db
    environment:
      POSTGRES_DB: btc
      POSTGRES_USER: user
      POSTGRES_PASSWORD: password
    volumes:
      - ./postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    networks:
      - btc_network

  adminer:
    image: adminer
    container_name: adminer
    ports:
      - "8080:8080"
    networks:
      - btc_network

  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    environment:
      - GF_SECURITY_ADMIN_USER=admin
      - GF_SECURITY_ADMIN_PASSWORD=admin
    ports:
      - "3000:3000"
    volumes:
      - ./grafana_data:/var/lib/grafana
    networks:
      - btc_network
    depends_on:
      - postgres

networks:
  btc_network:
    driver: bridge
```
  
## Contributing
Contributions are welcome! Please submit a pull request or open an issue to discuss changes.
## License
This project is licensed under the MIT License.