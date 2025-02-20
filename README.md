# Stock Market Medallion ETL Pipeline

## Overview

The **Stock Market Medallion ETL Pipeline** is a robust data processing project that simulates a real-world stock market data system. The pipeline ingests stock-related data generated every second from multiple sources—including user transactions, market feeds, and stock exchanges—and processes it through a multi-layer architecture:

- **Bronze Layer:** Captures raw, unprocessed data (e.g., user transactions, market information, and stock exchange registration details).
- **Silver Layer:** Contains cleaned, structured, and granular data after aggregation and cleaning.
- **Gold Layer:** Aggregates data into summary views (hourly, daily, monthly, quarterly) that are optimized for reporting, dashboards, and business intelligence applications.

> **Note:** The data used in this project is artificially generated and is intended to mimic real-world scenarios with realistic stock price fluctuations and transaction volumes.

## Features

- **Multi-Source Data Integration:** Combines data from a MySQL database (stock market data) and MongoDB (transactions).
- **Medallion Architecture:** Implements Bronze, Silver, and Gold layers for a clean, scalable ETL pipeline.
- **Real-Time Data Simulation:** Processes stock-related data updated every second.
- **PySpark-Driven:** Utilizes PySpark exclusively for data extraction, transformation, and aggregation.

## Setup Instructions

### 1. MySQL Database Setup

1. Ensure you have MySQL installed on your machine
2. Open your terminal and login to MySQL:
   ```bash
   mysql -u your_username -p
   ```
3. Create a new database:
   ```sql
   CREATE DATABASE stock_market;
   ```
4. Exit MySQL and import the data:
   ```bash
   mysql -u your_username -p stock_market < data/stock_market.sql
   ```
5. Verify the data is loaded:
   ```sql
   USE stock_market;
   SHOW TABLES;
   ```
   You should see tables: company_info, market_index

### 2. MongoDB Setup

1. Ensure MongoDB is installed and running
2. Import the transaction data:
   ```bash
   mongoimport --db stock_market --collection transactions --file data/transactions.json --jsonArray
   ```
3. Verify the data:
   ```bash
   mongosh
   use stock_market
   db.transactions.find().limit(1)
   ```

### 3. Environment Setup

1. Create a Python virtual environment (optional):
   ```bash
   python -m venv venv
   source venv/bin/activate 
   ```


2. Copy the environment template:
   ```bash
   cp .env.example .env
   ```

3. Edit `.env` with your database credentials and paths:

### 4. Running the Code

1. Implement the required methods in `src/data_processor.py` and `src/main.py` (Already implemented)

2. Run the main script:
   ```bash
   python src/main.py
   ```

3. Check the output:
   - The processed data will be saved to the path specified in OUTPUT_PATH
   - The console will show progress and data samples at each step

## Project Structure

- `src/`
  - `data_processor.py`: Implement your data processing methods here
  - `main.py`: Main script orchestrating the data processing pipeline
- `data/`
  - `stock_market.sql`: MySQL database dump
  - `transactions.json`: MongoDB transaction data
- `.env.example`: Template for environment variables
- `config.yaml`: Configuration file for environment

##  Credits & Acknowledgements
I had the opportunity to work on and test this project while serving as a teaching assistant for the course DSCI 644. I am deeply grateful to Teacher [Zimeng Lyu](https://zimenglyu.com/) for being kind enough to allow me to share this work on my personal repository. For reference, you can view the original repository [here](https://github.com/zimenglyu/DSCI-644-Project-1-2).