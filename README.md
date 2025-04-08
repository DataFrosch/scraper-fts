# FTS Data Processor

A Python application that downloads, processes, and stores data from the EU's Financial Transparency System (FTS) in a PostgreSQL database.

## Features

- Downloads Excel datasets from the EU's FTS for years 2007 to present
- Cleans and normalizes data (dates, numeric values, boolean fields)
- Stores records in a PostgreSQL database
- Processes data in batches for improved performance

## Requirements

- Python 3.12+
- PostgreSQL database
- Dependencies listed in pyproject.toml

## Setup

1. Clone this repository
2. Create a `.env` file with the following database credentials:
   ```
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=your_database_name
   DB_USER=your_username
   DB_PASSWORD=your_password
   ```
3. Run `main.py` to download and process the data

## Data Schema

The application creates a `fts_data` table with columns for all fields from the FTS Excel files, including:
- Beneficiary information
- Contract details
- Financial amounts
- Project dates
- Geographic information