# FTS Data Processor - Project Context

## Overview

This project downloads and processes data from the EU's Financial Transparency System (FTS), storing it in a PostgreSQL database. It handles Excel datasets from 2007 to present, containing information about EU budget spending and beneficiaries.

## Tech Stack

- **Language**: Python 3.12+
- **Database**: PostgreSQL
- **Key Libraries**: openpyxl (Excel processing), psycopg2 (database), requests (HTTP downloads)

## Project Structure

```
fts/
├── main.py          # Main application logic
├── .env             # Database credentials (gitignored)
├── .env.example     # Template for environment variables
├── pyproject.toml   # Python dependencies
└── README.md        # User documentation
```

## Key Functionality

### main.py

- **Downloads**: Fetches Excel files from EU FTS website (years 2007-current)
- **Processing**: Cleans and normalizes data (dates, booleans, numeric values)
- **Database**: Creates and populates `fts_data` table with 40+ columns
- **Performance**: Batch processing (5000 rows) using psycopg2's execute_values

## Database Schema

The `fts_data` table contains:

- Financial data (amounts in EUR)
- Beneficiary details (name, VAT, address, country)
- Contract/grant information
- Project dates and locations
- Management and funding types

## Environment Setup

Requires `.env` file with:

- DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

## Notes for Development

- Uses openpyxl for efficient Excel reading (replaced pandas for performance)
- Handles data type conversions (Yes/No → boolean, date formatting, numeric parsing)
- Processes data in batches to handle large datasets efficiently
- Downloads are temporary (uses tempfile directory)
