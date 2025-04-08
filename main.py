import requests
import pandas as pd
import os
import tempfile
import psycopg2
from dotenv import load_dotenv
from datetime import datetime


def create_table(conn):
    """Create FTS table if it doesn't exist"""
    cursor = conn.cursor()

    # Columns from the Excel file comment in the original code
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS fts_data (
        id SERIAL PRIMARY KEY,
        year INTEGER,
        budget VARCHAR(255),
        reference_legal_commitment VARCHAR(255),
        reference_budget VARCHAR(255),
        beneficiary_name VARCHAR(255),
        beneficiary_vat VARCHAR(255),
        not_for_profit BOOLEAN,
        non_governmental BOOLEAN,
        coordinator BOOLEAN,
        address VARCHAR(255),
        city VARCHAR(255),
        postal_code VARCHAR(50),
        beneficiary_country VARCHAR(100),
        nuts2 VARCHAR(100),
        geographical_zone VARCHAR(255),
        action_location VARCHAR(255),
        beneficiary_contracted_amount NUMERIC,
        beneficiary_estimated_contracted_amount NUMERIC,
        beneficiary_estimated_consumed_amount NUMERIC,
        commitment_contracted_amount NUMERIC,
        additional_reduced_amount NUMERIC,
        commitment_total_amount NUMERIC,
        commitment_consumed_amount NUMERIC,
        source_estimated_detailed_amount VARCHAR(255),
        expense_type VARCHAR(100),
        subject_grant_contract TEXT,
        responsible_department VARCHAR(255),
        budget_line_number VARCHAR(100),
        budget_line_name VARCHAR(255),
        programme_name VARCHAR(255),
        funding_type VARCHAR(100),
        beneficiary_group_code VARCHAR(50),
        beneficiary_type VARCHAR(100),
        project_start_date DATE,
        project_end_date DATE,
        type_of_contract VARCHAR(100),
        management_type VARCHAR(100),
        benefiting_country VARCHAR(100)
    )
    """
    )
    conn.commit()


def clean_data(df):
    """Clean and normalize the data"""
    # Replace missing values with None
    df = df.where(pd.notna(df), None)

    # Convert Yes/No to boolean
    for col in [
        "Not-for-profit organisation (NFPO)",
        "Non-governmental organisation (NGO)",
        "Coordinator",
    ]:
        if col in df.columns:
            df[col] = df[col].map({"Yes": True, "No": False})

    # Convert date columns to proper format
    for col in ["Project start date", "Project end date"]:
        if col in df.columns:
            # Convert to datetime but handle NaT values before database insertion
            df[col] = pd.to_datetime(df[col], errors="coerce")
            # Convert NaT to None for database compatibility
            df[col] = df[col].where(pd.notna(df[col]), None)

    # Convert numeric columns to float
    numeric_cols = [
        "Beneficiary’s contracted amount (EUR)",
        "Beneficiary’s estimated contracted amount (EUR)",
        "Beneficiary’s estimated consumed amount (EUR)",
        "Commitment contracted amount (EUR) (A)",
        "Additional/Reduced amount (EUR) (B)",
        "Commitment  total amount (EUR) (A+B)",
        "Commitment consumed amount (EUR)",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ""), errors="coerce"
            )

    return df


def insert_data(conn, df, year):
    """Insert data into the database"""
    cursor = conn.cursor()

    # Clean data before insertion
    df = clean_data(df)

    # Map DataFrame columns to database columns
    column_mapping = {
        "Year": "year",
        "Budget": "budget",
        "Reference of the Legal Commitment (LC)": "reference_legal_commitment",
        "Reference (Budget)": "reference_budget",
        "Name of beneficiary": "beneficiary_name",
        "VAT number of beneficiary": "beneficiary_vat",
        "Not-for-profit organisation (NFPO)": "not_for_profit",
        "Non-governmental organisation (NGO)": "non_governmental",
        "Coordinator": "coordinator",
        "Address": "address",
        "City": "city",
        "Postal code": "postal_code",
        "Beneficiary country": "beneficiary_country",
        "NUTS2": "nuts2",
        "Geographical Zone": "geographical_zone",
        "Action location": "action_location",
        "Beneficiary’s contracted amount (EUR)": "beneficiary_contracted_amount",
        "Beneficiary’s estimated contracted amount (EUR)": "beneficiary_estimated_contracted_amount",
        "Beneficiary’s estimated consumed amount (EUR)": "beneficiary_estimated_consumed_amount",
        "Commitment contracted amount (EUR) (A)": "commitment_contracted_amount",
        "Additional/Reduced amount (EUR) (B)": "additional_reduced_amount",
        "Commitment  total amount (EUR) (A+B)": "commitment_total_amount",
        "Commitment consumed amount (EUR)": "commitment_consumed_amount",
        "Source of (estimated) detailed amount": "source_estimated_detailed_amount",
        "Expense type": "expense_type",
        "Subject of grant or contract": "subject_grant_contract",
        "Responsible department": "responsible_department",
        "Budget line number": "budget_line_number",
        "Budget line name": "budget_line_name",
        "Programme name": "programme_name",
        "Funding type": "funding_type",
        "Beneficiary Group Code": "beneficiary_group_code",
        "Beneficiary type": "beneficiary_type",
        "Project start date": "project_start_date",
        "Project end date": "project_end_date",
        "Type of contract*": "type_of_contract",
        "Management type": "management_type",
        "Benefiting country": "benefiting_country",
    }

    # Create a list of columns that exist in both the DataFrame and database
    df_columns = df.columns.tolist()
    db_columns = []
    placeholders = []

    for excel_col, db_col in column_mapping.items():
        if excel_col in df_columns:
            db_columns.append(db_col)
            placeholders.append("%s")

    # Build the INSERT query
    columns_str = ", ".join(db_columns)
    placeholders_str = ", ".join(placeholders)
    query = f"INSERT INTO fts_data ({columns_str}) VALUES ({placeholders_str})"

    # Convert DataFrame to list of tuples for insertion
    rows = []
    for _, row in df.iterrows():
        row_values = []
        for excel_col, db_col in column_mapping.items():
            if excel_col in df_columns:
                # Handle date columns explicitly
                if excel_col in ["Project start date", "Project end date"]:
                    # Ensure NaT values are converted to None
                    value = None if pd.isna(row[excel_col]) else row[excel_col]
                else:
                    value = row[excel_col]
                row_values.append(value)
        rows.append(tuple(row_values))

    # Insert records in batches to improve performance
    batch_size = 1000
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        cursor.executemany(query, batch)
        conn.commit()
        print(
            f"Inserted {len(batch)} records for year {year} (batch {i//batch_size + 1}/{(len(rows)-1)//batch_size + 1})"
        )


def connect_to_database():
    """Connect to the PostgreSQL database using credentials from .env"""
    load_dotenv()

    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")

    if not all([db_name, db_user, db_password]):
        raise ValueError("Database credentials missing from .env file")

    conn = psycopg2.connect(
        host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password
    )

    return conn


def main():
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:138.0) Gecko/20100101 Firefox/138.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        # 'Accept-Encoding': 'gzip, deflate, br, zstd',
        "Connection": "keep-alive",
        "Referer": "https://ec.europa.eu/budget/financial-transparency-system/help.html",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Priority": "u=0, i",
    }

    current_year = datetime.now().year

    try:
        # Connect to the PostgreSQL database
        conn = connect_to_database()

        # Create the table if it doesn't exist
        create_table(conn)

        # Create temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            for year in range(2007, current_year + 1):
                url = f"https://ec.europa.eu/budget/financial-transparency-system/download/{year}_FTS_dataset_en.xlsx"
                print(f"Downloading data for year {year}...")

                try:
                    response = requests.get(url, headers=headers)

                    if response.status_code == 200:
                        print(f"Status code: {response.status_code} - Success")

                        # Save file to temporary location
                        temp_file_path = os.path.join(
                            temp_dir, f"{year}_FTS_dataset_en.xlsx"
                        )
                        with open(temp_file_path, "wb") as f:
                            f.write(response.content)

                        # Read Excel file from disk
                        df = pd.read_excel(temp_file_path)

                        # Print the column headers
                        print(f"Columns for year {year}:")
                        print(df.columns.tolist())

                        # Insert data into PostgreSQL
                        insert_data(conn, df, year)

                        print(f"Successfully processed data for year {year}")
                        print("-" * 50)

                    else:
                        print(
                            f"Status code: {response.status_code} - Failed to download"
                        )

                except Exception as e:
                    print(f"Error processing year {year}: {e}")

    except Exception as e:
        print(f"Database error: {e}")

    finally:
        if "conn" in locals() and conn:
            conn.close()
            print("Database connection closed")


if __name__ == "__main__":
    main()
