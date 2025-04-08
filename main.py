import requests
import pandas as pd
import os
import tempfile
from datetime import datetime


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
                    print("-" * 50)

                    # ['Year', 'Budget', 'Reference of the Legal Commitment (LC)', 'Reference (Budget)', 'Name of beneficiary', 'VAT number of beneficiary', 'Not-for-profit organisation (NFPO)', 'Non-governmental organisation (NGO)', 'Coordinator', 'Address', 'City', 'Postal code', 'Beneficiary country', 'NUTS2', 'Geographical Zone', 'Action location', 'Beneficiary’s contracted amount (EUR)', 'Beneficiary’s estimated contracted amount (EUR)', 'Beneficiary’s estimated consumed amount (EUR)', 'Commitment contracted amount (EUR) (A)', 'Additional/Reduced amount (EUR) (B)', 'Commitment  total amount (EUR) (A+B)', 'Commitment consumed amount (EUR)', 'Source of (estimated) detailed amount', 'Expense type', 'Subject of grant or contract', 'Responsible department', 'Budget line number', 'Budget line name', 'Programme name', 'Funding type', 'Beneficiary Group Code', 'Beneficiary type', 'Project start date', 'Project end date', 'Type of contract*', 'Management type', 'Benefiting country']

                else:
                    print(f"Status code: {response.status_code} - Failed to download")

            except Exception as e:
                print(f"Error processing year {year}: {e}")


if __name__ == "__main__":
    main()
