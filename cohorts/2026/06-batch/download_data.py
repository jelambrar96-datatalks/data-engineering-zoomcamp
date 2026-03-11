import argparse
import os
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta


def download_file(url, save_path):
    """Download a file from the given URL and save it to the specified path."""
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Raise an error for bad status codes
    with open(save_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded {url} to {save_path}")


def main():
    parser = argparse.ArgumentParser(description="Download taxi trip data from cloudfront.")
    parser.add_argument('--type', required=True, choices=['yellow', 'green'], help='Type of taxi data: yellow or green')
    parser.add_argument('--start_date', required=True, help='Start date (included) in YYYY-MM format (e.g., 2025-01)')
    parser.add_argument('--end_date', required=True, help='End date (not included) in YYYY-MM format (e.g., 2025-12)')

    args = parser.parse_args()

    # Parse dates
    start_date = datetime.strptime(args.start_date, '%Y-%m')
    end_date = datetime.strptime(args.end_date, '%Y-%m')

    # Create raw directory if it doesn't exist
    raw_dir = 'data/raw'
    if not os.path.exists(raw_dir):
        os.makedirs(raw_dir)

    # Loop from start_date to end_date, incrementing by one month
    current_date = start_date
    while current_date < end_date:
        year = current_date.year
        month = current_date.month

        # Construct URL
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{args.type}_tripdata_{year}-{month:02d}.parquet"

        # Construct save path
        partition_dir = os.path.join(raw_dir, f"type={args.type}", f"year={year}", f"month={month:02d}")
        if not os.path.exists(partition_dir):
            os.makedirs(partition_dir)
        filename = f"{args.type}_tripdata_{year}-{month:02d}.parquet"
        save_path = os.path.join(partition_dir, filename)

        # Download the file
        try:
            download_file(url, save_path)
        except requests.exceptions.RequestException as e:
            print(f"Failed to download {url}: {e}")

        # Increment by one month
        current_date += relativedelta(months=1)


if __name__ == "__main__":
    main()
