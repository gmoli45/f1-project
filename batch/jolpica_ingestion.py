from datetime import datetime
from time import sleep

import requests
import logging
import boto3

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# AWS Setup
S3_BUCKET = "f1-data-project-raw"
S3_CLIENT = boto3.client("s3")


def fetch_ergast_data(year: int, page: int = 1, limit: int = 30) -> dict:
    """Fetches race results from Jolpica Ergast API with pagination"""
    url = f'https://api.jolpi.ca/ergast/f1/{year}/results.json?offset={page * limit}'
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()     # Raise HTTP errors
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f'Failed to fetch {url}: {str(e)}')
        return None


def upload_to_s3(data: dict, year: int, race_round: int) -> bool:
    """Uploads JSON to S3 with partitioning."""
    try:
        key = f"jolpica_ergast_raw/year={year}/race={race_round}/data_{datetime.utcnow().isoformat()}.json"
        S3_CLIENT.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=str(data),
            ContentType="application/json"
        )
        logger.info(f'Uploaded race {year}-{race_round} to {key}')
        return True
    except Exception as e:
        logger.error(f'S3 upload failed: {str(e)}')
        return False


def process_year(year: int):
    """Main workflow for a single season"""
    page = 0
    while True:
        data = fetch_ergast_data(year, page)
        if not data:
            break  # Exit loop if API fails

        races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
        if not races:
            break  # No more races

        for race in races:
            race_round = race.get("round")
            if upload_to_s3(race, year, race_round):
                logger.info(f'Processed {year} Race {race_round}')
            else:
                logger.warning(f'Failed {year} Race {race_round}')

        page += 1
        sleep(1)


if __name__ == "__main__":
    # Test: Process 2020-2024 seasons
    for year in [2021, 2022, 2023, 2024]:
        logger.info(f'Starting ingestion for {year}')
        process_year(year)
