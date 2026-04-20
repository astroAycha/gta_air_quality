"""
fetcher.py — Standalone script run by GitHub Actions on a schedule.

It fetches the latest PM2.5 readings for the GTA and writes them
as date-partitioned Parquet files to S3.

Usage:
    python fetcher.py

Environment variables required (set as GitHub Actions secrets):
    OPENAQ_API_KEY
    S3_BUCKET
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_REGION          (optional, default us-east-1)
    CENTER_LAT          (optional, default 43.6532)
    CENTER_LON          (optional, default -79.3832)
    SEARCH_RADIUS       (optional, default 25000)
"""

import logging
import os
import sys
from datetime import datetime, timedelta

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],   # GitHub Actions captures stdout
)

LAT    = float(os.getenv("CENTER_LAT",    "43.6532"))
LON    = float(os.getenv("CENTER_LON",    "-79.3832"))
RADIUS = float(os.getenv("SEARCH_RADIUS", "25000"))


def main():
    from data_download import DataDownload
    from storage import upsert_readings

    # Fetch yesterday + today to catch any late-arriving data
    start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    logging.info(f"Fetching PM2.5 sensors within {RADIUS}m of ({LAT}, {LON})")
    logging.info(f"Start date: {start_date}")

    dd = DataDownload()
    df = dd.fetch_pm25_sensors(
        lat=LAT,
        lon=LON,
        radius=RADIUS,
        start_date=start_date,
    )

    if df.empty:
        logging.warning("No data returned — nothing written to S3.")
        sys.exit(0)

    logging.info(f"Fetched {len(df)} rows across {df['sensor_id'].nunique()} sensors")
    upsert_readings(df)
    logging.info("Done.")


if __name__ == "__main__":
    main()
