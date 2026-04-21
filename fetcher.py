"""
fetcher.py — Standalone script run by GitHub Actions on a schedule.

On the first run (no data in S3 yet) it backfills 30 days.
On subsequent runs it fetches the last 2 days only (yesterday + today)
to catch any late-arriving data without re-downloading everything.
"""

import logging
import os
import sys
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

LAT    = float(os.getenv("CENTER_LAT",    "43.6532"))
LON    = float(os.getenv("CENTER_LON",    "-79.3832"))
RADIUS = float(os.getenv("SEARCH_RADIUS", "25000"))

BACKFILL_DAYS  = 30   # how far back to go on first run
INCREMENTAL_DAYS = 2  # how far back to go on subsequent runs


def s3_has_data() -> bool:
    """Return True if the S3 bucket already contains at least one parquet file."""
    import boto3
    s3 = boto3.client(
        "s3",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    bucket = os.environ["S3_BUCKET"]
    prefix = os.getenv("S3_PREFIX", "readings")
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return resp.get("KeyCount", 0) > 0


def main():
    from data_download import DataDownload
    from storage import upsert_readings

    # Decide how far back to fetch
    if s3_has_data():
        days_back = INCREMENTAL_DAYS
        logging.info("Existing data found — incremental fetch (last 2 days)")
    else:
        days_back = BACKFILL_DAYS
        logging.info("No existing data — backfilling last 30 days")

    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

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
