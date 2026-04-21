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


def oldest_date_in_s3() -> str | None:
    """
    Return the oldest date in S3 as a string (yyyy-mm-dd),
    or None if no data exists yet.
    """
    from storage import _duckdb_conn, _parquet_glob
    try:
        con = _duckdb_conn()
        result = con.execute(f"""
            SELECT MIN(date) AS oldest
            FROM read_parquet('{_parquet_glob(days=90)}', hive_partitioning = true)
        """).fetchone()
        con.close()
        return result[0] if result and result[0] else None
    except Exception:
        # No files found or any other error — treat as empty
        return None


def main():
    from data_download import DataDownload
    from storage import upsert_readings

    # Decide how far back to fetch based on oldest date already in S3
    oldest = oldest_date_in_s3()
    cutoff = (datetime.now() - timedelta(days=BACKFILL_DAYS)).strftime("%Y-%m-%d")

    if oldest is None or oldest > cutoff:
        days_back = BACKFILL_DAYS
        logging.info(f"Oldest data in S3: {oldest or 'none'} — backfilling last {BACKFILL_DAYS} days")
    else:
        days_back = INCREMENTAL_DAYS
        logging.info(f"Oldest data in S3: {oldest} — incremental fetch (last {INCREMENTAL_DAYS} days)")

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
