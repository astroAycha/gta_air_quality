"""
fetcher.py — Standalone script run by GitHub Actions on a schedule.

On the first run (no data in S3 yet) it backfills 30 days.
On subsequent runs it fetches the last 2 days only (yesterday + today)
to catch any late-arriving data without re-downloading everything.

Multiple search centres are used to cover the full GTA area since
the OpenAQ API has a maximum radius of 25km.
"""

import logging
import os
import sys
from datetime import datetime, timedelta

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# OpenAQ max radius is 25,000m — use multiple centres to cover the GTA
SEARCH_CENTRES = [
    (43.6532, -79.3832, "Toronto"),       # downtown Toronto
    (43.5890, -79.6441, "Mississauga"),   # Mississauga centre
    (43.8634, -79.0296, "Ajax")
]
RADIUS = 25000   # metres — OpenAQ maximum

BACKFILL_DAYS    = 30
INCREMENTAL_DAYS = 2


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
        return str(result[0]) if result and result[0] else None
    except Exception:
        return None


def main():
    from data_download import DataDownload
    from storage import upsert_readings

    # Decide how far back to fetch
    oldest = oldest_date_in_s3()
    cutoff = (datetime.now() - timedelta(days=BACKFILL_DAYS)).strftime("%Y-%m-%d")

    if oldest is None or oldest > cutoff:
        days_back = BACKFILL_DAYS
        logging.info(f"Oldest data in S3: {oldest or 'none'} — backfilling last {BACKFILL_DAYS} days")
    else:
        days_back = INCREMENTAL_DAYS
        logging.info(f"Oldest data in S3: {oldest} — incremental fetch (last {INCREMENTAL_DAYS} days)")

    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    logging.info(f"Start date: {start_date}")

    dd = DataDownload()
    all_dfs = []

    for lat, lon, label in SEARCH_CENTRES:
        logging.info(f"Searching {label} ({lat}, {lon}) within {RADIUS}m ...")
        df = dd.fetch_pm25_sensors(
            lat=lat,
            lon=lon,
            radius=RADIUS,
            start_date=start_date,
        )
        if df.empty:
            logging.warning(f"No data returned for {label}.")
        else:
            logging.info(f"{label}: {len(df)} rows, {df['sensor_id'].nunique()} sensors")
            all_dfs.append(df)

    if not all_dfs:
        logging.warning("No data returned from any search centre.")
        sys.exit(0)

    # Merge and deduplicate — same sensor may appear in both search areas
    combined = pd.concat(all_dfs, ignore_index=True)
    before = len(combined)
    combined = combined.drop_duplicates(subset=["Date", "sensor_id"], keep="first")
    logging.info(f"Combined: {len(combined)} rows ({before - len(combined)} duplicates removed)")

    upsert_readings(combined)
    logging.info("Done.")


if __name__ == "__main__":
    main()
