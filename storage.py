"""
storage.py — Read/write PM2.5 readings as date-partitioned Parquet on S3.

Layout on S3:
    s3://<BUCKET>/readings/date=2026-04-20/readings.parquet
    s3://<BUCKET>/readings/date=2026-04-21/readings.parquet
    ...

Reading is done with DuckDB's native S3 extension — no local copy needed.
Writing uses pandas + boto3 (pyarrow serialisation).
"""

import io
import os
import logging
from datetime import datetime, timedelta

import boto3
import duckdb
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
S3_BUCKET        = os.environ["S3_BUCKET"]
S3_PREFIX        = os.getenv("S3_PREFIX", "readings")
AWS_REGION       = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY   = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_KEY   = os.environ["AWS_SECRET_ACCESS_KEY"]


def _s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
    )


def _s3_key(date_str: str) -> str:
    """Return the S3 object key for a given date string (yyyy-mm-dd)."""
    return f"{S3_PREFIX}/date={date_str}/readings.parquet"


# ── Write ─────────────────────────────────────────────────────────────────────

def upsert_readings(df: pd.DataFrame):
    """
    Write readings to S3, partitioned by date.

    For each unique date in `df`:
      1. Download the existing partition (if any).
      2. Merge with new rows (new rows win on duplicate sensor_id+date).
      3. Upload the merged Parquet back to S3.

    Parameters
    ----------
    df : DataFrame with columns [Date, PM2.5, name, sensor_id, latitude, longitude]
    """
    if df.empty:
        return

    s3 = _s3_client()

    for date_str, day_df in df.groupby("Date"):
        key = _s3_key(date_str)

        # Try to fetch existing partition
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            existing = pd.read_parquet(io.BytesIO(obj["Body"].read()))
            merged = (
                pd.concat([existing, day_df], ignore_index=True)
                .drop_duplicates(subset=["Date", "sensor_id"], keep="last")
            )
        except s3.exceptions.NoSuchKey:
            merged = day_df.copy()
        except Exception as exc:
            logging.warning(f"Could not fetch existing partition {key}: {exc}")
            merged = day_df.copy()

        # Upload
        buf = io.BytesIO()
        merged.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
        logging.info(f"Uploaded {len(merged)} rows → s3://{S3_BUCKET}/{key}")


# ── Read (DuckDB → S3) ────────────────────────────────────────────────────────

def _duckdb_conn() -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection pre-configured with S3 credentials."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_region      = '{AWS_REGION}';
        SET s3_access_key_id     = '{AWS_ACCESS_KEY}';
        SET s3_secret_access_key = '{AWS_SECRET_KEY}';
    """)
    return con


def _parquet_glob(days: int) -> str:
    """Return a glob pattern covering the last `days` days."""
    dates = [
        (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(days)
    ]
    # Build a brace-expansion glob if DuckDB supports it, else use wildcard
    # Wildcard is simpler and DuckDB filters partition columns cheaply
    return f"s3://{S3_BUCKET}/{S3_PREFIX}/**/*.parquet"


def load_readings(days: int = 30) -> pd.DataFrame:
    """
    Return the last `days` days of readings using DuckDB over S3.
    """
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    glob   = _parquet_glob(days)

    try:
        con = _duckdb_conn()
        df  = con.execute(f"""
            SELECT  date        AS "Date",
                    pm25        AS "PM2.5",
                    name,
                    sensor_id,
                    latitude,
                    longitude
            FROM    read_parquet('{glob}', hive_partitioning = true)
            WHERE   date >= '{cutoff}'
            ORDER BY date ASC
        """).df()
        con.close()
        return df

    except Exception as exc:
        logging.error(f"load_readings failed: {exc}")
        return pd.DataFrame(columns=["Date", "PM2.5", "name",
                                     "sensor_id", "latitude", "longitude"])


def load_latest_readings() -> pd.DataFrame:
    """
    Return the single most-recent reading per sensor.
    """
    glob = _parquet_glob(days=7)   # only look back a week for latest

    try:
        con = _duckdb_conn()
        df  = con.execute(f"""
            WITH ranked AS (
                SELECT  date        AS "Date",
                        pm25        AS "PM2.5",
                        name,
                        sensor_id,
                        latitude,
                        longitude,
                        ROW_NUMBER() OVER (
                            PARTITION BY sensor_id
                            ORDER BY date DESC
                        ) AS rn
                FROM read_parquet('{glob}', hive_partitioning = true)
            )
            SELECT "Date", "PM2.5", name, sensor_id, latitude, longitude
            FROM   ranked
            WHERE  rn = 1
        """).df()
        con.close()
        return df

    except Exception as exc:
        logging.error(f"load_latest_readings failed: {exc}")
        return pd.DataFrame(columns=["Date", "PM2.5", "name",
                                     "sensor_id", "latitude", "longitude"])
