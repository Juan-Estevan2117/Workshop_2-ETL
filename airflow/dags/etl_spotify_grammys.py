"""Airflow DAG for the Spotify + Grammys ETL pipeline.

Each task wraps a function from src/ and stages intermediate results as
pickle files in /opt/airflow/data/ so downstream tasks can pick them up
without relying on XCom for large DataFrames.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Make the src/ modules importable inside the Airflow container.
# The volume mount maps host src/ → /opt/airflow/src (read-only).
sys.path.insert(0, "/opt/airflow/src")

import pendulum
from airflow.sdk import DAG, task

STAGING = Path("/opt/airflow/data")
SPOTIFY_CSV = STAGING / "spotify_dataset.csv"


def _mysql_url(database: str) -> str:
    """Build a SQLAlchemy URL from the container's environment variables."""
    user = os.environ["MYSQL_USER"]
    password = os.environ["MYSQL_PASSWORD"]
    return f"mysql+pymysql://{user}:{password}@mysql-dw:3306/{database}"


# Google Drive settings — paths from the container's perspective.
OAUTH_CLIENT_PATH = Path("/opt/airflow/config/google_oauth_client.json")
OAUTH_TOKEN_PATH = Path("/opt/airflow/config/google_oauth_token.json")


@task
def extract_spotify():
    """Read Spotify CSV and persist as pickle."""
    from extract import extract_spotify_csv

    df = extract_spotify_csv(SPOTIFY_CSV)
    df.to_pickle(STAGING / "_stage_spotify_raw.pkl")


@task
def extract_grammys():
    """Read Grammy awards from MySQL source and persist as pickle."""
    from extract import extract_grammys_db

    df = extract_grammys_db(_mysql_url(os.environ["MYSQL_SRC_DB"]))
    df.to_pickle(STAGING / "_stage_grammys_raw.pkl")


@task
def clean_spotify_task():
    """Clean raw Spotify data."""
    import pandas as pd
    from clean import clean_spotify

    raw = pd.read_pickle(STAGING / "_stage_spotify_raw.pkl")
    clean = clean_spotify(raw)
    clean.to_pickle(STAGING / "_stage_spotify_clean.pkl")


@task
def clean_grammys_task():
    """Clean raw Grammy data."""
    import pandas as pd
    from clean import clean_grammys

    raw = pd.read_pickle(STAGING / "_stage_grammys_raw.pkl")
    clean = clean_grammys(raw)
    clean.to_pickle(STAGING / "_stage_grammys_clean.pkl")


@task
def merge_task():
    """Merge cleaned Spotify and Grammy datasets."""
    import pandas as pd
    from merge import merge_spotify_grammys

    spotify = pd.read_pickle(STAGING / "_stage_spotify_clean.pkl")
    grammys = pd.read_pickle(STAGING / "_stage_grammys_clean.pkl")
    merged = merge_spotify_grammys(spotify, grammys)
    merged.to_pickle(STAGING / "_stage_merged.pkl")


@task
def load_dw_task():
    """Build star schema and load into the MySQL warehouse."""
    import pandas as pd
    from load_dw import load_star_schema
    from transform import build_star_schema

    merged = pd.read_pickle(STAGING / "_stage_merged.pkl")
    star = build_star_schema(merged)
    load_star_schema(star, _mysql_url(os.environ["MYSQL_DW_DB"]))


@task
def upload_drive_task():
    """Upload the merged CSV to Google Drive."""
    import pandas as pd
    from load_drive import upload_csv_to_drive

    folder_id = os.getenv("GDRIVE_FOLDER_ID", "")
    if not folder_id or not OAUTH_CLIENT_PATH.exists():
        print("Drive upload skipped: credentials or folder ID not configured.")
        return

    merged = pd.read_pickle(STAGING / "_stage_merged.pkl")
    upload_csv_to_drive(
        merged,
        "merged_spotify_grammys.csv",
        folder_id,
        OAUTH_CLIENT_PATH,
        OAUTH_TOKEN_PATH,
    )


with DAG(
    dag_id="etl_spotify_grammys",
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["etl", "spotify", "grammys"],
) as dag:
    # Extract (parallel)
    sp_raw = extract_spotify()
    gr_raw = extract_grammys()

    # Clean (each depends on its own extract)
    sp_clean = clean_spotify_task()
    gr_clean = clean_grammys_task()

    sp_raw >> sp_clean
    gr_raw >> gr_clean

    # Merge (depends on both cleans)
    merged = merge_task()
    [sp_clean, gr_clean] >> merged

    # Load (both depend on merge, but are independent of each other)
    dw = load_dw_task()
    drive = upload_drive_task()
    merged >> [dw, drive]
