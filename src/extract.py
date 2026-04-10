"""Extraction functions for the ETL pipeline.

Each function is a thin wrapper around pandas/SQLAlchemy so the rest of
the pipeline can treat raw ingestion as a single call that returns a
DataFrame. Keep this module free of cleaning logic — that lives in
clean.py — so each stage has a single responsibility.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine


def extract_spotify_csv(path: Path) -> pd.DataFrame:
    """Load the Spotify dataset from a local CSV.

    The upstream file ships with an unnamed index column (``Unnamed: 0``)
    that is just the original row number — it carries no information, so
    we drop it on the way in to keep downstream schemas clean.
    """
    df = pd.read_csv(path)
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])
    return df


def extract_grammys_db(mysql_src_url: str) -> pd.DataFrame:
    """Load the full Grammy awards table from the MySQL source database.

    A fresh engine is created per call so the function remains stateless
    and safe to use from both the local orchestrator and an Airflow task.
    The engine is disposed before returning to release the connection.
    """
    engine = create_engine(mysql_src_url)
    try:
        df = pd.read_sql("SELECT * FROM awards", engine)
    finally:
        engine.dispose()
    return df
