"""Central configuration for the ETL pipeline.

Loads the .env file that lives next to airflow/docker-compose.yaml and
exposes every path and connection string as a Path object or a plain str.
Every other module in src/ should import from here instead of reading
os.environ on its own, so there is a single source of truth.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Repository root resolved from this file's location:
#   src/config.py  ->  src/  ->  repo root
PROJECT_ROOT: Path = Path(__file__).resolve().parents[1]

# The .env file lives in airflow/ so docker compose and this module share
# the same file. load_dotenv is idempotent, calling it at import time is
# cheap and deterministic.
ENV_FILE: Path = PROJECT_ROOT / "airflow" / ".env"
load_dotenv(ENV_FILE)


def _require(key: str) -> str:
    """Return an env var or raise a clear error pointing at the .env file."""
    value = os.getenv(key)
    if not value:
        raise RuntimeError(
            f"Required environment variable '{key}' is missing or empty. "
            f"Check {ENV_FILE}."
        )
    return value


# ---------- Datasets ----------
# Absolute path to the Spotify CSV (bundled in the repo under airflow/data).
SPOTIFY_CSV: Path = PROJECT_ROOT / "airflow" / "data" / "spotify_dataset.csv"

# Shared directory where pipeline stages drop intermediate pickle files.
# The same directory is mounted inside Airflow containers at /opt/airflow/data,
# so Phase B will find the files without any path rewriting.
STAGING_DIR: Path = PROJECT_ROOT / "airflow" / "data"


# ---------- MySQL Data Warehouse ----------
MYSQL_HOST: str = _require("MYSQL_HOST")
MYSQL_PORT: int = int(_require("MYSQL_PORT"))
MYSQL_USER: str = _require("MYSQL_USER")
MYSQL_PASSWORD: str = _require("MYSQL_PASSWORD")
MYSQL_SRC_DB: str = _require("MYSQL_SRC_DB")
MYSQL_DW_DB: str = _require("MYSQL_DW_DB")

# SQLAlchemy URLs for the local pipeline. Phase B will build different
# URLs inside Airflow containers (host = mysql-dw, port = 3306) from the
# same credentials.
MYSQL_SRC_URL: str = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_SRC_DB}"
)

MYSQL_DW_URL: str = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DW_DB}"
)


# ---------- Google Drive ----------
# These two values are only consumed in Step A.10. They are optional at
# import time so the rest of the pipeline can run before Drive credentials
# are provisioned.
GDRIVE_FOLDER_ID: str = os.getenv("GDRIVE_FOLDER_ID", "")

_oauth_client_raw = os.getenv("GOOGLE_OAUTH_CLIENT_PATH", "")
GOOGLE_OAUTH_CLIENT_PATH: Path = (
    PROJECT_ROOT / _oauth_client_raw if _oauth_client_raw else Path()
)

_oauth_token_raw = os.getenv("GOOGLE_OAUTH_TOKEN_PATH", "")
GOOGLE_OAUTH_TOKEN_PATH: Path = (
    PROJECT_ROOT / _oauth_token_raw if _oauth_token_raw else Path()
)
