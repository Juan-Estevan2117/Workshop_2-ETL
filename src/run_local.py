"""Local orchestrator for the Spotify + Grammys ETL pipeline.

Run from the repo root:
    python src/run_local.py
"""
from __future__ import annotations

import logging

from config import (
    GDRIVE_FOLDER_ID,
    GOOGLE_OAUTH_CLIENT_PATH,
    GOOGLE_OAUTH_TOKEN_PATH,
    MYSQL_DW_URL,
    MYSQL_SRC_URL,
    SPOTIFY_CSV,
)
from clean import clean_grammys, clean_spotify
from extract import extract_grammys_db, extract_spotify_csv
from load_drive import upload_csv_to_drive
from load_dw import load_star_schema
from merge import merge_spotify_grammys
from transform import build_star_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def main() -> None:
    log.info("Extracting Spotify CSV")
    spotify_raw = extract_spotify_csv(SPOTIFY_CSV)
    log.info("Spotify raw: %s rows", len(spotify_raw))

    log.info("Extracting Grammys from MySQL")
    grammys_raw = extract_grammys_db(MYSQL_SRC_URL)
    log.info("Grammys raw: %s rows", len(grammys_raw))

    log.info("Cleaning Spotify")
    spotify_clean = clean_spotify(spotify_raw)
    log.info("Spotify clean: %s rows", len(spotify_clean))

    log.info("Cleaning Grammys")
    grammys_clean = clean_grammys(grammys_raw)
    log.info("Grammys clean: %s rows", len(grammys_clean))

    log.info("Merging datasets")
    merged = merge_spotify_grammys(spotify_clean, grammys_clean)
    log.info("Merged: %s rows", len(merged))

    log.info("Building star schema")
    star = build_star_schema(merged)

    log.info("Loading star schema into DW")
    load_star_schema(star, MYSQL_DW_URL)
    log.info("DW loaded")

    if GDRIVE_FOLDER_ID and GOOGLE_OAUTH_CLIENT_PATH.exists():
        log.info("Uploading merged CSV to Drive")
        upload_csv_to_drive(
            merged,
            "merged_spotify_grammys.csv",
            GDRIVE_FOLDER_ID,
            GOOGLE_OAUTH_CLIENT_PATH,
            GOOGLE_OAUTH_TOKEN_PATH,
        )
        log.info("Drive upload complete")
    else:
        log.warning(
            "Skipping Drive upload: GDRIVE_FOLDER_ID or "
            "GOOGLE_OAUTH_CLIENT_PATH not configured"
        )

    log.info("Pipeline complete")


if __name__ == "__main__":
    main()
