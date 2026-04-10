"""Cleaning functions for the ETL pipeline.

Each function takes a raw DataFrame (as produced by extract.py) and
returns a cleaned copy. Cleaning never mutates the input — downstream
stages assume they can re-read the raw pickle if they need to.
"""
from __future__ import annotations

import pandas as pd


def _normalize_artist(series: pd.Series) -> pd.Series:
    """Return a normalized artist key used for joining across sources.

    The Spotify dataset stores collaborations as a single string with
    artists separated by ``;`` (e.g. ``"Gen Hoshino;Foo Bar"``). The
    Grammy dataset uses a single primary artist. To make a best-effort
    join we keep the first artist, lowercase it, and strip whitespace.
    """
    return (
        series.fillna("unknown")
        .astype(str)
        .str.split(";")
        .str[0]
        .str.strip()
        .str.lower()
    )


def clean_spotify(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the raw Spotify DataFrame.

    Rows with null ``artists`` / ``album_name`` / ``track_name`` are
    dropped because they cannot be matched to anything downstream (in
    the bundled dataset this removes exactly one row). Duplicate
    ``track_id`` rows are kept on purpose: the same track legitimately
    appears under multiple genres, which drives the compound grain of
    the fact table.
    """
    out = df.copy()
    out = out.dropna(subset=["artists", "album_name", "track_name"])
    # The source CSV ships ~450 full-row duplicates (every column
    # identical). They are not analytically meaningful and would break
    # the compound grain (track_id, track_genre) of the fact table, so
    # we collapse them here.
    out = out.drop_duplicates()
    out["artist_norm"] = _normalize_artist(out["artists"])
    return out.reset_index(drop=True)


def clean_grammys(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the raw Grammy awards DataFrame.

    The ``workers`` and ``img`` columns are dropped: the former is a
    free-text credit list that we do not model, the latter is a URL to a
    thumbnail that has no analytical value. Date columns are parsed, the
    ``winner`` flag is cast to a real boolean, and text fields are
    lowercased so downstream joins are case-insensitive.
    """
    out = df.copy()
    out = out.drop(columns=["workers", "img"], errors="ignore")

    # MySQL's LOAD DATA INFILE loads blank CSV fields as empty strings on
    # text columns rather than NULL, so we promote them back to NaN here
    # to let the rest of the cleaning treat them uniformly.
    text_cols = out.select_dtypes(include="object").columns
    out[text_cols] = out[text_cols].replace("", pd.NA)

    # The raw column mixes naive and offset-aware strings, so we force a
    # single timezone (UTC) to get a homogeneous tz-aware datetime dtype.
    out["published_at"] = pd.to_datetime(
        out["published_at"], errors="coerce", utc=True
    )
    out["updated_at"] = pd.to_datetime(
        out["updated_at"], errors="coerce", utc=True
    )

    out["winner"] = (
        out["winner"].astype(str).str.strip().str.lower().eq("true")
    )

    for col in ("category", "nominee"):
        out[col] = out[col].astype(str).str.strip().str.lower()

    out["artist_norm"] = _normalize_artist(out["artist"])
    return out.reset_index(drop=True)
