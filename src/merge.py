"""Merge logic between the two cleaned sources.

The join is intentionally asymmetric: Spotify is the spine (every track
must survive) and Grammy information is attached as extra columns. The
match key is ``artist_norm``, which both clean_* functions produce with
the same normalization rule.
"""
from __future__ import annotations

import pandas as pd


def merge_spotify_grammys(
    spotify_df: pd.DataFrame, grammys_df: pd.DataFrame
) -> pd.DataFrame:
    """Attach per-artist Grammy stats to every Spotify row.

    Grammy rows are first collapsed to one row per normalized artist with
    four aggregates: total nominations, total wins, earliest and latest
    ceremony year. Rows whose artist is unknown are excluded from the
    aggregate so they do not poison a bucket that Spotify can never
    match against. The resulting table is left-joined onto Spotify and
    missing values are filled with zeros so numeric columns stay int.
    """
    usable = grammys_df[grammys_df["artist_norm"] != "unknown"]

    grammy_stats = (
        usable.groupby("artist_norm", as_index=False)
        .agg(
            grammy_nominations=("artist_norm", "size"),
            grammy_wins=("winner", "sum"),
            first_grammy_year=("year", "min"),
            last_grammy_year=("year", "max"),
        )
    )

    merged = spotify_df.merge(grammy_stats, on="artist_norm", how="left")

    fill_zero = [
        "grammy_nominations",
        "grammy_wins",
        "first_grammy_year",
        "last_grammy_year",
    ]
    merged[fill_zero] = merged[fill_zero].fillna(0).astype(int)
    return merged
