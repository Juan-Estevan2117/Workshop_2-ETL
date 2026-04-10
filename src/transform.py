"""Star schema construction from the merged DataFrame.

The output is a dict with the five tables that will be loaded into the
MySQL warehouse. Keys are generated as simple surrogate integers
starting at 1 so they are friendly to inspect and to reference from SQL.
The fact table has a compound grain ``(track_id, genre_key)`` because
the same Spotify track can legitimately appear under multiple genres.
"""
from __future__ import annotations

import pandas as pd


def build_star_schema(merged_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Split the merged DataFrame into dims and a fact table.

    Returns a dict with keys ``dim_artist``, ``dim_album``, ``dim_genre``,
    ``dim_year`` and ``fact_track``. The dicts is used downstream by
    ``load_dw.load_star_schema`` in the order dims → fact.
    """
    df = merged_df.copy()

    # One row per normalized artist. The grammy_* columns come from the
    # merge step and are constant within an artist, so taking "first" is
    # equivalent to taking any value in the group.
    dim_artist = (
        df.groupby("artist_norm", as_index=False)
        .agg(
            artist_name=("artists", "first"),
            grammy_nominations=("grammy_nominations", "first"),
            grammy_wins=("grammy_wins", "first"),
            first_grammy_year=("first_grammy_year", "first"),
            last_grammy_year=("last_grammy_year", "first"),
        )
    )
    dim_artist.insert(0, "artist_key", range(1, len(dim_artist) + 1))

    # Album grain is (album_name, artist_norm): two different artists
    # can legitimately share an album title, so the artist is part of
    # the natural key even though dim_album does not hold a FK to
    # dim_artist (that would make it a snowflake).
    dim_album = (
        df[["album_name", "artist_norm"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_album.insert(0, "album_key", range(1, len(dim_album) + 1))

    dim_genre = pd.DataFrame(
        {"genre_name": sorted(df["track_genre"].unique())}
    )
    dim_genre.insert(0, "genre_key", range(1, len(dim_genre) + 1))

    # dim_year is keyed on first_grammy_year. A value of 0 is a sentinel
    # meaning "the artist of this track was never nominated for a
    # Grammy", which lets the fact table keep a non-null FK everywhere.
    dim_year = pd.DataFrame(
        {"year_value": sorted(df["first_grammy_year"].unique())}
    )
    dim_year.insert(0, "year_key", range(1, len(dim_year) + 1))

    # Resolve surrogate keys back onto the flattened rows so the fact
    # table only stores integer FKs plus the numeric measures.
    fact = (
        df.merge(
            dim_artist[["artist_key", "artist_norm"]], on="artist_norm"
        )
        .merge(
            dim_album[["album_key", "album_name", "artist_norm"]],
            on=["album_name", "artist_norm"],
        )
        .merge(
            dim_genre.rename(columns={"genre_name": "track_genre"}),
            on="track_genre",
        )
        .merge(
            dim_year.rename(columns={"year_value": "first_grammy_year"}),
            on="first_grammy_year",
        )
    )

    # ``key`` is a reserved word in MySQL — rename to ``music_key``.
    fact = fact.rename(columns={"key": "music_key"})

    fact_cols = [
        "track_id",
        "artist_key",
        "album_key",
        "genre_key",
        "year_key",
        "popularity",
        "duration_ms",
        "explicit",
        "danceability",
        "energy",
        "music_key",
        "loudness",
        "mode",
        "speechiness",
        "acousticness",
        "instrumentalness",
        "liveness",
        "valence",
        "tempo",
        "time_signature",
    ]
    fact_track = fact[fact_cols].reset_index(drop=True)

    return {
        "dim_artist": dim_artist,
        "dim_album": dim_album,
        "dim_genre": dim_genre,
        "dim_year": dim_year,
        "fact_track": fact_track,
    }
