"""Load the star schema into the MySQL data warehouse.

Tables are truncated in reverse dependency order (fact first) and
reloaded in correct order (dims → fact) to maintain referential integrity.
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine, text


def load_star_schema(
    star_dict: dict[str, pd.DataFrame], mysql_dw_url: str
) -> None:
    """Load all star schema tables into the MySQL warehouse.

    Args:
        star_dict: dict with keys dim_artist, dim_album, dim_genre,
            dim_year, fact_track.
        mysql_dw_url: SQLAlchemy connection URL for grammys_dw.
    """
    engine = create_engine(mysql_dw_url)

    table_order = [
        "dim_artist",
        "dim_album",
        "dim_genre",
        "dim_year",
        "fact_track",
    ]
    delete_order = list(reversed(table_order))

    with engine.begin() as conn:
        for table in delete_order:
            conn.execute(text(f"DELETE FROM {table}"))

        for table in table_order:
            df = star_dict[table]
            # method="multi" packs many rows per INSERT statement and
            # chunksize caps each batch so we stay under MySQL's
            # max_allowed_packet. The fact has ~113k rows — without this
            # pandas would issue one INSERT per row and the load would
            # take minutes instead of seconds.
            df.to_sql(
                table,
                conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=5000,
            )

    engine.dispose()
