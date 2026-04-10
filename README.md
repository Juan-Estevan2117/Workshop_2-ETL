# ETL Workshop 2 — Spotify + Grammys

End-to-end ETL pipeline that combines the Spotify tracks dataset with the
Grammy Awards dataset, loads the result into a MySQL star-schema data
warehouse, and publishes the merged CSV to Google Drive. The pipeline is
implemented twice: first as a local Python orchestrator (Phase A — done),
and later as an Airflow DAG that reuses the same modules (Phase B —
pending).

This README documents **Phase A only**. Phase B and the dashboard will be
appended once they are implemented.

---

## 1. Architecture overview

```
                   ┌───────────────────────────┐
                   │ spotify_dataset.csv       │
                   │ (114k rows × 21 cols)     │
                   └────────────┬──────────────┘
                                │ extract_spotify_csv
                                ▼
                         ┌──────────────┐
                         │ clean_spotify│
                         └──────┬───────┘
                                │
┌────────────────┐              │                 ┌────────────────────┐
│ grammys_src DB │              │                 │  merge_spotify_    │
│ (MySQL, awards)│─extract──▶ clean_grammys ─────▶│  grammys           │
└────────────────┘                                └──────────┬─────────┘
                                                             │
                                                             ▼
                                                   ┌──────────────────┐
                                                   │ build_star_schema│
                                                   └──────┬───────────┘
                                                          │
                                      ┌───────────────────┴──────────┐
                                      ▼                              ▼
                             ┌─────────────────┐           ┌──────────────────┐
                             │ load_star_schema│           │ upload_csv_to_   │
                             │ → grammys_dw    │           │ drive (OAuth)    │
                             └─────────────────┘           └──────────────────┘
```

Both MySQL databases live in the **same** `mysql-dw` container but in
**different schemas**:

- `grammys_src` — landing layer, populated by `LOAD DATA INFILE` from
  `the_grammy_awards.csv` on container startup.
- `grammys_dw`  — analytical layer (star schema), populated by the
  Python pipeline.

Keeping them apart is deliberate: the landing layer is immutable raw
data, the warehouse is the curated model that downstream BI tools query.

---

## 2. Repository layout

```
etl-workshop_2/
├── airflow/
│   ├── docker-compose.yaml       # Airflow + mysql-dw (Phase A uses only mysql-dw)
│   ├── requirements.txt          # Runtime deps for Airflow workers (Phase B)
│   ├── .env                      # gitignored — real credentials
│   ├── .env.example              # template checked into the repo
│   ├── config/                   # gitignored — google_oauth_client.json, token
│   └── data/
│       ├── spotify_dataset.csv
│       └── the_grammy_awards.csv
├── src/                          # flat layout, run with `python src/run_local.py`
│   ├── config.py                 # reads airflow/.env, exposes Path + URLs
│   ├── extract.py                # extract_spotify_csv, extract_grammys_db
│   ├── clean.py                  # clean_spotify, clean_grammys
│   ├── merge.py                  # merge_spotify_grammys
│   ├── transform.py              # build_star_schema (dict of 5 DataFrames)
│   ├── load_dw.py                # load_star_schema
│   ├── load_drive.py             # upload_csv_to_drive (OAuth user creds)
│   └── run_local.py              # local orchestrator entry point
├── sql/
│   ├── init_db_grammys.sql       # creates grammys_src + awards + LOAD DATA
│   ├── init_dw.sh                # sourced init: creates grammys_dw + grants
│   └── create_star_schema.sql    # DDL for the 5 star-schema tables
├── notebooks/
│   └── data_profiling.ipynb      # initial exploration (not part of the pipeline)
├── figs/                         # diagrams (referenced from this README)
├── requirements.txt              # local .venv deps (Phase A runtime)
└── README.md
```

There are **no subfolders inside `src/`** on purpose — every module is a
sibling and imports with bare names (`from extract import …`). Running
`python src/run_local.py` from the repo root puts `src/` on `sys.path`
automatically.

---

## 3. Data sources

### 3.1 Spotify (CSV)

- File: `airflow/data/spotify_dataset.csv`
- Shape: **114 000 × 21** (drops to **113 549 × 21** after cleaning).
- The raw file ships with an unnamed index column (`Unnamed: 0`) and
  ~450 full-row duplicates that become visible only after dropping
  that index column.
- A track can appear under multiple genres, so `track_id` alone is
  **not** unique. The grain of the fact table is therefore compound:
  `(track_id, genre_key)`.

### 3.2 Grammy Awards (MySQL)

- Source: `grammys_src.awards` table populated from
  `airflow/data/the_grammy_awards.csv` on container startup.
- Shape: **4 810 × 10**.
- Cleaning drops `workers` (free-text credit list) and `img` (thumbnail
  URL) — neither is modeled downstream.
- `LOAD DATA INFILE` stores blank CSV fields as empty strings on text
  columns (not `NULL`), so cleaning promotes `""` → `NA` before any
  other transformation.
- Every row in the bundled CSV has `winner = True`. This is a property
  of the source data, not a bug. Downstream KPIs that compare winners
  vs. nominees will therefore need a different source or manual tagging.

---

## 4. Data model (star schema)

The warehouse follows a classic Kimball star: one fact and four
conformed dimensions. All surrogate keys are assigned sequentially in
Python (`range(1, n+1)`) rather than via `AUTO_INCREMENT`, so the DDL is
simple and the loader is idempotent (a `DELETE` + `INSERT` cycle inside
a single transaction).

![Star schema](figs/awards_and_star_schema.png)

### 4.1 Tables

| Table        | Grain                                  | Source of keys                          |
|--------------|----------------------------------------|-----------------------------------------|
| `dim_artist` | 1 row per normalized artist            | `artist_norm` (first artist, lowercased)|
| `dim_album`  | 1 row per `(album_name, artist_norm)`  | composite natural key                   |
| `dim_genre`  | 1 row per distinct `track_genre`       | natural key                             |
| `dim_year`   | 1 row per first Grammy nomination year | natural key + sentinel `0 = never`      |
| `fact_track` | 1 row per `(track_id, genre_key)`      | all four dim FKs + numeric measures     |

### 4.2 Design notes

- **`dim_artist` holds the Grammy aggregates** (`grammy_nominations`,
  `grammy_wins`, `first_grammy_year`, `last_grammy_year`) because those
  attributes describe the artist, not any individual track. This keeps
  `fact_track` narrow and lets BI tools filter tracks by artist-level
  Grammy status without joining anything else.
- **`dim_year` uses `0` as a sentinel** for "never nominated". Every
  track has a non-nullable `year_key`, which means BI queries can join
  without worrying about `NULL` handling. Tracks whose artists were
  never nominated all point at `year_key = 0`.
- **MySQL reserved word**: `key` (the Spotify audio feature) was renamed
  to `music_key` in `fact_track` to avoid quoting it everywhere.
- **Collations** are mixed on purpose:
  - `dim_artist.artist_norm` is `utf8mb4_bin` so `"AJR"` and `"ajr"`
    are distinct keys. The whole table inherits `utf8mb4_bin` so the
    unique key on `artist_norm` is consistent with row comparisons.
  - Every other table uses `utf8mb4_0900_ai_ci` (case/accent
    insensitive), which is the usual choice for human-readable text.

### 4.3 DDL

See `sql/create_star_schema.sql`. Highlights:

```sql
CREATE TABLE dim_artist (
  artist_key         INT NOT NULL,
  artist_norm        VARCHAR(255) NOT NULL COLLATE utf8mb4_bin,
  artist_name        VARCHAR(512) NOT NULL,
  grammy_nominations INT NOT NULL DEFAULT 0,
  grammy_wins        INT NOT NULL DEFAULT 0,
  first_grammy_year  INT NOT NULL DEFAULT 0,
  last_grammy_year   INT NOT NULL DEFAULT 0,
  PRIMARY KEY (artist_key),
  UNIQUE KEY uk_dim_artist_norm (artist_norm)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE fact_track (
  track_id  VARCHAR(32) NOT NULL,
  artist_key INT NOT NULL,
  album_key  INT NOT NULL,
  genre_key  INT NOT NULL,
  year_key   INT NOT NULL,
  -- ... measures ...
  PRIMARY KEY (track_id, genre_key),
  CONSTRAINT fk_fact_artist FOREIGN KEY (artist_key) REFERENCES dim_artist (artist_key),
  CONSTRAINT fk_fact_album  FOREIGN KEY (album_key)  REFERENCES dim_album  (album_key),
  CONSTRAINT fk_fact_genre  FOREIGN KEY (genre_key)  REFERENCES dim_genre  (genre_key),
  CONSTRAINT fk_fact_year   FOREIGN KEY (year_key)   REFERENCES dim_year   (year_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```

---

## 5. Pipeline stages

All functions live in `src/` and are pure: each takes DataFrames and/or
paths as inputs and returns DataFrames or writes to an external system.
Nothing mutates its arguments in place.

### 5.1 Extract (`src/extract.py`)

- `extract_spotify_csv(path: Path) -> DataFrame` — `pd.read_csv` then
  drops `Unnamed: 0`.
- `extract_grammys_db(mysql_src_url: str) -> DataFrame` — opens a
  SQLAlchemy engine against `grammys_src`, runs `SELECT * FROM awards`,
  and disposes the engine in a `finally` block so connections don't
  leak on error.

### 5.2 Clean (`src/clean.py`)

- `_normalize_artist(series)` — helper that produces the join key:
  keeps the first artist from a `;`-separated list, strips whitespace,
  lowercases. Used by both cleaners.
- `clean_spotify(df)`:
  - Drops rows where `artists` / `album_name` / `track_name` are null
    (1 row in the bundled dataset).
  - Drops **full-row duplicates** (~450 rows). These were hidden in the
    raw CSV by the `Unnamed: 0` index column and would have broken the
    compound grain of the fact table.
  - Adds `artist_norm`.
- `clean_grammys(df)`:
  - Drops `workers` and `img`.
  - Promotes empty strings to `NA` on all text columns.
  - Parses `published_at` and `updated_at` with `utc=True` (the source
    mixes naive and tz-aware strings; forcing UTC gives a homogeneous
    dtype).
  - Casts `winner` to a real boolean.
  - Lowercases `category` and `nominee`.
  - Adds `artist_norm`.

### 5.3 Merge (`src/merge.py`)

Groups the cleaned Grammys by `artist_norm` (excluding `"unknown"`) into
four artist-level aggregates:

| Column               | Aggregation                       |
|----------------------|-----------------------------------|
| `grammy_nominations` | `count(*)`                        |
| `grammy_wins`        | `sum(winner)`                     |
| `first_grammy_year`  | `min(year)`                       |
| `last_grammy_year`   | `max(year)`                       |

Then left-joins the cleaned Spotify onto that aggregate on
`artist_norm`. Spotify rows that don't match get `0` on all four
columns. The left join is intentional — we want every Spotify track to
survive, even if its artist never touched a Grammy.

### 5.4 Transform (`src/transform.py`)

`build_star_schema(merged_df)` returns a `dict[str, DataFrame]` with the
five target tables. Each dimension is built by:

1. Selecting the distinct natural keys from the merged frame.
2. Sorting them (so surrogate keys are reproducible across runs).
3. Assigning `*_key = range(1, n+1)`.
4. Joining back onto the merged frame to populate the fact's FKs.

`dim_year` also prepends a sentinel row `(year_key=0, year_value=0)` so
every fact row gets a non-null `year_key` even when the artist was
never nominated.

### 5.5 Load DW (`src/load_dw.py`)

```python
with engine.begin() as conn:
    for table in reversed(table_order):
        conn.execute(text(f"DELETE FROM {table}"))
    for table in table_order:
        star_dict[table].to_sql(
            table, conn, if_exists="append", index=False,
            method="multi", chunksize=5000,
        )
```

- One transaction, `DELETE` in reverse FK order then `INSERT` in
  forward order. Idempotent and safe to rerun.
- `method="multi"` + `chunksize=5000` is the difference between seconds
  and minutes on a 113k-row fact table. Without it, pandas emits one
  `INSERT` per row.

### 5.6 Load Drive (`src/load_drive.py`)

Uploads the merged DataFrame as `merged_spotify_grammys.csv` to a given
Google Drive folder using **OAuth 2.0 user credentials** via
`google_auth_oauthlib.InstalledAppFlow`.

- Scope: `drive.file` (the narrowest scope — the app can only touch
  files it created itself).
- First run opens a browser for consent and persists a refresh token
  to `airflow/config/google_oauth_token.json`. Subsequent runs reuse
  that token with no browser.
- If a file with the target name already exists in the folder, it is
  **updated in place** instead of creating a duplicate.

**Why OAuth and not a Service Account?** Service accounts have no
personal Drive storage quota, so they cannot own files inside a
personal Gmail "My Drive" — the first attempt hit a
`storageQuotaExceeded` error. OAuth user credentials upload the file
as the authenticated user, which bypasses the quota issue entirely.

### 5.7 Orchestrator (`src/run_local.py`)

Plain Python, no Airflow. Chains the stages in order, logs row counts
between each, and gates the Drive upload on whether
`GDRIVE_FOLDER_ID` and the OAuth client file are both configured. Run
from the repo root:

```bash
python src/run_local.py
```

---

## 6. MySQL container bootstrap

The `mysql-dw` service in `airflow/docker-compose.yaml` mounts **three**
init scripts into `/docker-entrypoint-initdb.d/`, and they run in the
alphanumeric order shown:

```yaml
volumes:
  - mysql-dw-volume:/var/lib/mysql
  - ../sql/init_db_grammys.sql:/docker-entrypoint-initdb.d/01_init_db_grammys.sql:ro
  - ../sql/init_dw.sh:/docker-entrypoint-initdb.d/02_init_dw.sh:ro
  - ../sql/create_star_schema.sql:/docker-entrypoint-initdb.d/03_create_star_schema.sql:ro
```

1. **`01_init_db_grammys.sql`** — `USE grammys_src;` then
   `CREATE TABLE awards (...)` and `LOAD DATA INFILE` the CSV into it.
2. **`02_init_dw.sh`** — shell script (sourced, not executed) that
   creates the `grammys_dw` database and grants the application user
   access to it. Shell is required because we need the `MYSQL_DW_DB`
   env var expanded at runtime and the `docker_process_sql` helper
   that the mysql entrypoint only exposes inside sourced `.sh` files.
3. **`03_create_star_schema.sql`** — `USE grammys_dw;` then the DDL
   for all five star-schema tables.

### 6.1 Why `.sh` for step 2 and not `.sql`?

The official `mysql:8.0` image processes init files differently:

| Extension | How the entrypoint handles it              | Can read env vars? | Can `USE` arbitrary DBs? |
|-----------|--------------------------------------------|--------------------|--------------------------|
| `.sql`    | Piped into `mysql --database=$MYSQL_DATABASE` | No (no shell expansion) | No (fixed DB)         |
| `.sh`     | **Sourced** into the entrypoint's shell     | Yes                | Yes (via `docker_process_sql --database=mysql`) |

`.sql` files always run against `$MYSQL_DATABASE`, which is hard-coded
to `grammys_src`. To create a *different* database we need the shell
context. The script is tiny but it earns its keep by being the only
place where we can reach outside of `grammys_src` on first boot.

**Gotcha**: because the `.sh` file is *sourced*, any shell options it
sets persist in the entrypoint's own shell after the source returns.
An early version used `set -euo pipefail`, which propagated to the
entrypoint and crashed it on line 331 (`$1` with no default value under
`set -u`). The script now explicitly disables those flags and has a
comment explaining why.

### 6.2 Validating the bootstrap

```bash
docker compose -f airflow/docker-compose.yaml --project-directory airflow down -v
docker compose -f airflow/docker-compose.yaml --project-directory airflow up -d mysql-dw
# wait ~20s for init scripts to finish, then:
docker compose -f airflow/docker-compose.yaml --project-directory airflow exec mysql-dw \
  mysql -uadmin -p"$MYSQL_PASSWORD" -e "
    SELECT COUNT(*) FROM grammys_src.awards;
    SHOW TABLES FROM grammys_dw;
  "
```

Expected output:

```
COUNT(*)  = 4810
TABLES    = dim_album, dim_artist, dim_genre, dim_year, fact_track
```

---

## 7. Environment configuration

All credentials live in `airflow/.env` (gitignored). A template is at
`airflow/.env.example`. The **same file** is read by both
`docker-compose` (for the MySQL container) and by `src/config.py` (for
the Python pipeline), so there is exactly one source of truth.

```env
# --- Airflow ---
AIRFLOW_UID=1000

# --- MySQL (host-side port, so the local .venv can reach the container) ---
MYSQL_HOST=localhost
MYSQL_PORT=3307
MYSQL_USER=admin
MYSQL_ROOT_PASSWORD=<strong password>
MYSQL_PASSWORD=<strong password>
MYSQL_SRC_DB=grammys_src
MYSQL_DW_DB=grammys_dw

# --- Google Drive (OAuth user credentials) ---
GDRIVE_FOLDER_ID=<folder id from the Drive URL>
GOOGLE_OAUTH_CLIENT_PATH=./config/google_oauth_client.json
GOOGLE_OAUTH_TOKEN_PATH=./config/google_oauth_token.json
```

`config.py` resolves the two OAuth paths relative to the repo root, so
`./config/...` inside `.env` maps to
`airflow/config/google_oauth_client.json` on disk.

---

## 8. Google Drive (OAuth) setup

1. **GCP Console → OAuth consent screen**:
   - User Type: **External**
   - Add your own Gmail address under **Test users**. Without this,
     Google blocks the consent flow with an "unverified app" error.
2. **GCP Console → Credentials → Create OAuth client ID**:
   - Application type: **Desktop app**
   - Download the JSON and save it as
     `airflow/config/google_oauth_client.json`.
3. **Enable the Drive API** (APIs & Services → Library → Google Drive API).
4. **Create the target folder in Drive** and copy its ID from the URL
   (`drive.google.com/drive/folders/<ID>`). Put the ID in
   `GDRIVE_FOLDER_ID` in `airflow/.env`.
5. **First run** of `python src/run_local.py` opens a browser. You'll
   see a "Google hasn't verified this app" warning — this is normal for
   Testing-mode apps. Click **Advanced → Go to etl-workshop-2
   (unsafe)** and authorize. The script persists the refresh token to
   `airflow/config/google_oauth_token.json`; subsequent runs are
   non-interactive.

---

## 9. Running Phase A end-to-end

```bash
# 1. (First time) create and activate the venv, install Python deps
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Start the MySQL container (from the repo root)
docker compose -f airflow/docker-compose.yaml --project-directory airflow up -d mysql-dw

# 3. Run the pipeline
python src/run_local.py
```

Expected log tail:

```
Spotify raw: 114000 rows
Grammys raw: 4810 rows
Spotify clean: 113549 rows
Grammys clean: 4810 rows
Merged: 113549 rows
DW loaded
Drive upload complete
Pipeline complete
```

### 9.1 Sanity queries

```sql
USE grammys_dw;
SELECT COUNT(*) FROM fact_track;               -- ≈ 113 549
SELECT COUNT(*) FROM dim_artist WHERE grammy_wins > 0;
SELECT a.artist_name, AVG(f.popularity) AS avg_pop
FROM fact_track f JOIN dim_artist a USING (artist_key)
GROUP BY a.artist_name ORDER BY avg_pop DESC LIMIT 10;
```

---

## 10. Assumptions and decisions

- **Duplicate `track_id` values are kept**: the same track legitimately
  appears under multiple genres, and that drives the compound grain
  `(track_id, genre_key)` of the fact table. Dropping them would
  collapse a real dimension.
- **Full-row duplicates are removed**: the ~450 duplicates are
  artifacts of the source CSV (the `Unnamed: 0` index column hid them
  from `duplicated()`) and would violate the compound PK.
- **Artist match is best-effort, not fuzzy**: the join key is
  `lower(strip(first_artist))`. This under-counts Grammy nominations
  for collaborations listed second and for artists with punctuation
  variants. A fuzzy matcher would improve recall at the cost of a lot
  of complexity; out of scope for this workshop.
- **Every Grammy row has `winner = True`**: confirmed in the raw CSV.
  The `grammy_wins` and `grammy_nominations` aggregates therefore
  happen to be equal in the current dataset. KPIs that depend on the
  nominees-vs-winners distinction would need a different source.
- **Artist-level Grammy aggregates live on `dim_artist`**, not on the
  fact. They describe the artist, not any single track, and putting
  them on the dimension keeps `fact_track` slim and BI queries fast.
- **Surrogate keys are Python-generated**, not MySQL
  `AUTO_INCREMENT`. This keeps the loader idempotent (`DELETE` +
  `INSERT` in one transaction) and makes the DDL trivial.

---

## 11. Status

- [x] **Phase A — Local pipeline**: extract, clean, merge, transform,
      load DW, upload to Drive, orchestrator. Validated end-to-end.
- [ ] **Phase B — Airflow adaptation**: mount `src/` into the Airflow
      containers, write the DAG that wraps each stage in a
      `PythonOperator` and stages intermediate pickles in
      `airflow/data/`.
- [ ] **Phase C — Dashboard**: Looker Studio over MySQL via ngrok TCP,
      ≥3 KPIs and ≥3 charts combining both sources.
