-- Star schema for the Spotify + Grammys data warehouse.
-- Creates the grammys_dw database and the star schema tables inside it.
-- Drops are issued in reverse FK order so the script is re-runnable.

CREATE DATABASE IF NOT EXISTS grammys_dw;
USE grammys_dw;

DROP TABLE IF EXISTS fact_track;
DROP TABLE IF EXISTS dim_album;
DROP TABLE IF EXISTS dim_artist;
DROP TABLE IF EXISTS dim_genre;
DROP TABLE IF EXISTS dim_year;

CREATE TABLE dim_artist (
  artist_key          INT            NOT NULL,
  artist_norm         VARCHAR(255)   NOT NULL COLLATE utf8mb4_bin,
  artist_name         VARCHAR(512)   NOT NULL,
  grammy_nominations  INT            NOT NULL DEFAULT 0,
  grammy_wins         INT            NOT NULL DEFAULT 0,
  first_grammy_year   INT            NOT NULL DEFAULT 0,
  last_grammy_year    INT            NOT NULL DEFAULT 0,
  PRIMARY KEY (artist_key),
  UNIQUE KEY uk_dim_artist_norm (artist_norm)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE dim_album (
  album_key    INT            NOT NULL,
  album_name   VARCHAR(512)   NOT NULL,
  artist_norm  VARCHAR(255)   NOT NULL,
  PRIMARY KEY (album_key),
  KEY ix_dim_album_natural (album_name(255), artist_norm)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE dim_genre (
  genre_key   INT           NOT NULL,
  genre_name  VARCHAR(64)   NOT NULL,
  PRIMARY KEY (genre_key),
  UNIQUE KEY uk_dim_genre_name (genre_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE dim_year (
  year_key    INT  NOT NULL,
  year_value  INT  NOT NULL,
  PRIMARY KEY (year_key),
  UNIQUE KEY uk_dim_year_value (year_value)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE fact_track (
  track_id          VARCHAR(32)       NOT NULL,
  artist_key        INT               NOT NULL,
  album_key         INT               NOT NULL,
  genre_key         INT               NOT NULL,
  year_key          INT               NOT NULL,
  popularity        TINYINT UNSIGNED  NOT NULL,
  duration_ms       INT               NOT NULL,
  explicit          BOOLEAN           NOT NULL,
  danceability      FLOAT             NOT NULL,
  energy            FLOAT             NOT NULL,
  music_key         TINYINT           NOT NULL,
  loudness          FLOAT             NOT NULL,
  mode              TINYINT           NOT NULL,
  speechiness       FLOAT             NOT NULL,
  acousticness      FLOAT             NOT NULL,
  instrumentalness  FLOAT             NOT NULL,
  liveness          FLOAT             NOT NULL,
  valence           FLOAT             NOT NULL,
  tempo             FLOAT             NOT NULL,
  time_signature    TINYINT           NOT NULL,
  PRIMARY KEY (track_id, genre_key),
  KEY ix_fact_artist (artist_key),
  KEY ix_fact_album  (album_key),
  KEY ix_fact_year   (year_key),
  CONSTRAINT fk_fact_artist FOREIGN KEY (artist_key) REFERENCES dim_artist (artist_key),
  CONSTRAINT fk_fact_album  FOREIGN KEY (album_key)  REFERENCES dim_album  (album_key),
  CONSTRAINT fk_fact_genre  FOREIGN KEY (genre_key)  REFERENCES dim_genre  (genre_key),
  CONSTRAINT fk_fact_year   FOREIGN KEY (year_key)   REFERENCES dim_year   (year_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
