-- ============================================================
-- Looker Studio queries for the Spotify + Grammys dashboard.
-- All queries run against grammys_dw (the star schema).
-- Copy-paste each query as a Custom Query data source in Looker.
-- ============================================================

USE grammys_dw;


-- ----------------------------------------------------------
-- KPI 1: General overview (scorecard cards)
-- Total tracks, unique artists, Grammy-linked artists, genres
-- ----------------------------------------------------------
SELECT
    COUNT(*)                                         AS total_tracks,
    COUNT(DISTINCT f.artist_key)                     AS unique_artists,
    COUNT(DISTINCT CASE WHEN a.grammy_wins > 0
                        THEN f.artist_key END)       AS grammy_artists,
    COUNT(DISTINCT f.genre_key)                      AS total_genres,
    ROUND(AVG(f.popularity), 1)                      AS avg_popularity
FROM fact_track f
JOIN dim_artist a USING (artist_key);


-- ----------------------------------------------------------
-- KPI 2: Top 10 artists by average popularity
-- Scatter or bar chart — shows who dominates streams
-- ----------------------------------------------------------
SELECT
    a.artist_name,
    a.grammy_nominations,
    a.grammy_wins,
    COUNT(*)              AS track_count,
    ROUND(AVG(f.popularity), 1) AS avg_popularity
FROM fact_track f
JOIN dim_artist a USING (artist_key)
GROUP BY a.artist_key, a.artist_name, a.grammy_nominations, a.grammy_wins
ORDER BY avg_popularity DESC
LIMIT 10;


-- ----------------------------------------------------------
-- KPI 3: Grammy winners vs non-winners — popularity comparison
-- Two scorecards or a grouped bar
-- ----------------------------------------------------------
SELECT
    CASE WHEN a.grammy_wins > 0 THEN 'Grammy Winner' ELSE 'No Grammy' END AS grammy_status,
    COUNT(DISTINCT f.artist_key)  AS artist_count,
    COUNT(*)                      AS track_count,
    ROUND(AVG(f.popularity), 1)   AS avg_popularity,
    ROUND(AVG(f.danceability), 3) AS avg_danceability,
    ROUND(AVG(f.energy), 3)       AS avg_energy
FROM fact_track f
JOIN dim_artist a USING (artist_key)
GROUP BY grammy_status;


-- ----------------------------------------------------------
-- Chart 1: Average audio features by genre (top 15 genres by track count)
-- Radar chart or grouped bar chart
-- ----------------------------------------------------------
SELECT
    g.genre_name,
    COUNT(*)                           AS track_count,
    ROUND(AVG(f.danceability), 3)      AS avg_danceability,
    ROUND(AVG(f.energy), 3)            AS avg_energy,
    ROUND(AVG(f.valence), 3)           AS avg_valence,
    ROUND(AVG(f.acousticness), 3)      AS avg_acousticness,
    ROUND(AVG(f.speechiness), 3)       AS avg_speechiness,
    ROUND(AVG(f.instrumentalness), 3)  AS avg_instrumentalness
FROM fact_track f
JOIN dim_genre g USING (genre_key)
GROUP BY g.genre_key, g.genre_name
ORDER BY track_count DESC
LIMIT 15;


-- ----------------------------------------------------------
-- Chart 2: Popularity distribution by Grammy nomination count
-- Scatter plot: x = grammy_nominations, y = avg_popularity
-- ----------------------------------------------------------
SELECT
    a.artist_name,
    a.grammy_nominations,
    a.grammy_wins,
    ROUND(AVG(f.popularity), 1)   AS avg_popularity,
    ROUND(AVG(f.danceability), 3) AS avg_danceability,
    ROUND(AVG(f.energy), 3)       AS avg_energy,
    COUNT(*)                      AS track_count
FROM fact_track f
JOIN dim_artist a USING (artist_key)
WHERE a.grammy_nominations > 0
GROUP BY a.artist_key, a.artist_name, a.grammy_nominations, a.grammy_wins
ORDER BY a.grammy_nominations DESC;


-- ----------------------------------------------------------
-- Chart 3: Energy vs Valence by genre (mood quadrant)
-- Scatter plot: x = avg_energy, y = avg_valence, color = genre
-- ----------------------------------------------------------
SELECT
    g.genre_name,
    ROUND(AVG(f.energy), 3)       AS avg_energy,
    ROUND(AVG(f.valence), 3)      AS avg_valence,
    ROUND(AVG(f.danceability), 3) AS avg_danceability,
    ROUND(AVG(f.popularity), 1)   AS avg_popularity,
    COUNT(*)                      AS track_count
FROM fact_track f
JOIN dim_genre g USING (genre_key)
GROUP BY g.genre_key, g.genre_name;


-- ----------------------------------------------------------
-- Chart 4: Danceability vs Energy colored by Grammy status
-- Scatter at artist level — are Grammy winners more energetic?
-- ----------------------------------------------------------
SELECT
    a.artist_name,
    CASE WHEN a.grammy_wins > 0 THEN 'Grammy Winner' ELSE 'No Grammy' END AS grammy_status,
    ROUND(AVG(f.danceability), 3) AS avg_danceability,
    ROUND(AVG(f.energy), 3)       AS avg_energy,
    ROUND(AVG(f.popularity), 1)   AS avg_popularity,
    COUNT(*)                      AS track_count
FROM fact_track f
JOIN dim_artist a USING (artist_key)
GROUP BY a.artist_key, a.artist_name, grammy_status
HAVING track_count >= 5;


-- ----------------------------------------------------------
-- Chart 5: Top 10 genres with highest Grammy representation
-- Bar chart — which genres produce the most Grammy winners?
-- ----------------------------------------------------------
SELECT
    g.genre_name,
    COUNT(DISTINCT f.artist_key)                                AS total_artists,
    COUNT(DISTINCT CASE WHEN a.grammy_wins > 0
                        THEN f.artist_key END)                  AS grammy_artists,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN a.grammy_wins > 0
                                     THEN f.artist_key END)
        / COUNT(DISTINCT f.artist_key), 1
    )                                                           AS grammy_pct
FROM fact_track f
JOIN dim_artist a USING (artist_key)
JOIN dim_genre  g USING (genre_key)
GROUP BY g.genre_key, g.genre_name
HAVING total_artists >= 20
ORDER BY grammy_pct DESC
LIMIT 10;


-- ----------------------------------------------------------
-- Chart 6: Explicit vs non-explicit tracks — popularity & features
-- Grouped bar or table
-- ----------------------------------------------------------
SELECT
    CASE WHEN f.explicit = 1 THEN 'Explicit' ELSE 'Clean' END AS content_type,
    COUNT(*)                          AS track_count,
    ROUND(AVG(f.popularity), 1)       AS avg_popularity,
    ROUND(AVG(f.danceability), 3)     AS avg_danceability,
    ROUND(AVG(f.energy), 3)           AS avg_energy,
    ROUND(AVG(f.speechiness), 3)      AS avg_speechiness,
    ROUND(AVG(f.valence), 3)          AS avg_valence
FROM fact_track f
GROUP BY content_type;
