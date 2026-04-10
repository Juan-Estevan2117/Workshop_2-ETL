-- This script runs on first boot of the mysql-dw container.
-- It creates the grammys_src database and the raw awards table inside it.

CREATE DATABASE IF NOT EXISTS grammys_src;
USE grammys_src;

CREATE TABLE IF NOT EXISTS `awards` (
  `year` int DEFAULT NULL,
  `title` text,
  `published_at` text,
  `updated_at` text,
  `category` text,
  `nominee` text,
  `artist` text,
  `workers` text,
  `img` text,
  `winner` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- The CSV is mounted by docker-compose at /seed/the_grammy_awards.csv,
-- and the server is started with --secure-file-priv=/seed so LOAD DATA
-- is allowed from that path.
LOAD DATA INFILE '/seed/the_grammy_awards.csv'
INTO TABLE awards
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES; 