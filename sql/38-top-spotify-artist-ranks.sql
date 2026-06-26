-- Top Spotify Artist Rankings
-- Difficulty: MEDIUM | Companies

-- ---- Problem ----
-- Assume there are three Spotify tables: ar_artists, ar_songs, and ar_global_song_rank,
-- containing information about the artists, songs, and music charts, respectively. Write a
-- query to find the Top 5 artists whose songs appear most frequently in the Top 10 of the
-- global song rank. Display the top 5 artist names in ascending order, along with their song
-- appearance ranking.
--
-- Schema columns:
-- ar_artists: artist_id, artist_name, label_owner
-- ar_global_song_rank: day, song_id, rank
-- ar_songs: song_id, artist_id, name
--
-- Output columns: artist_name, artist_rank

-- ---- Examples ----
-- Example 1

-- Input:
-- ar_artists:
-- artist_id | artist_name | label_owner
-- 101       | Ed Sheeran  | Warner Music Group
-- 120       | Drake       | Warner Music Group
-- 125       | Bad Bunny   | Rimas Entertainment

-- ar_global_song_rank:
-- day | song_id | rank
-- 1   | 45202   | 5
-- 3   | 45202   | 2
-- 1   | 19960   | 3
-- 9   | 19960   | 15

-- ar_songs:
-- song_id | artist_id | name
-- 55511   | 101       | Perfect
-- 45202   | 101       | Shape of You
-- 22222   | 120       | One Dance
-- 19960   | 120       | Hotline Bling

-- Output:
-- artist_name | artist_rank
-- Ed Sheeran  | 1
-- Drake       | 2

-- ---- Constraints ----
-- - Handle NULL values appropriately
-- - Return results matching the expected output schema and order

-- Write query here
-- TABLE NAME: `ar_artists`, `ar_songs` and `ar_global_song_rank`

WITH 
    frequency_artists_songs AS (
SELECT 
    aa.artist_name,
    COUNT(*) AS frequency
FROM ar_global_song_rank AS agsr
INNER JOIN ar_songs AS aso
ON agsr.song_id = aso.song_id
INNER JOIN ar_artists AS aa 
ON aso.artist_id = aa.artist_id
WHERE rank <= 10
GROUP BY aa.artist_name
    )
SELECT 
    artist_name,
    ROW_NUMBER() OVER (ORDER BY frequency DESC) AS artist_rank
FROM frequency_artists_songs
ORDER BY frequency DESC 
LIMIT 5