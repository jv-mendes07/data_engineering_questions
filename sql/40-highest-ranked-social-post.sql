-- Highest Ranked Social Post
-- Difficulty: MEDIUM | Companies | Hints

-- ---- Problem ----
-- For each channel, identify the top 3 posts with the highest like counts. Assign ranks to
-- posts based on their like counts. When multiple posts have the same like count they
-- should share the same rank, and subsequent ranks should reflect this gap.
--
-- Schema columns:
-- trp_post: post_id, channel_id, created_at, likes, shares, comments
-- trp_channel: channel_id, channel_name, channel_type
--
-- Output columns: channel_name, post_id, created_at, likes, rank
--
-- Sort the results by channel_name, then rank.

-- ---- Examples ----
-- Example 1

-- Input:
-- trp_post:
-- post_id | channel_id | created_at | likes | shares | comments
-- 101     | 1          | 2024-08-16 | 0     | 10     | 7
-- 102     | 1          | 2024-08-07 | 0     | 18     | 6
-- 103     | 1          | 2024-07-24 | 11    | 3      | 7
-- 104     | 1          | 2024-09-13 | 3     | 1      | 7
-- 105     | 1          | 2024-09-07 | 38    | 0      | 11
-- 106     | 1          | 2024-08-10 | 0     | 11     | 8
-- 107     | 1          | 2024-08-19 | 0     | 9      | 11
-- 108     | 1          | 2024-08-08 | 0     | 14     | 13

-- trp_channel:
-- channel_id | channel_name | channel_type
-- 1          | TechNews     | news
-- 2          | GameStream   | gaming
-- 3          | SocialBuzz   | social_media
-- 4          | DailyUpdates | news
-- 5          | ProGamer     | gaming

-- Output:
-- channel_name | post_id | created_at | likes | rank
-- TechNews     | 105     | 2024-09-07 | 38    | 1
-- TechNews     | 103     | 2024-07-24 | 11    | 2
-- TechNews     | 104     | 2024-09-13 | 3     | 3

-- ---- Constraints ----
-- - Handle NULL values appropriately
-- - Return results matching the expected output schema and order

-- Write query here
-- TABLE NAME: `trp_channel` and `trp_post`

WITH 
    rank_posts AS (
SELECT 
    tc.channel_name,
    tp.post_id,
    tp.created_at,
    tp.likes,
    RANK() OVER (PARTITION BY tc.channel_name ORDER BY likes DESC) AS rank
FROM trp_post AS tp 
INNER JOIN trp_channel AS tc 
ON tp.channel_id = tc.channel_id
WHERE tp.likes > 0
)
SELECT 
    channel_name,
    post_id,
    created_at,
    likes,
    rank
FROM rank_posts 
WHERE rank <= 3
ORDER BY channel_name ASC, rank ASC;