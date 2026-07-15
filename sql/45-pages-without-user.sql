-- Pages Without User Likes
-- Difficulty: EASY | Companies

-- ---- Problem ----
-- You are given two tables with data related to Facebook Pages and the users who liked
-- them. Write a SQL query to find the IDs of all Facebook pages that have not received any
-- likes. The output should be sorted in ascending order by page ID.
--
-- Schema columns:
-- nlp_page_likes: user_id, page_id, liked_date
-- nlp_pages: page_id, page_name
--
-- Output columns: page_id

-- ---- Examples ----
-- Example 1

-- Input:
-- nlp_page_likes:
-- user_id | page_id | liked_date
-- 111     | 20001   | 2022-04-08 00:00:00
-- 121     | 20045   | 2022-03-12 00:00:00
-- 156     | 20001   | 2022-07-25 00:00:00

-- nlp_pages:
-- page_id | page_name
-- 20001   | SQL Masters
-- 20045   | Brain Boosters
-- 20701   | Insights for Analysts

-- Output:
-- page_id
-- 20701

-- ---- Constraints ----
-- - Handle NULL values appropriately
-- - Return results matching the expected output schema and order

-- Write query here
-- TABLE NAME: `nlp_pages` & `nlp_page_likes`
SELECT 
    np.page_id
FROM nlp_pages AS np 
EXCEPT 
SELECT 
    npl.page_id
FROM nlp_page_likes AS npl;