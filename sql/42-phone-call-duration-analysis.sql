-- Phone Call Duration Analysis
-- Difficulty: MEDIUM | Companies | Hints

-- ---- Problem ----
-- You're provided with a table that contains records of calls made to a healthcare support
-- center. Each call is assigned to a specific category, but in some cases, the calls are either:
-- marked as 'n/a', or left blank/null. Objective: Your task is to calculate the percentage of
-- uncategorized calls out of the total number of calls.
--
-- Schema columns:
-- valid_call_logs: policy_holder_id, case_id, call_category, call_date,
-- call_duration_secs
--
-- Output columns: uncategorised_call_pct

-- ---- Examples ----
-- Example 1

-- Input:
-- valid_call_logs:
-- policy_holder_id | case_id                              | call_category        | call_date
-- 1                | f1d012f9-9d02-4966-a968-bf6c5b       | emergency assistance | 2023-04-13 19:16:53
-- 1                | 41ce8fb6-1ddd-4f50-ac31-07bfcc       | authorisation        | 2023-05-25 09:09:30
-- 2                | 9b1af84b-eedb-4c21-9730-6f099c       | n/a                  | 2023-01-26 01:21:27
-- 2                | 8471a3d4-6fc7-4bb2-9fc7-4583e3       | emergency assistance | 2023-03-09 10:58:54

-- Output:
-- uncategorised_call_pct
-- 20.0

-- ---- Constraints ----
-- - Handle NULL values appropriately
-- - Return results matching the expected output schema and order


-- Write query here
-- TABLE NAME: `valid_call_logs`

WITH 
    call_nulls AS (
SELECT 
    SUM(CASE 
            WHEN call_category = 'n/a' OR call_category IS NULL THEN 1 
            ELSE 0
        END) AS call_nulls,
    COUNT(*) AS calls
FROM valid_call_logs
    )
SELECT 
    (call_nulls::FLOAT / calls::FLOAT) * 100 AS uncategorised_call_pct
FROM call_nulls