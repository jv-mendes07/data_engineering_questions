-- Top Contributor Identification
-- Difficulty: EASY | Companies | Hints

-- ---- Problem ----
-- You are part of the recruitment team tasked with shortlisting candidates for a Data Analyst
-- role. The job requires proficiency in Python, Tableau, and PostgreSQL. Write an SQL query
-- to identify candidates who meet all the skill requirements for the role.
--
-- Schema columns:
-- dahc_candidates_skills: candidate_id, skill
--
-- Output columns: candidate_id
--
-- Sort the results by candidate_id ASC.
--
-- Schema: 1 table
-- dahc_candidates_skills - 2 cols

-- ---- Examples ----

-- Input:
-- dahc_candidates_skills:
-- candidate_id | skill
-- 101          | Python
-- 101          | Tableau
-- 101          | PostgreSQL
-- 202          | R

-- Output:
-- candidate_id
-- 101

-- ---- Constraints ----
-- - Handle NULL values appropriately
-- - Return results matching the expected output schema and order


-- Write your query here
-- Table name `dahc_candidates_skills`
SELECT 
    candidate_id
FROM dahc_candidates_skills
WHERE skill IN ('Python', 'Tableau', 'PostgreSQL')
GROUP BY candidate_id
HAVING COUNT(DISTINCT skill) = 3