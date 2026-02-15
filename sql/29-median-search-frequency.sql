--Google’s Data Analytics team is preparing a marketing campaign and wants to analyze user search behavior. You are given a summary table that shows how many users performed a certain number of searches last year.
--
--Your task is to write a SQL query to compute the median number of searches per user, based on the distribution in the table.
--
--🎯 Requirements:
--Compute the median number of searches per user.
--
--Round the result to one decimal place.
--
--The table gives aggregated data: each row tells you how many users performed a given number of searches.
--
--You need to simulate a full list of users using the num_users counts to correctly calculate the median.
--
--📘 Table: msf_search_frequency
--searches	num_users
--1	2
--2	2
--3	3
--4	1
--✅ Output:
--median
--2.5
--💡 Explanation:
--There are 8 users in total:
--Two users searched 1 time, two searched 2 times, three searched 3 times, and one searched 4 times.
--The sorted list of all search counts:
--[1, 1, 2, 2, 3, 3, 3, 4]
--The median is the average of the 4th and 5th values: (2 + 3) / 2 = 2.5

-- Write query here
-- TABLE NAME: msf_search_frequency

WITH 
  search_users AS (
  SELECT 
    GENERATE_SERIES(
    1,
    (SELECT SUM(num_users) FROM msf_search_frequency)) AS user_id 
)
SELECT 
  (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY searches))::numeric(4,1) AS median
FROM msf_search_frequency
CROSS JOIN search_users