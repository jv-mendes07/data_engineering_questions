--You are given a table of user posts. For each user who posted at least twice in 2021, calculate the number of days between their first and last post during the year 2021.
--
--📦 Table: post_gap_posts
--Column	Type	Description
--user_id	INTEGER	ID of the user
--post_id	INTEGER	Unique ID of the post
--post_content	TEXT	Content of the post
--post_date	TIMESTAMP	Date and time of the post
--📋 Sample Input
--user_id	post_id	post_content          	post_date          
--101    	1      	First post of 2021    	2021-01-01 08:00:00  
--202    	2      	Second post of 2021    	2021-01-05 09:30:00  
--303    	3      	Another post in 2021  	2021-01-07 18:00:00  
--101    	4      	Follow-up post        	2021-12-30 15:00:00  
--202    	5      	More updates          	2021-12-31 10:00:00  
--303    	6      	Enjoying life          	2021-01-15 22:00:00  
--101    	7      	Final post of the year	2021-12-31 23:59:00  
--✅ Expected Output
--Return a list of users who posted 2 or more times in 2021, along with the number of days between their first and last post of that year.
--
--user_id	days_between
--101    	364          
--202    	360          
--303    	8            
--💡 Notes:
--Only consider posts made in the year 2021.
--
--Calculate the date difference (in full days) between the earliest and latest post timestamps for qualifying users.
--
--Ignore users who posted only once in 2021.

-- Write query here
-- TABLE NAME: `post_gap_posts`

WITH 
  posts_user AS (
SELECT 
  user_id,
  MIN(post_date) AS first_post,
  MAX(post_date) AS last_post
FROM post_gap_posts
WHERE 
  EXTRACT(YEAR FROM post_date) = 2021
GROUP BY 
  user_id
  )
SELECT 
  user_id,
  EXTRACT(DAY FROM (last_post - first_post)) AS days_between
FROM posts_user;
  