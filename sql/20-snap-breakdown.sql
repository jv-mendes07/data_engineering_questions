--You are given two tables containing social media user activity logs and their corresponding age group segments. Each user performs various activities, such as sending, opening, or chatting. You are tasked with analyzing how different age groups divide their time between sending and opening content.
--
--🧠 Objective
--Write a SQL query to:
--
--Join the activity data with age group information.
--
--For each age group (age_bucket), calculate:
--
--The percentage of time spent sending content.
--
--The percentage of time spent opening content.
--
--Round both percentages to 2 decimal places.
--
--Ensure the percentages are calculated only from "send" and "open" activities.
--
--If an age group has no send/open activity, return NULL for both percentages.
--
--Sort the results by age_bucket (optional but often useful for readability).
--
--🛢 Tables
--Table 1: sb_activities
--Column Name	Data Type	Description
--activity_id	int	Unique identifier for the activity
--user_id	int	ID of the user performing the activity
--activity_type	varchar	Type of activity: send, open, chat, etc.
--time_spent	float	Time (in minutes) spent on the activity
--activity_date	datetime	Timestamp when the activity occurred
--Table 2: sb_age_breakdown
--Column Name	Data Type	Description
--user_id	int	User ID
--age_bucket	varchar	Age group of the user
--📥 Sample Input
--sb_activities
--activity_id	user_id	activity_type	time_spent	activity_date
--7274	123	open	4.50	2022-06-22 12:00:00
--2425	123	send	3.50	2022-06-22 12:00:00
--1413	456	send	5.67	2022-06-23 12:00:00
--1414	789	chat	11.00	2022-06-25 12:00:00
--2536	456	open	3.00	2022-06-25 12:00:00
--sb_age_breakdown
--user_id	age_bucket
--123	31-35
--456	26-30
--789	21-25
--📤 Expected Output
--age_bucket	send_perc	open_perc
--21-25	NULL	NULL
--26-30	65.40	34.60
--31-35	43.75	56.25
--🔍 Explanation
--For age group 31-35 (user 123):
--
--Total time = 3.5 (send) + 4.5 (open) = 8.0
--
--Send % = (3.5 / 8.0) × 100 = 43.75%
--
--Open % = (4.5 / 8.0) × 100 = 56.25%
--
--For age group 26-30 (user 456):
--
--Total time = 5.67 (send) + 3.0 (open) = 8.67
--
--Send % = (5.67 / 8.67) × 100 ≈ 65.40%
--
--Open % = (3.0 / 8.67) × 100 ≈ 34.60%
--
--For age group 21-25 (user 789):
--
--Only "chat" activity → No send/open activity
--
--Result: NULL for both percentages
--
--✅ Output Requirements
--Return a result with the following columns:
--
--age_bucket — Age group label
--
--send_perc — % time spent sending content (rounded to 2 decimal places)
--
--open_perc — % time spent opening content (rounded to 2 decimal places)

-- Write query here
-- TABLE NAME: sb_activities & sb_age_breakdown

SELECT 
  sab.age_bucket,
  ((SUM(CASE 
        WHEN sa.activity_type = 'send' THEN sa.time_spent
        ELSE 0
      END)::FLOAT / SUM(time_spent)::FLOAT) * 100)::NUMERIC(4, 2) AS send_perc,
  ((SUM(CASE 
        WHEN sa.activity_type = 'open' THEN sa.time_spent
        ELSE 0
      END)::FLOAT / SUM(time_spent)::FLOAT) * 100)::NUMERIC(4, 2) AS open_perc
FROM sb_age_breakdown AS sab
LEFT JOIN sb_activities AS sa 
ON sab.user_id = sa.user_id AND sa.activity_type IN ('open', 'send')
GROUP BY 
  sab.age_bucket
ORDER BY age_bucket;