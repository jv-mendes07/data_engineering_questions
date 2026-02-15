--You are given two tables that track user signups and their confirmation request history. A confirmation is either successful ('confirmed') or results in a timeout ('timeout').
--
--🧠 Objective
--Write a SQL query to calculate the confirmation rate for each user. The confirmation rate is defined as:
--
--🧮 confirmation rate = confirmed messages / total confirmation requests
--
--📌 Notes
--If a user has not made any confirmation requests, their confirmation rate should be 0.00.
--
--The confirmation rate must be rounded to 2 decimal places.
--
--Return results ordered by:
--
--confirmation_rate in ascending order
--
--Then by user_id in ascending order (as a tie-breaker)
--
--🛢️ Table: csa_signups
--Column Name	Data Type	Description
--user_id	int	Unique identifier for the user
--time_stamp	datetime	Signup time of the user
--🛢️ Table: csa_confirmations
--Column Name	Data Type	Description
--user_id	int	ID of the user who requested a confirmation
--time_stamp	datetime	Timestamp of the confirmation request
--action	ENUM	'confirmed' or 'timeout'
--📥 Sample Input
--csa_signups
--user_id	time_stamp
--3	2020-03-21 10:16:13
--7	2020-01-04 13:57:59
--2	2020-07-29 23:09:44
--6	2020-12-09 10:39:37
--csa_confirmations
--user_id	time_stamp	action
--3	2021-01-06 03:30:46	timeout
--3	2021-07-14 14:00:00	timeout
--7	2021-06-12 11:57:29	confirmed
--7	2021-06-13 12:58:28	confirmed
--7	2021-06-14 13:59:27	confirmed
--2	2021-01-22 00:00:00	confirmed
--2	2021-02-28 23:59:59	timeout
--📤 Expected Output
--user_id	confirmation_rate
--6	0.00
--3	0.00
--2	0.50
--7	1.00
--🔍 Explanation
--User 6 has no confirmation activity → rate = 0.00
--
--User 3 had 2 timeouts, no confirmed → rate = 0.00
--
--User 2 had 1 confirmed, 1 timeout → rate = 1 / 2 = 0.50
--
--User 7 had 3 confirmed, 0 timeout → rate = 3 / 3 = 1.00
--
--✅ Output Requirements
--Return a result with the following columns:
--
--user_id: ID of the user
--
--confirmation_rate: Percentage of confirmations (rounded to 2 decimals)

--- Write query here
-- TABLE NAME: csa_confirmations and csa_signups

SELECT 
  cs.user_id,
  (SUM(CASE 
        WHEN cc.action = 'confirmed' THEN 1
        ELSE 0
      END)::FLOAT/COUNT(*)::FLOAT)::NUMERIC(5, 2) AS confirmation_rate
FROM csa_signups AS cs 
LEFT JOIN csa_confirmations AS cc
ON cc.user_id = cs.user_id
GROUP BY 
  cs.user_id
ORDER BY cs.user_id ASC, confirmation_rate ASC