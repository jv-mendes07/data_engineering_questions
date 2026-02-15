--You are given a table that tracks server session activity, where each row logs either a "start" or "stop" event for a server. Each session starts with a "start" and ends with a "stop".
--
--Your task is to calculate the total uptime in full days for all servers combined.
--The result should return a single column: total_uptime_days.
--
--🧾 Table: su_server_utilization
--Column Name	Data Type	Description
--server_id	INT	Unique identifier for each server
--status_tim	DATE	Timestamp indicating the event time
--session_status	STRING	Either "start" or "stop" for the session
--📦 Sample Input
--server_id	status_tim	session_status
--1	2022/08/02	start
--1	2022/08/04	stop
--2	2022/08/17	start
--2	2022/08/24	stop
--✅ Expected Output
--total_uptime_days
--9
--💡 Notes
--Each session consists of a start and stop event. You can assume all sessions are properly paired.
--
--Uptime for each session should be calculated as the number of full days between start and stop, inclusive of the start date but exclusive of the stop date.
--
--The output should return only one row with the total of all sessions.

-- Write your SQL here
-- Table Name: su_server_utilization

WITH
  next_status_time_server AS (
SELECT 
  *,
  LEAD(CASE 
        WHEN session_status = 'stop' THEN status_time
        ELSE NULL
    END) OVER (PARTITION BY server_id ORDER BY status_time) AS next_status_time
FROM su_server_utilization
  )
SELECT 
  SUM((next_status_time::DATE - status_time::DATE)) AS total_uptime_days
FROM next_status_time_server