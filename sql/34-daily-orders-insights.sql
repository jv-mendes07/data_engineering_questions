--Daily Orders Insights
--
--You are working with two datasets:
--
--One dataset records user sessions.
--
--The other dataset logs product orders.
--
--Your task is to identify users who initiated a session and placed one or more orders on the same day. For these users, you must compute:
--
--The total number of orders placed on that day
--The cumulative value of those orders on that same session day
--Goal

--Return a result containing the following columns:
--
--user_id
--The unique identifier of the user
--session_date
--The date when the session occurred
--total_orders
--The total number of orders placed by the user on that day
--total_order_value
--The sum of the values of those orders
--Input Table 1: doi_session
--Column Name	Data Type	Description
--session_id	INT	Unique ID for each session
--user_id	INT	Unique ID of the user
--session_date	DATETIME	Date the session occurred
--Input Table 2: doi_orders
--Column Name	Data Type	Description
--order_id	INT	Unique ID for each order
--user_id	INT	Unique ID of the user
--order_value	INT	Value of the order
--order_date	DATETIME	Date the order was placed
--Key Requirement
--
--Only include records where:

--A user has a session on a given day
--The same user placed at least one order on that same day

--This implies a date-level match between session_date and order_date.

--Expected Output Example
--user_id	session_date	total_orders	total_order_value
--1	2024-01-01	1	152
--2	2024-01-02	1	485
--3	2024-01-05	3	870
--4	2024-01-03	2	277
--5	2024-01-04	3	479

-- Write query here
-- TABLE NAME: `doi_orders` and `doi_session`

SELECT 
  ds.user_id,
  ds.session_date,
  COUNT(DISTINCT o.order_id) AS total_orders,
  SUM(o.order_value) / COUNT(DISTINCT o.order_id) AS total_order_value
FROM doi_session AS ds 
INNER JOIN doi_orders AS o 
ON ds.user_id = o.user_id
GROUP BY 
  ds.user_id,
  ds.session_date