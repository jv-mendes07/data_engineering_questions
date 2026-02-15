--You work for a customer analytics platform that tracks how users interact with your services and what kind of content they create. You are given two datasets:
--
--aca_customer_interactions: logs customer interaction events (clicks, views, likes, etc.)
--
--aca_user_content: stores the content posted by each customer (e.g., comments)
--
--Your goal is to determine the total number of interactions and the total number of content items created by each customer.
--
--📥 Input Data
--1. aca_customer_interactions
--Column Name	Data Type	Description
--interaction_id	Integer	Unique ID of the interaction event
--customer_id	Integer	ID of the customer who performed the action
--interaction_type	String	Type of interaction (click, view, like, etc.)
--interaction_date	Date	Date of the interaction
--2. aca_user_content
--Column Name	Data Type	Description
--content_id	Integer	Unique ID of the content item
--customer_id	Integer	ID of the customer who created it
--content_type	String	Type of content (e.g., comment)
--content_text	String	Text body of the content
--🎯 Expected Output
--Return a summary table with the following structure:
--
--Column Name	Data Type	Description
--customer_id	Integer	ID of the customer
--total_interactions	Integer	Total number of interactions performed by the user
--total_content	Integer	Total number of content items created by the user
--Include all customers that appear in either table.
--
--Customers with zero interactions or zero content items should still appear in the result with a count of 0 in the respective column.
--
--🧪 Sample Input
--aca_customer_interactions
--interaction_id	customer_id	interaction_type	interaction_date
--1	7	click	2024-02-13
--2	4	view	2024-01-25
--3	8	like	2024-02-18
--4	5	like	2024-01-27
--5	7	view	2024-02-28
--6	10	view	2024-02-11
--7	3	view	2024-01-28
--8	7	like	2024-02-29
--9	8	like	2024-01-16
--10	5	click	2024-01-15
--11	4	click	2024-02-16
--12	8	like	2024-02-20
--13	8	view	2024-02-13
--14	3	view	2024-02-24
--15	6	click	2024-02-21
--aca_user_content
--content_id	customer_id	content_type	content_text
--1	2	comment	hello world! this is a TEST.
--2	8	comment	what a great day
--3	4	comment	WELCOME to the event.
--4	2	comment	e-commerce is booming.
--5	6	comment	Python is fun!!
--✅ Expected Output
--customer_id	total_interactions	total_content
--7	3	0
--4	2	1
--8	4	1
--5	2	0
--10	1	0
--3	2	0
--6	1	1
--2	0	2
--💡 Explanation of Output
--Customer 7: 3 interactions (click, view, like); 0 content items
--
--Customer 4: 2 interactions (view, click); 1 content item (comment)
--
--Customer 8: 4 interactions (3 likes, 1 view); 1 content item (comment)
--
--Customer 2: 0 interactions; 2 content items (comments)
--
--All other rows follow the same logic.

-- Write query here
-- TABLE NAME: `aca_customer_interactions` and `aca_user_content`

SELECT 
  COALESCE(ci.customer_id, uc.customer_id) AS customer_id,
  COALESCE(COUNT(DISTINCT ci.interaction_id), 0) AS total_interactions,
  COALESCE(COUNT(DISTINCT uc.content_id), 0) AS total_content
FROM aca_customer_interactions AS ci
FULL OUTER JOIN aca_user_content AS uc 
ON ci.customer_id = uc.customer_id 
GROUP BY 
  ci.customer_id,
  uc.customer_id
ORDER BY COALESCE(ci.customer_id, uc.customer_id)