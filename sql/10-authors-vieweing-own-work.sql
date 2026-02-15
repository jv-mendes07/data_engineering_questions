--You are given a table that tracks article views on a publishing platform. Each article has a specific author_id, and each view is recorded with the viewer_id and view_date.
--
--📌 Objective
--Write a SQL query to retrieve a list of authors who have viewed at least one of their own articles.
--
--✅ Output Requirements
--Return only distinct author IDs who viewed their own articles.
--
--The result should be sorted in ascending order.
--
--The output column should be named id.
--
--🛢️ Table: avow_sample
--Column Name	Data Type	Description
--article_id	int	Unique ID for the article
--author_id	int	ID of the user who wrote the article
--viewer_id	int	ID of the user who viewed the article
--view_date	date	Date when the article was viewed
--A row where author_id = viewer_id indicates that the author viewed their own article.
--
--Duplicate rows are allowed in the dataset.
--
--📥 Sample Input
--article_id	author_id	viewer_id	view_date
--1	3	5	2019-08-01
--1	3	6	2019-08-02
--2	7	7	2019-08-01
--2	7	6	2019-08-02
--4	7	1	2019-07-22
--3	4	4	2019-07-21
--3	4	4	2019-07-21
--📤 Expected Output
--id
--4
--7
--🔍 Explanation
--Author 7 viewed their own article (viewer_id = 7, author_id = 7)
--
--Author 4 also viewed their own article twice.
--
--Author 3 did not view their own article.
--
--Duplicates are ignored; only distinct author IDs are returned.

-- Write query here
-- TABLE NAME: `avow_sample`

SELECT 
  DISTINCT
    author_id AS id 
FROM avow_sample
WHERE author_id = viewer_id
ORDER BY author_id DESC;
