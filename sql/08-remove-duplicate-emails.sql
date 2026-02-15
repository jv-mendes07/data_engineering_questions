--You are given a table person that contains email addresses. Some emails may appear multiple times with different id values. Your task is to write a query that removes duplicates, keeping only the record with the smallest id for each unique email.
--
--⚠️ Since we do not support DELETE, you must write a SELECT query that returns only the deduplicated result without modifying the original table.
--
--Table Schema
--Table name: rde_person
--
--Column Name	Type
--id	int
--email	varchar
--id is the primary key.
--
--email consists of only lowercase letters.
--
--Example Input
--person
--
--id	email
--1	john@example.com
--2	bob@example.com
--3	john@example.com
--Expected Output
--id	email
--1	john@example.com
--2	bob@example.com
--🔍 Goal
--Write a query that returns each unique email address once, and keeps the lowest id for that email.

-- Write query here
-- TABLE NAME: `rde_person`

WITH 
  deduplication AS (
SELECT 
  id,
  email,
  ROW_NUMBER() OVER (PARTITION BY email ORDER BY id ASC) AS row_n
FROM rde_person
  )
SELECT 
  id,
  email
FROM deduplication 
WHERE row_n = 1
ORDER BY id ASC