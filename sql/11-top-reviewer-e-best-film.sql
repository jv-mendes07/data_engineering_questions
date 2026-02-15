--You are given three tables containing information about films, viewers, and their respective ratings. Your task is to:
--
--Identify the viewer who has rated the most number of films.
--
--If multiple viewers have the same highest number of ratings, return the viewer whose name is lexicographically smallest.
--
--Identify the film with the highest average rating in February 2020.
--
--If multiple films share the same highest average rating, return the one with the lexicographically smallest title.
--
--Table Schemas
--trb_films
--Column Name	Type	Description
--film_id	int	Primary key
--title	varchar	Name of the film
--trb_viewers
--Column Name	Type	Description
--viewer_id	int	Primary key
--full_name	varchar	Unique name of the viewer
--trb_films_ratings
--Column Name	Type	Description
--film_id	int	Foreign key to trb_films
--viewer_id	int	Foreign key to trb_viewers
--rating	int	Rating score given by the viewer
--review_date	date	Date when the rating was submitted
--Primary Key for trb_films_ratings is a composite of (film_id, viewer_id)
--
--Expected Output
--A single-column result set with the names:
--
--results
--Daniel
--Frozen 2
--Sample Input
--trb_films
--film_id	title
--1	Avengers
--2	Frozen 2
--3	Joker
--trb_viewers
--viewer_id	full_name
--1	Daniel
--2	Monica
--3	Maria
--4	James
--trb_films_ratings
--film_id	viewer_id	rating	review_date
--1	1	3	2020-01-12
--1	2	4	2020-02-11
--1	3	2	2020-02-12
--1	4	1	2020-01-01
--2	1	5	2020-02-17
--2	2	2	2020-02-01
--2	3	2	2020-03-01
--3	1	3	2020-02-22
--3	2	4	2020-02-25
--Explanation
--Most Active Viewer:
--Daniel and Monica both rated 3 films. Since “Daniel” comes before “Monica” alphabetically, Daniel is selected.
--
--Highest Rated Film (Feb 2020):
--
--Frozen 2 and Joker both have an average February 2020 rating of 3.5.
--
--“Frozen 2” comes before “Joker” lexicographically.
--So, Frozen 2 is the correct answer.

-- Write query here
-- TABLE NAME: `trb_viewers` and `trb_films_ratings` and `trb_films` 

WITH 
  base AS (
  SELECT 
    r.film_id,
    r.viewer_id,
    r.rating,
    r.review_date,
    v.full_name,
    f.title
  FROM trb_film_ratings AS r
  JOIN trb_viewers AS v 
  ON r.viewer_id = v.viewer_id
  JOIN trb_films AS f
  ON f.film_id = r.film_id
  ),
  user_more_ratings AS (
  SELECT 
    full_name,
    COUNT(DISTINCT film_id) AS films_rated,
    ROW_NUMBER() OVER(ORDER BY COUNT(DISTINCT film_id) DESC, full_name) AS row_n
  FROM base
  GROUP BY
    full_name
  ),
  film_with_more_rating AS (
  SELECT 
    title,
    AVG(rating) AS avg_rating,
    ROW_NUMBER() OVER(ORDER BY AVG(rating) DESC, title) AS row_n
  FROM base
  WHERE EXTRACT(YEAR FROM review_date) = 2020
  AND EXTRACT(MONTH FROM review_date) = 2
  GROUP BY title
  )
SELECT 
  full_name AS results
FROM user_more_ratings
WHERE row_n = 1
UNION ALL 
SELECT 
  title AS results
FROM film_with_more_rating
WHERE row_n = 1