--You are given a table containing user transactions for different products. Your task is to write a SQL query to calculate the year-on-year (YoY) growth rate for the total spend of each product.
--
--📝 Requirements
--For each product and year:
--
--Show the year (in ascending order)
--
--Show the product ID
--
--Show the current year's total spend
--
--Show the previous year's total spend
--
--Calculate the YoY growth percentage, using the formula:
--
--
--Round the YoY rate to 2 decimal places
--
--If no previous year exists, show NULL for prev_year_spend and yoy_rate
--
--📦 Table: ga_user_transactions
--transaction_id	product_id	spend	transaction_date
--1341	123424	1500.60	12/31/2019
--1423	123424	1000.20	12/31/2020
--1623	123424	1246.44	12/31/2021
--1322	123424	2145.32	12/31/2022
--✅ Expected Output
--year	product_id	curr_year_spend	prev_year_spend	yoy_rate
--2019	123424	1500.60	NULL	NULL
--2020	123424	1000.20	1500.60	-33.35
--2021	123424	1246.44	1000.20	24.62
--2022	123424	2145.32	1246.44	72.12

-- Write query here
-- TABLE NAME: ga_user_transactions

WITH 
  spend_by_product AS (
SELECT 
  EXTRACT(YEAR FROM transaction_date) AS year,
  product_id,
  SUM(spend) AS curr_year_spend
FROM ga_user_transactions
GROUP BY 
  year,
  product_id
  ),
  previous_spend_year AS (
  SELECT 
    *,
    LAG(curr_year_spend) OVER (PARTITION BY product_id ORDER BY year) AS prev_year_spend
  FROM spend_by_product AS sbp
  )
SELECT 
  *,
  (((curr_year_spend - prev_year_spend) / prev_year_spend) * 100)::numeric(5, 2) AS yoy_rate
FROM previous_spend_year AS psy