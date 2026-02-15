--Amazon wants to optimize warehouse storage by prioritizing prime-eligible batches and efficiently using any remaining space for non-prime batches.
--
--Each batch (item) occupies a fixed square footage, and the total warehouse capacity is 500,000 square feet.
--
--Your task is to write a query or program to determine how many items of each type can be stored under the following constraints:
--
--📌 Constraints
--Maximize the number of prime_eligible items.
--
--Use any remaining space for not_prime items.
--
--Each batch allocation must be an integer.
--
--Each item has a fixed square_footage (from the input table).
--
--🧾 Table: mis_inventory
--item_id	item_type	item_category	square_footage
--1374	prime_eligible	mini refrigerator	68.00
--4245	not_prime	standing lamp	26.40
--2452	prime_eligible	television	85.00
--3255	not_prime	side table	22.60
--1672	prime_eligible	laptop	8.50
--✅ Expected Output
--item_type	item_count
--prime_eligible	9285
--not_prime	6
--🔍 Notes
--Your solution should maximize the number of prime_eligible items stored first.
--
--Then, from the remaining space, maximize the number of not_prime items.
--
--The item_count should reflect how many batches of each item_type can fit.

-- Write query here
-- TABLE NAME: `mis_inventory`

WITH 
  items_type_sqr AS (
  SELECT 
    item_type,
    SUM(mi.square_footage) AS square_footage,
    COUNT(DISTINCT item_id) AS items,
    MIN(mi.square_footage) AS min_square_footage
  FROM mis_inventory AS mi 
  GROUP BY 
    item_type
  ),
  prime_itens_batch AS (
SELECT 
  its.item_type,
  its.square_footage,
  its.items,
  (FLOOR(500000/ its.square_footage) * items)::INT AS item_count,
  500000 / items AS area_per_item
FROM items_type_sqr AS its 
WHERE its.item_type = 'prime_eligible'
  ),
  remaining_area_sqr AS (
SELECT 
  item_type,
  item_count,
  500000 - (item_count * (square_footage / items)) AS remaining_area
FROM prime_itens_batch
  )
SELECT 
  item_type,
  item_count
FROM remaining_area_sqr
UNION ALL
SELECT 
  item_type,
  (FLOOR((SELECT remaining_area FROM remaining_area_sqr) / its.square_footage) * items)::INT AS item_count
FROM items_type_sqr AS its 
WHERE its.item_type = 'not_prime'