--A group of passengers is waiting in line to board a bus. However, the bus has a maximum weight capacity of 1000 kilograms. Passengers board one at a time based on their boarding order, and the boarding process stops as soon as the next passenger would cause the total weight to exceed the limit.
--
--🧠 Objective
--Write a SQL query to return the passenger_name of the last person who can board the bus without exceeding the 1000 kg weight limit.
--
--📌 Assumptions
--Passengers board in the order defined by boarding_order, starting from 1.
--
--Each passenger has a defined weight in kilograms (weight_kg).
--
--The total weight onboard should not exceed 1000 kg at any point.
--
--The test case guarantees that at least the first passenger can board.
--
--🛢️ Table: mcb_sample
--Column Name	Data Type	Description
--passenger_id	int	Unique ID of each passenger
--passenger_name	varchar	Name of the passenger
--weight_kg	int	Weight of the passenger in kilograms
--boarding_order	int	The order in which the passenger boards
--📥 Sample Input
--passenger_id	passenger_name	weight_kg	boarding_order
--5	Alice	250	1
--4	Bob	175	5
--3	Alex	350	2
--6	John Cena	400	3
--1	Winston	500	6
--2	Marie	200	4
--📤 Expected Output
--passenger_name
--John Cena
--🔍 Explanation
--Passengers board in the following order:
--
--Boarding Order	Name	Weight	Cumulative Weight
--1	Alice	250	250
--2	Alex	350	600
--3	John Cena	400	1000
--4	Marie	200	1200 ⛔ Exceeds
--The total reaches exactly 1000 kg after John Cena boards.
--
--Marie would exceed the limit, so she and all passengers after her are skipped.
--
--Therefore, the query should return "John Cena".
--
--✅ Output Requirements
--Return a single-column result:
--
--passenger_name: The name of the last person who boarded without exceeding the weight limit.

-- Write query here
-- TABLE NAME: `mcb_sample`

WITH 
  weight_cummulative AS (
SELECT 
  passenger_id,
  passenger_name,
  weight_kg,
  boarding_order,
  SUM(weight_kg) OVER(ORDER BY boarding_order ASC) AS weight_sum
FROM mcb_sample
  )
SELECT 
  passenger_name
FROM weight_cummulative
WHERE weight_sum <= 1000
ORDER BY weight_sum DESC 
LIMIT 1