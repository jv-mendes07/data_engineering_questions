--You are tasked with identifying the top 3 areas with the highest customer density based on transaction and store data.
--
--📌 Definition
--Customer Density is defined as:
--
--​
--
--You need to return the area name and its calculated customer density.
--If multiple areas have the same density, they should share the same rank.
--📥 Input Tables
--1. tad_transactions
--Column Name	Data Type	Description
--transaction_id	Integer	Unique ID of the transaction
--customer_id	Integer	ID of the customer
--transaction_amount	Float	Amount of the transaction
--transaction_date	Date	Date of the transaction
--store_id	Integer	Foreign key referencing the store
--2. tad_store_info
--Column Name	Data Type	Description
--store_id	Integer	Unique ID of the store
--store_location	String	Location of the store
--store_open_date	Date	Date the store opened
--area_name	String	Name of the area where the store is
--area_size	Integer	Size of the area (used in calculation)
--✅ Output Requirements
--Return a table with the following columns:
--
--Column Name	Description
--area_name	Name of the area
--customer_density	Calculated customer density (as per formula)
--Return only the top 3 areas with the highest density.
--
--If multiple areas have the same density, they must be ranked equally (i.e., use dense ranking logic).
--
--🧪 Sample Output
--area_name	customer_density
--Area C	0.0714285714
--Area A	0.0578512397
--Area D	0.03125

-- Write query here
-- TABLE NAME: `tad_store_info` or `tad_transactions`

WITH 
  customer_density AS (
  SELECT 
    tsi.area_name,
    (COUNT(DISTINCT tt.customer_id)::FLOAT / tsi.area_size::FLOAT)  AS customer_density,
    DENSE_RANK() OVER (ORDER BY (COUNT(DISTINCT tt.customer_id)::FLOAT / tsi.area_size::FLOAT) DESC, tsi.area_name ASC) AS rank
  FROM tad_transactions AS tt 
  INNER JOIN tad_store_info AS tsi 
  ON tt.store_id = tsi.store_id
  GROUP BY 
    tsi.area_name,
    tsi.area_size
  )
SELECT 
  area_name,
  customer_density
FROM customer_density
WHERE rank <= 3