--You are given a table named duplicate_product_listing that contains product listings from various vendors on an e-commerce platform.
--Your task is to write a SQL query to find the number of vendors who have posted duplicate product listings.
--
--📌 Definition of a Duplicate Listing:
--A duplicate product listing is defined as two or more listings from the same vendor that have the same product_name and description.
--
--📦 Table: duplicate_product_listing
--Column Name	Type	Description
--listing_id	INTEGER	Unique ID for the product listing
--vendor_id	INTEGER	ID of the vendor who posted the listing
--product_name	STRING	Name of the product
--description	STRING	Description of the product
--📋 Sample Input
--listing_id	vendor_id	product_name	description
--101	501	Bluetooth Speaker	A portable speaker with Bluetooth connectivity and a battery life of 10 hours.
--102	501	Bluetooth Speaker	A portable speaker with Bluetooth connectivity and a battery life of 10 hours.
--103	502	Wireless Earbuds	Comfortable earbuds with high-quality sound and noise cancellation.
--104	502	Wireless Earbuds	Comfortable earbuds with high-quality sound and noise cancellation.
--105	503	Smart Watch	A smartwatch with fitness tracking, heart rate monitoring, and notifications support.
--✅ Expected Output
--duplicate_vendors
--2
--💡 Explanation:
--Vendor 501 has two identical listings for "Bluetooth Speaker."
--
--Vendor 502 has two identical listings for "Wireless Earbuds."
--
--Vendor 503 has only one unique listing.
--
--Thus, 2 vendors have at least one duplicate listing.

-- Write SQL Query here
-- Table name `duplicate_product_listing`

WITH 
  duplicated_listing AS (
  SELECT 
    vendor_id
  FROM duplicate_product_listing
  GROUP BY 
    vendor_id,
    product_name, 
    description
  HAVING COUNT(*) > 1
  )
SELECT 
  COUNT(DISTINCT vendor_id) AS duplicate_vendors
FROM duplicated_listing