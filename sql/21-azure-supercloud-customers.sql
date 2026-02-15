--A Supercloud customer is defined as a customer who has purchased **at least one product from every product category available in the product catalog.
--
--Your task is to write a SQL query to identify the customer_id of all Supercloud customers.
--
--🧠 Objective
--Use the asc_customer_contracts table to track which products each customer has purchased.
--
--Use the asc_products table to get the category for each product.
--
--Determine which customers have made purchases covering all product categories listed in asc_products.
--
--Return a list of customer_ids who qualify as Supercloud customers.
--
--🛢️ Table 1: asc_customer_contracts
--Column Name	Data Type	Description
--customer_id	int	ID of the customer
--product_id	int	ID of the product purchased
--amount	numeric	Purchase amount
--🛢️ Table 2: asc_products
--Column Name	Data Type	Description
--product_id	int	ID of the product
--product_category	varchar	Category of the product
--product_name	varchar	Name of the product
--📥 Sample Input
--asc_customer_contracts
--customer_id	product_id	amount
--1	1	1000
--1	3	2000
--1	5	1500
--2	2	3000
--2	6	2000
--asc_products
--product_id	product_category	product_name
--1	Analytics	Azure Databricks
--2	Analytics	Azure Stream Analytics
--3	Compute	Elastic
--4	Containers	Azure Kubernetes Service
--5	Containers	Azure Service Fabric
--6	Compute	Virtual Machines
--7	Compute	Azure Functions
--📤 Expected Output
--customer_id
--1
--🔍 Explanation
--There are 3 product categories: Analytics, Compute, and Containers.
--
--Customer 1 has purchased products from all three categories:
--
--Product 1 → Analytics
--
--Product 3 → Compute
--
--Product 5 → Containers
--
--Customer 2 has only purchased from Analytics and Compute, so they are not a Supercloud customer.
--
--✅ Output Requirement
--Return a table with a single column:
--
--customer_id: the ID of each Supercloud customer

-- Write query here
-- TABLE NAME: asc_customer_contracts & asc_products

SELECT 
  acc.customer_id AS customer_id
FROM asc_customer_contracts AS acc
INNER JOIN asc_products AS ap 
ON acc.product_id = ap.product_id
GROUP BY acc.customer_id
HAVING COUNT(DISTINCT ap.product_category) = (SELECT COUNT(DISTINCT product_category) AS product_categories FROM asc_products)