--You’re working as a Data Engineer at a company that builds Customer Relationship Management (CRM) software. Your goal is to build a unified view that shows order details along with customer and product information — useful for internal dashboards and reporting.
--
--You are given three datasets (or tables) that store customer, order, and product details.
--
--🎯 Task
--Write a function (or query) that:
--
--Joins the customer, order, and product data.
--
--Creates a new column customer_name by concatenating first_name and last_name with a space in between.
--
--Returns a new DataFrame (or result set) with the following reordered column layout:
--category, customer_name, email, order_date, order_id, product_name
--
--🧩 Input Data
--✅ For Python, PySpark, Scala: the DataFrames are named
--customers, orders, products
--
--✅ For SQL/dbt: the table names are
--crm_customers, crm_orders, crm_products
--
--🗃️ Schema Details
--📋 customers
--Column Name	Data Type
--customer_id	Integer
--first_name	String
--last_name	String
--email	String
--🧾 orders
--Column Name	Data Type
--order_id	Integer
--customer_id	Integer
--product_id	Integer
--order_date	Date
--📦 products
--Column Name	Data Type
--product_id	Integer
--product_name	String
--category	String
--✅ Expected Output Schema
--The output should contain:
--
--Column Name	Data Type
--order id	Integer
--customer name	String
--customer email	String
--product name	String
--product category	String
--order date	String
--⚠️ Note: The column order matters — structure your output exactly as shown above.
--
--🔍 Example Input
--customers
--customer_id	first_name	last_name	email
--1	John	Doe	john.doe@email.com
--2	Jane	Smith	jane.smith@email.com
--orders
--order_id	customer_id	product_id	order_date
--1001	1	101	2023-01-10
--1002	2	102	2023-01-11
--products
--product_id	product_name	category
--101	Product A	Category1
--102	Product B	Category2
--🧾 Expected Output
-- 
--order id	customer name	customer email	product name	product category	order date
--1001	John Doe	john.doe@email.com	Product A	Category1	2023-01-10 00:00:00
--1002	Jane Smith	jane.smith@email.com	Product B	Category2	2023-01-11 00:00:00
 

-- Write query here
-- TABLE NAME: `crm_orders`, `crm_customers` and `crm_products`

SELECT 
  co.order_id,
  CONCAT(cc.first_name, ' ', cc.last_name) AS customer_name,
  cc.email AS customer_email,
  cp.product_name,
  cp.category AS product_category,
  co.order_date
FROM crm_orders AS co 
INNER JOIN crm_customers AS cc 
ON co.customer_id = cc.customer_id
INNER JOIN crm_products AS cp 
ON cp.product_id = co.product_id