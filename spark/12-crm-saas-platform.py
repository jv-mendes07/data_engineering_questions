#You’re working as a Data Engineer at a company that builds Customer Relationship Management (CRM) software. Your goal is to build a unified view that shows order details along with customer and product information — useful for internal dashboards and reporting.
#
#You are given three datasets (or tables) that store customer, order, and product details.
#
#🎯 Task
#Write a function (or query) that:
#
#Joins the customer, order, and product data.
#
#Creates a new column customer_name by concatenating first_name and last_name with a space in between.
#
#Returns a new DataFrame (or result set) with the following reordered column layout:
#category, customer_name, email, order_date, order_id, product_name
#
#🧩 Input Data
#✅ For Python, PySpark, Scala: the DataFrames are named
#customers, orders, products
#
#✅ For SQL/dbt: the table names are
#crm_customers, crm_orders, crm_products
#
#🗃️ Schema Details
#📋 customers
#Column Name	Data Type
#customer_id	Integer
#first_name	String
#last_name	String
#email	String
#🧾 orders
#Column Name	Data Type
#order_id	Integer
#customer_id	Integer
#product_id	Integer
#order_date	Date
#📦 products
#Column Name	Data Type
#product_id	Integer
#product_name	String
#category	String
#✅ Expected Output Schema
#The output should contain:
#
#Column Name	Data Type
#order id	Integer
#customer name	String
#customer email	String
#product name	String
#product category	String
#order date	String
#⚠️ Note: The column order matters — structure your output exactly as shown above.
#
#🔍 Example Input
#customers
#customer_id	first_name	last_name	email
#1	John	Doe	john.doe@email.com
#2	Jane	Smith	jane.smith@email.com
#orders
#order_id	customer_id	product_id	order_date
#1001	1	101	2023-01-10
#1002	2	102	2023-01-11
#products
#product_id	product_name	category
#101	Product A	Category1
#102	Product B	Category2
#🧾 Expected Output
# 
#order id	customer name	customer email	product name	product category	order date
#1001	John Doe	john.doe@email.com	Product A	Category1	2023-01-10 00:00:00
#1002	Jane Smith	jane.smith@email.com	Product B	Category2	2023-01-11 00:00:00

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(customers, orders, products):
  orders_summary = (orders.join(customers, on = 'customer_id', how = 'inner').join(products, on = 'product_id', how = 'inner'))

  orders_summary = orders_summary.select(
    F.col('order_id'),
    F.concat_ws(" ", F.col('first_name'), F.col('last_name')).alias('customer_name'),
    F.col('email').alias('customer_email'),
    F.col('product_name'),
    F.col('category').alias('product_category'),
    F.col('order_date')
  )
  
  return orders_summary