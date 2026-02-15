#You are working in the Food and Beverage industry, where product performance and stock availability must be tracked across sales and inventory systems.
#
#You are provided with three datasets containing information about products, their sales, and stock levels.
#
#For Python / PySpark / Scala, these are the DataFrames: products, sales, and inventory
#
#For SQL / dbt, these are the table names: fnb_products, fnb_sales, and fnb_inventory
#
#Your goal is to generate a summary report for each product that includes:
#
#Total sales quantity
#
#Total revenue
#
#Total stock across all warehouses
#
#Product name and category
#
#Any missing values should be treated as 0.
#
#📥 Input Schemas
#fnb_products / products
#Column	Type	Description
#product_id	Integer	Unique identifier for product
#name	String	Name of the product
#category	String	Product category
#fnb_sales / sales
#Column	Type	Description
#sale_id	Integer	Unique ID of the sale
#product_id	Integer	ID of the product sold
#quantity	Integer	Quantity sold
#revenue	Float	Revenue generated from the sale
#fnb_inventory / inventory
#Column	Type	Description
#product_id	Integer	ID of the product in inventory
#stock	Integer	Number of units in stock
#warehouse	String	Name of the warehouse
#🎯 Output Requirements
#Your final result should include:
#
#Column	Type	Description
#product_id	Integer	ID of the product
#name	String	Name of the product
#category	String	Product category
#total_quantity	Integer	Total quantity sold (across all sales)
#total_revenue	Integer	Total revenue generated (across all sales)
#total_stock	Integer	Total stock available across all warehouses
#Products with no sales or no stock should still appear with 0 in the respective columns.
#
#The output should be sorted or grouped by product_id for consistency.

#📊 Example Data
#You are given the following tables (for SQL/dbt) or DataFrames (for PySpark, Scala, Python):
#
#fnb_products (products)
#
#fnb_sales (sales)
#
#fnb_inventory (inventory)
#
#🗃️ fnb_products / products
#product_id	name	category
#1	Apple Juice	Beverages
#2	Orange Juice	Beverages
#3	Chocolate Bar	Snacks
#4	Potato Chips	Snacks
#5	Fresh Strawberries	Fruits
#🧾 fnb_sales / sales
#sale_id	product_id	quantity	revenue
#1	1	10	20.0
#2	1	5	10.0
#3	2	8	16.0
#4	3	2	4.0
#5	4	15	30.0
#📦 fnb_inventory / inventory
#product_id	stock	warehouse
#1	50	Warehouse1
#2	40	Warehouse1
#3	30	Warehouse1
#4	20	Warehouse1
#5	10	Warehouse1
#✅ Expected Output
#📌 Note: Products with no sales (e.g., Fresh Strawberries) still appear with 0 for sales and revenue.
#Products with no inventory should appear with 0 for stock if such cases exist.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(inventory, products, sales):
	# Write code here
  product_sales_summary = sales.join(products, on = 'product_id',
                                    how = 'left')

  product_stock_summary = inventory.join(products, on = 'product_id',
                                    how = 'left')

  product_stock_summary = product_stock_summary.groupBy('product_id', 'name', 'category').agg(F.sum(F.coalesce(F.col('stock'), F.lit(0))).alias('total_stock'))

  product_sales_summary = product_sales_summary.groupBy('product_id', 'name', 'category').agg(
    F.sum(F.coalesce(F.col('quantity'),
F.lit(0))).alias('total_quantity'),   F.sum(F.coalesce(F.col('revenue'),F.lit(0))).alias('total_revenue') )

  product_summary_report = product_sales_summary.join(product_stock_summary, on = ['product_id', 'name', 'category'], how = 'outer').select(
    F.col('product_id'),
    F.col('name'),
    F.col('category'),
    F.coalesce(F.col('total_quantity'), F.lit(0)).alias('total_quantity'),
    F.coalesce(F.col('total_revenue'), F.lit(0)).alias('total_revenue'),
    F.coalesce(F.col('total_stock'), F.lit(0)).alias('total_stock')).orderBy(F.col('category').asc())
  
  return product_summary_report