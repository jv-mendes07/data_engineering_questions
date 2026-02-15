##You are given a table containing product sales data. Your task is to identify the top 5 products that generated the highest total revenue during the first half of the year 2022 (i.e., from January 1st to June 30th, 2022, inclusive).
##
##🧾 Table Structure
##trp_top_revenueTable
##
##Column Name	Data Type	Description
##product_id	INTEGER	Unique identifier for the product
##promotion_id	INTEGER	Identifier for applied promotion
##cost_in_dollars	INTEGER	Price of one unit of the product
##customer_id	INTEGER	ID of the customer who made the order
##date	DATE	Date of the transaction
##units_sold	INTEGER	Number of units sold
##🎯 Goal
##Compute the total revenue per product, where
##revenue = cost_in_dollars × units_sold.
##
##Filter the records to only include transactions between 2022-01-01 and 2022-06-30.
##
##Return the top 5 products based on total revenue.
##
##Your result should be sorted in descending order of revenue.
##
##📌 Output Format
##Column Name	Data Type
##product_id	INTEGER
##total_revenue	INTEGER
##📊 Example Input
##product_id	promotion_id	cost_in_dollars	customer_id	date	units_sold
##1	1	2	1	2022-04-01	4
##3	3	6	3	2022-05-24	6
##1	2	2	10	2022-05-01	3
##1	2	3	2	2022-05-01	9
##2	2	10	2	2022-05-01	1
##✅ Expected Output
##product_id	total_revenue
##1	39
##3	36
##2	10

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

def etl(trp_top_revenue):

  trp_top_revenue = trp_top_revenue.filter((F.col('date') >='2022-01-01') & (F.col('date') <= '2022-06-30'))

  trp_top_revenue = trp_top_revenue.withColumn('total_revenue', F.col('units_sold') * F.col('cost_in_dollars'))

  trp_top_revenue = trp_top_revenue.groupBy('product_id').agg(
    F.sum(F.col('total_revenue')).alias('total_revenue')
  )

  trp_top_revenue = trp_top_revenue.orderBy(F.col('total_revenue').desc()).limit(5)

  return trp_top_revenue