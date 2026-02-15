##You work for an E-Commerce giant that has provided you two separate DataFrames.
##
##The first, ecom_dt_orders, contains order information with the following schema:
##
##+-------------+-----------+
##| Column Name | Data Type |
##+-------------+-----------+
##|  order_id   |  string   |
##| product_id  |  string   |
##|   user_id   |  string   |
##| order_date  |  string   |
##+-------------+-----------+
##The order_date column contains the date of order placement in 'MM/DD/YYYY' format.
##
##The second, ecom_dt_products, contains product details with the following schema:
##
##+--------------+-----------+
##| Column Name  | Data Type |
##+--------------+-----------+
##|  product_id  |  string   |
##| product_name |  string   |
##|   category   |  string   |
##+--------------+-----------+
##Write a function that combines the DataFrames and creates a column named is_weekend that should indicate whether the order was placed on a weekend (Saturday or Sunday).
##
##Please be aware that you might encounter incorrect date formats, which should be handled appropriately. Also note that an order might have multiple products, so the same order might appear multiple times in the output.
##
##+--------------+-----------+
##| Column Name  | Data Type |
##+--------------+-----------+
##|   user_id    |  string   |
##| product_name |  string   |
##|   category   |  string   |
##|  order_date  |  string   |
##|  is_weekend  |  boolean  |
##+--------------+-----------+
##Example
##
##ecom_dt_orders
##+----------+------------+---------+------------+
##| order_id | product_id | user_id | order_date |
##+----------+------------+---------+------------+
##|    1     |    P001    |  U001   | 02/25/2023 |
##|    2     |    P002    |  U001   | 03/14/2023 |
##|    3     |    P001    |  U002   | 03/16/2023 |
##|    4     |    P003    |  U002   | 03/18/2023 |
##|    5     |    P004    |  U003   | 04/01/2023 |
##+----------+------------+---------+------------+
##
##ecom_dt_products
##+------------+--------------+-------------+
##| product_id | product_name |  category   |
##+------------+--------------+-------------+
##|    P001    |  Product 1   | Electronics |
##|    P002    |  Product 2   |  Clothing   |
##|    P003    |  Product 3   | Home Goods  |
##|    P004    |  Product 4   |    Books    |
##+------------+--------------+-------------+
##
##Expected
##+-------------+------------+------------+--------------+---------+
##|  category   | is_weekend | order_date | product_name | user_id |
##+-------------+------------+------------+--------------+---------+
##|    Books    |     1      | 04/01/2023 |  Product 4   |  U003   |
##|  Clothing   |     0      | 03/14/2023 |  Product 2   |  U001   |
##| Electronics |     0      | 03/16/2023 |  Product 1   |  U002   |
##| Electronics |     1      | 02/25/2023 |  Product 1   |  U001   |
##| Home Goods  |     1      | 03/18/2023 |  Product 3   |  U002   |
##+-------------+------------+------------+--------------+---------+

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(ecom_dt_orders, ecom_dt_products):
  
  ecom_orders_products = ecom_dt_orders.join(ecom_dt_products, how = 'inner', on = 'product_id')

  ecom_orders_products = ecom_orders_products.withColumn(
    'is_weekend',
    F.when(F.dayofweek(F.to_date(F.col('order_date'),
          'MM/dd/yyyy')) >=6, 1).otherwise(0)
  )
  
  ecom_orders_products = ecom_orders_products.withColumn(
    'order_date',
  F.date_format(F.to_date(F.col('order_date'),
          'MM/dd/yyyy'), 'MM/dd/yyyy')
  )

  ecom_orders_products = ecom_orders_products.select(
    F.col('category'),
    F.col('is_weekend'),
    F.col('order_date'),
    F.col('product_name'),
    F.col('user_id')
  ).orderBy(F.col('category').asc(), F.col('is_weekend').asc())

  return ecom_orders_products