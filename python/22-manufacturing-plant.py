#Your task is to write a function or SQL query that returns the top 3 selling products in each product category, ranked by revenue (highest first).
#
#The ranking must:
#
#Be based on total revenue per product.
#
#Use dense ranking — i.e., no gaps in the ranking sequence if there's a tie.
#
#Output only the top 3 products per category.
#
#You are given two datasets related to a manufacturing company.
#
#In Python/PySpark/Scala, the input DataFrames are:
#
#products
#
#sales
#
#In SQL/DBT, the corresponding table names are:
#
#manufacture_product
#
#manufacture_sales
#
#📥 Input Schemas
#products / manufacture_product
#Column Name	Data Type
#product_id	integer
#category	string
#product_name	string
#sales / manufacture_sales
#Column Name	Data Type
#sale_id	integer
#product_id	integer
#quantity	integer
#revenue	double
#📤 Output Schema
#Column Name	Data Type
#category	string
#product_name	string
#revenue	integer
#rank	integer
#🧪 Example
#products
#product_id	category	product_name
#1	A	Product1
#2	A	Product2
#3	A	Product3
#4	B	Product4
#5	B	Product5
#6	B	Product6
#7	C	Product7
#8	C	Product8
#9	C	Product9
#sales
#sale_id	product_id	quantity	revenue
#1	1	10	100.0
#2	2	8	120.0
#3	3	12	180.0
#4	4	5	50.0
#5	5	3	30.0
#6	6	7	70.0
#7	7	15	150.0
#8	8	10	100.0
#9	9	8	80.0
#✅ Expected Output
#category	product_name	revenue	rank
#A	Product3	180	1
#A	Product2	120	2
#A	Product1	100	3
#B	Product6	70	1
#B	Product4	50	2
#B	Product5	30	3
#C	Product7	150	1
#C	Product8	100	2
#C	Product9	80	3


import pandas as pd
import numpy as np
import datetime
import json
import math
import re

def etl(products, sales):

  products_sales = pd.merge(sales, products, how = 'inner', on = ['product_id'])

  products_sales = products_sales.groupby(['product_name', 'category']).agg(
    revenue = ('revenue', 'sum')
  ).reset_index()

  products_sales['rank'] = products_sales.groupby(['category'])['revenue'].rank(
    method='dense',
    ascending = False
  )

  products_sales = products_sales[products_sales['rank'] <= 3]

  products_sales = products_sales.sort_values(['category', 'rank'])

  products_sales = products_sales[['product_name', 'category', 'rank', 'revenue']]

  return products_sales