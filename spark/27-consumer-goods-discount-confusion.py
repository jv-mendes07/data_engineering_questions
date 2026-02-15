#As an analyst working in a Consumer Goods company, you've been provided a DataFrame with the sales data for various stores. This DataFrame includes multiple fields, namely: StoreID, ProductName, Category, SoldUnits, and Description.
#
# 
#
#Write a function that performs the following tasks:
#
#The Description column contains text information about the product. Some products have a special discount tagged inside square brackets within the Description column, e.g., [10% off]. You need to extract this discount information from the Description column and create a new column called Discount. The discount should be expressed as a decimal (e.g., 0.10 for a 10% discount), and if no discount is present, the value should be 0.
#
#Keep all other columns as they are.
#
#The schema for the input DataFrame is:
#
#+-------------+-----------+
#| Column Name | Data Type |
#+-------------+-----------+
#|   StoreID   |  String   |
#| ProductName |  String   |
#|  Category   |  String   |
#|  SoldUnits  |  Integer  |
#| Description |  String   |
#+-------------+-----------+
#The schema for the output DataFrame is:
#
#+-------------+-----------+
#| Column Name | Data Type |
#+-------------+-----------+
#|   StoreID   |  String   |
#| ProductName |  String   |
#|  Category   |  String   |
#|  SoldUnits  |  Integer  |
#| Description |  String   |
#|  Discount   |   Float   |
#+-------------+-----------+
#Example
#
#df
#+---------+-------------+----------+-----------+--------------------------+
#| StoreID | ProductName | Category | SoldUnits |       Description        |
#+---------+-------------+----------+-----------+--------------------------+
#|  S101   |  Biscuits   |   Food   |    120    | Tasty Biscuits [10% off] |
#|  S102   |   Shampoo   | Hygiene  |    85     | Smoothens Hair [5% off]  |
#|  S103   |   Banana    |   Food   |    150    |      Fresh Bananas       |
#|  S101   | Toothpaste  | Hygiene  |    300    |      Protects Teeth      |
#|  S102   |    Shirt    | Clothes  |    65     | Cotton Shirts [20% off]  |
#+---------+-------------+----------+-----------+--------------------------+
#
#Expected
#+----------+--------------------------+----------+-------------+-----------+---------+
#| Category |       Description        | Discount | ProductName | SoldUnits | StoreID |
#+----------+--------------------------+----------+-------------+-----------+---------+
#| Clothes  | Cotton Shirts [20% off]  |   0.2    |    Shirt    |    65     |  S102   |
#|   Food   |      Fresh Bananas       |   0.0    |   Banana    |    150    |  S103   |
#|   Food   | Tasty Biscuits [10% off] |   0.1    |  Biscuits   |    120    |  S101   |
#| Hygiene  |      Protects Teeth      |   0.0    | Toothpaste  |    300    |  S101   |
#| Hygiene  | Smoothens Hair [5% off]  |   0.05   |   Shampoo   |    85     |  S102   |
#+----------+--------------------------+----------+-------------+-----------+---------+

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df):

  df = df.withColumn('Discount', 
                    F.when((F.regexp_extract(F.col('Description'), '([0-9]+)', 1) == '') | (F.regexp_extract(F.col('Description'), '([0-9]+)', 1).isNull()), 0).otherwise(F.regexp_extract(F.col('Description'), '([0-9]+)', 1)) / 100 
                    )

  return df