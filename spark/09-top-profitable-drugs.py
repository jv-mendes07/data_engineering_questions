#A healthcare company wants to analyze its pharmacy sales to identify the most profitable drugs. Each drug is exclusively produced by one manufacturer.
#
#Write a query to retrieve the top 3 most profitable drugs along with their respective profits. The results should be sorted in descending order of profit. Assume there are no ties in profit values.
#
#Profit Calculation:
#
#Profit = Total Sales - Cost of Goods Sold (COGS)
#Table Schema: pharmacy_sales
#Input Example:
#| **Column Name** | **Type** |
#| --------------- | -------- |
#| product_id      | integer  |
#| units_sold      | integer  |
#| total_sales     | decimal  |
#| cogs            | decimal  |
#| manufacturer    | varchar  |
#| drug            | varchar  |
#Sample data from the pharmacy_sales table:
#
#Output Example:
#| **product_id** | **units_sold** | **total_sales** | **cogs**   | **manufacturer** | **drug**        |
#| -------------- | -------------- | --------------- | ---------- | ---------------- | --------------- |
#| 9              | 37410          | 293452.54       | 208876.01  | Eli Lilly        | Zyprexa         |
#| 34             | 94698          | 600997.19       | 521182.16  | AstraZeneca      | Surmontil       |
#| 61             | 77023          | 500101.61       | 419174.97  | Biogen           | Varicose Relief |
#| 136            | 144814         | 1084258         | 1006447.73 | Biogen           | Burkhart        |
#The query should output the top 3 most profitable drugs:
#
#| `drug`          | `total_profit` |
#| --------------- | -------------- |
#| Zyprexa         | 84576.53       |
#| Varicose Relief | 80926.64       |
#| Surmontil       | 79815.03       |
#Explanation:
#Profit Calculation:
#
#For Zyprexa, Profit = 293452.54 - 208876.01 = 84576.53.
#For Varicose Relief, Profit = 500101.61 - 419174.97 = 80926.64.
#For Surmontil, Profit = 600997.19 - 521182.16 = 79815.03.
#Ranking:
#
#Drugs are ranked by their profits in descending order.
#The top 3 profitable drugs are displayed in the output.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(video_stream_df):
  
  pharmacy_sales_df = video_stream_df

  pharmacy_sales_df = pharmacy_sales_df.withColumn('total_profit', F.round(F.col('total_sales') - F.col('cogs'), 2))

  pharmacy_sales_df = pharmacy_sales_df.select(F.col('drug'), F.col('total_profit')).orderBy(F.col('total_profit').desc()).limit(3)

  return pharmacy_sales_df