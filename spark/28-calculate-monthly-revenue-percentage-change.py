#You are given a table named monthly_purchases containing transaction details. Your goal is to calculate the month-over-month percentage change in revenue. The output should include the year-month (YYYY-MM) and the percentage change in revenue for each month. The percentage change is calculated as:
#
#Percentage Change=This Month’s Revenue−Last Month’s RevenueLast Month’s Revenue×100Percentage Change=Last Month’s RevenueThis Month’s Revenue−Last Month’s Revenue​×100
#
#Round the percentage change to two decimal places.
#Ensure the data is sorted chronologically from the earliest to the latest month.
#The percentage change for the first month should be NULL since there is no prior month for comparison.
#Input Table: monthly_purchases
#| Column Name   | Data Type | Description                                 |
#| ------------- | --------- | ------------------------------------------- |
#| `id`          | INT       | Unique identifier for each transaction.     |
#| `created_at`  | DATETIME  | The date and time the transaction was made. |
#| `value`       | INT       | Revenue generated from the transaction.     |
#| `purchase_id` | INT       | Identifier for the specific purchase.       |
#Example Input Data
#| id | created_at          | value  | purchase_id |
#| -- | ------------------- | ------ | ----------- |
#| 1  | 2019-01-01 00:00:00 | 172692 | 43          |
#| 2  | 2019-01-05 00:00:00 | 177194 | 36          |
#| 3  | 2019-02-02 00:00:00 | 140032 | 25          |
#| 4  | 2019-03-02 00:00:00 | 157548 | 19          |
#Output Table: revenue_change
#| year_month | percentage_change |
#| ---------- | ----------------- |
#| 2019-01    | N/A              |
#| 2019-02    | -59.98           |
#| 2019-03    | 12.51             |

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

# Initialize Spark session
spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):
  
  input_df = input_df.withColumn(
    'year_month',
    F.concat_ws("/", F.year(F.col('created_at')), F.lpad(F.month(F.col('created_at')), 2, "0"))
  )

  input_df = input_df.groupBy('year_month').agg(
    F.sum(F.col('value')).alias('actual_revenue')
  )

  windowSpec = W.orderBy(F.col('year_month').asc())

  output_df = input_df.withColumn(
    'previous_value',
    F.lag(F.col('actual_revenue')).over(windowSpec)
  )

  output_df = output_df.withColumn(
    'percentage_change',
    F.round((F.col('actual_revenue') - F.col('previous_value')) / F.col('previous_value') * 100, 2)
  ).select(
    F.col('year_month'),
    F.col('percentage_change')
  )
    
  return output_df