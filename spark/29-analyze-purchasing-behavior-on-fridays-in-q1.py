##Let’s explore purchasing trends for all Fridays in the first quarter of the year. For each Friday, calculate the average amount spent per order and include the week number of that Friday. This exercise helps build an understanding of how to handle date and time-related computations effectively.
##
##Dataset and Schema:
##Below are the details of the table used:
##
##Input Table (user_purchases)
##| **Column Name** | **Data Type** | **Description**                                 |
##| --------------- | ------------- | ----------------------------------------------- |
##| `user_id`       | `INT`         | Unique identifier for each user.                |
##| `date`          | `DATE`        | Date of the purchase.                           |
##| `amount_spent`  | `FLOAT`       | Total amount spent by the user on that date.    |
##| `day_name`      | `VARCHAR(15)` | Name of the day on which the purchase occurred. |
##Sample Data (Input Table)
##| **user_id** | **date**   | **amount_spent** | **day_name** |
##| ----------- | ---------- | ---------------- | ------------ |
##| 1047        | 2023-01-01 | 288              | Sunday       |
##| 1099        | 2023-01-04 | 803              | Wednesday    |
##| 1052        | 2023-01-13 | 889              | Friday       |
##| 1052        | 2023-01-13 | 596              | Friday       |
##| 1095        | 2023-01-27 | 424              | Friday       |
##| 1019        | 2023-02-03 | 185              | Friday       |
##| 1019        | 2023-02-03 | 995              | Friday       |
##| 1023        | 2023-02-24 | 259              | Friday       |
##Output Table
##| **week_number** | **avg_amount_spent** |
##| --------------- | -------------------- |
##| 2               | 742.5                |
##| 4               | 424.0                |
##| 5               | 590.0                |
##| 8               | 259.0                |

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from datetime import date
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):

    output_df = input_df.filter(
      (F.quarter(F.col('date')) == 1) 
    & (F.col('day_name') == 'Friday')
    )

    output_df = output_df.withColumn(
      'week_number',
      F.weekofyear(F.col('date'))
    )

    output_df = output_df.groupBy('week_number').agg(
      F.avg(F.col('amount_spent')).alias('avg_amount_spent')
    ).orderBy('week_number')

    return output_df