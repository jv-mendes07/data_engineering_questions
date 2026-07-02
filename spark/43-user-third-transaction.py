## User Third Transaction
## Difficulty: MEDIUM | Companies | Hints

## ---- Problem ----
## You are provided with a table containing records of Uber transactions made by users.
## Write a SQL query to fetch the details of the third transaction for each user. Your output
## should include the following columns: user_id, spend, and transaction_date.
##
## Schema columns:
## ttq_transaction_third: user_id, spend, transaction_date
##
## Output columns: user_id, spend, transaction_date
##
## Schema: 1 table
## ttq_transaction_third - 3 cols

## ---- Examples ----
## Example 1

## Input:
## ttq_transaction_third:
## user_id | spend  | transaction_date
## 111     | 100.5  | 2022-01-08T00:00:00.000Z
## 111     | 55.0   | 2022-01-10T00:00:00.000Z
## 121     | 36.0   | 2022-01-18T00:00:00.000Z
## 145     | 24.99  | 2022-01-26T00:00:00.000Z

## Output:
## user_id | spend | transaction_date
## 111     | 89.6  | 2022-02-05T00:00:00.000Z

## ---- Constraints ----
## - Handle NULL values appropriately
## - Return results matching the expected output schema and order


from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql import SparkSession
import datetime
import json

spark = SparkSession.builder.appName("run-pyspark-code").getOrCreate()

def etl(ttq_transaction_third):

    window = W.partitionBy(F.col('user_id')).orderBy(F.col('transaction_date'))

    ttq_transaction_third = ttq_transaction_third.withColumn(
        'row',
        F.row_number().over(window)
    )

    ttq_transaction_third = ttq_transaction_third.filter(
        F.col('row') == 3
    )

    ttq_transaction_third = ttq_transaction_third.select(
        F.col('user_id'),
        F.col('spend'),
        F.to_date(F.col('transaction_date')).alias('transaction_date')
    )

    return ttq_transaction_third