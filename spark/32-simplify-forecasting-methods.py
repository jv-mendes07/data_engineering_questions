#Forecasting can sometimes be simplified using straightforward techniques, and the naïve forecasting method is a prime example. In this exercise, you’ll create a forecast for "distance per dollar" (calculated as distance_to_travel / monetary_cost). Follow these steps:
#
#Aggregate Monthly Data: Calculate the total "distance to travel" and "monetary cost" for each month.
#Determine Actual Values: Use the monthly totals to compute the "distance per dollar" for the current month.
#Generate Forecasts: Use the value from the previous month as the forecast for the current month.
#Evaluate Accuracy: Calculate the Root Mean Squared Error (RMSE) using the formula:RMSE=mean((actual−forecast)2)
#Round the RMSE value to two decimal places and report it.
# 
#
#Table Format
#
#| **Column Name**             | **Description**                                                  |
#| --------------------------- | ---------------------------------------------------------------- |
#| `request_id`                | Unique identifier for each ride request.                         |
#| `request_date`              | Date and time of the ride request.                               |
#| `request_status`            | Status of the request (e.g., 'success' or 'fail').               |
#| `distance_to_travel`        | Distance to be traveled for the ride (in kilometers).            |
#| `monetary_cost`             | Cost of the ride (in local currency).                            |
#| `driver_to_client_distance` | Distance between the driver and client before starting the ride. |
#Sample Input Table
#
#| **request_id** | **request_date** | **request_status** | **distance_to_travel** | **monetary_cost** | **driver_to_client_distance** |
#| -------------- | ---------------- | ------------------ | ---------------------- | ----------------- | ----------------------------- |
#| 1              | 2020-01-09       | success            | 70.59                  | 6.56              | 14.36                         |
#| 2              | 2020-01-24       | success            | 93.36                  | 22.68             | 19.90                         |
#| 3              | 2020-02-08       | fail               | 51.24                  | 11.39             | 21.32                         |
#| 4              | 2020-02-23       | success            | 61.58                  | 8.04              | 44.26                         |
#| 5              | 2020-03-09       | success            | 25.04                  | 7.19              | 1.74                          |
#Sample Output Table
#
#| **month** | **total_distance** | **total_cost** | **actual_distance_per_dollar** | **forecasted_distance_per_dollar** | **error**           |
#| --------- | ------------------ | -------------- | ------------------------------ | ---------------------------------- | ------------------- |
#| 2020-01   | 163.95             | 29.24          | 5.60704514363885               |                                    |                     |
#| 2020-02   | 112.82             | 19.43          | 5.806484817292846              | 5.60704514363885                   | 0.19943967365399562 |
#| 2020-03   | 25.04              | 7.19           | 3.4826147426981917             | 5.806484817292846                  | 2.3238700745946543  |

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):

  output_df = (input_df
               .groupBy(F.date_trunc('month', F.col('request_date')).alias('month'))
               .agg(                 F.sum(F.col('distance_to_travel')).alias('total_distance'),
F.sum(F.col('monetary_cost')).alias('total_cost'),
               ))

  output_df = (output_df
               .withColumn(
      'actual_distance_per_dollar',
      F.col('total_distance') / F.col('total_cost') 
  ))

  windowSpec = W.orderBy('month')

  output_df = (output_df
               .withColumn(
      'forecasted_distance_per_dollar',
      F.lag(F.col('actual_distance_per_dollar')).over(windowSpec)
  ))

  windowSpec = W.partitionBy('month').orderBy('month')

  output_df = (output_df
               .withColumn(
              'error',
              F.avg(F.sqrt(F.pow((F.col('actual_distance_per_dollar') - F.col('forecasted_distance_per_dollar')), 2))).over(windowSpec)
              ))

  output_df = output_df.select(
    F.col('month'),
    F.col('total_distance'),
    F.col('total_cost'),
    F.col('actual_distance_per_dollar'),
    F.col('forecasted_distance_per_dollar'),
    F.col('error')
  )
  
  return output_df