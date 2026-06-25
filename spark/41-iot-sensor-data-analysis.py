## SDA Sensor Data Analysis
## Difficulty: MEDIUM | Companies | Hints

## ---- Problem ----
## The SDA_sensor_data table holds IoT sensor readings, each with a measurement_value and
## a measurement_time string formatted MM/DD/YYYY HH:MM:SS. For each day, sum the values
## at odd-numbered versus even-numbered positions when that day's readings are ordered
## by time. Return one row per day with measurement_day, odd_sum, and even_sum.
##
## Schema columns:
## SDA_sensor_data: measurement_id, measurement_value, measurement_time
##
## Output columns: measurement_day, odd_sum, even_sum
##
## Sort the results by measurement_day.

## ---- Examples ----
## Example 1

## Input:
## SDA_sensor_data:
## measurement_id | measurement_value | measurement_time
## 131233         | 1109.51           | 07/10/2022 09:00:00
## 135211         | 1662.74           | 07/10/2022 11:00:00
## 523542         | 1246.24           | 07/10/2022 13:15:00
## 143562         | 1124.5            | 07/11/2022 15:00:00
## 346462         | 1234.14           | 07/11/2022 16:45:00

## Output:
## measurement_day     | odd_sum  | even_sum
## 07/10/2022 00:00:00 | 2355.75  | 1662.74
## 07/11/2022 00:00:00 | 1124.5   | 1234.14

## Explanation: On 07/10/2022 the readings ordered by time are positions 1, 2, 3; odd
## positions (1, 3) sum to 2355.75 and the even position (2) is 1662.74.

## ---- Constraints ----
## - measurement_time is a string formatted MM/DD/YYYY HH:MM:SS; a reading belongs to the
##   day given by its date part.
## - Positions are determined per day by ordering that day's readings by measurement_time
##   ascending, numbering from 1; the first reading is position 1 (odd).
## - odd_sum adds the values at odd positions (1st, 3rd, ...); even_sum adds those at even
##   positions (2nd, 4th, ...).
## - Output measurement_day as the string MM/DD/YYYY 00:00:00.
## - Return results matching the expected output schema and order.


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(sda_sensor_data):

    sda_sensor_data = sda_sensor_data.withColumn(
        'measurement_day',
        F.date_format(F.date_trunc('day', F.to_timestamp(F.col('measurement_time'), 'MM/dd/yyyy HH:mm:ss')), 'MM/dd/yyyy HH:mm:ss')
    )

    window = W.partitionBy(F.col('measurement_day')).orderBy(F.col('measurement_time'))

    sda_sensor_data = sda_sensor_data.withColumn(
        'row_number',
        F.row_number().over(window)
    )

    sda_sensor_data = sda_sensor_data.groupBy('measurement_day').agg(
        F.sum(F.when(F.col('row_number') % 2 != 0, F.col('measurement_value')).otherwise(0)).alias('odd_sum'),
        F.sum(F.when(F.col('row_number') % 2 == 0, F.col('measurement_value')).otherwise(0)).alias('even_sum')
    )

    return sda_sensor_data