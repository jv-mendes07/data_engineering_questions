#Your goal is to analyze the GPS tracking data from fitness sessions. For each session:
#
#Compute the distance between the farthest steps (highest and lowest step IDs) using two methods:
#Curved Earth Model (Haversine formula): Assumes Earth's curvature.
#Flat Surface Model: Assumes a flat terrain.
#Sessions with only one step are excluded from the calculations.
#Output the average distance for both methods and the difference between the two averages.
#User Session Data
#
#| user_id | session_id | step_id | day | latitude | longitude  | altitude |
#| ------- | ---------- | ------- | --- | -------- | ---------- | -------- |
#| user_1  | 101        | 1       | 1   | 37.7749  | \-122.4194 | 15.0     |
#| user_1  | 101        | 2       | 1   | 37.7750  | \-122.4195 | 15.5     |
#| user_1  | 101        | 3       | 1   | 37.7751  | \-122.4196 | 16.0     |
#| user_1  | 102        | 1       | 1   | 34.0522  | \-118.2437 | 20.0     |
#| user_1  | 102        | 2       | 1   | 34.0523  | \-118.2438 | 20.5     |
#| user_2  | 201        | 1       | 1   | 40.7128  | \-74.0060  | 5.0      |
#| user_2  | 201        | 2       | 1   | 40.7129  | \-74.0061  | 5.5      |
#| user_2  | 202        | 1       | 1   | 51.5074  | \-0.1278   | 10.0     |
#| user_2  | 202        | 2       | 1   | 51.5075  | \-0.1279   | 10.5     |
#| user_3  | 301        | 1       | 1   | 48.8566  | 2.3522     | 25.0     |
#| user_3  | 301        | 2       | 1   | 48.8567  | 2.3523     | 25.5     |
#Output
#
#| session_id | avg_distance_curved | avg_distance_flat | difference |
#| ---------- | ------------------- | ----------------- | ---------- |
#| 101        | 0.0283              | 0.0314            | 0.003      |
#| 102        | 0.0144              | 0.0157            | 0.0013     |
#| 201        | 0.014               | 0.0157            | 0.0017     |
#| 202        | 0.0131              | 0.0157            | 0.0026     |
#| 301        | 0.0133              | 0.0157            | 0.0024     |
# 
#d=6371⋅arccos(sin(φ1​)⋅sin(φ2​)+cos(φ1​)⋅cos(φ2​)⋅cos(λ2​−λ1​))
#
#d=111⋅(lat2​−lat1​)2+(lon2​−lon1​)2

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, count, first, last, radians, sin, cos, sqrt, pow, atan2, lit, abs, round as round_func, acos, bround
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import when

def etl(input_df):

  input_df = (input_df.groupBy('session_id')
                      .agg(
                        F.countDistinct('step_id').alias('steps'),
                        F.min('step_id').alias('min_step'),
                        F.min('latitude').alias('min_lat'),
                        F.min('longitude').alias('min_long'),
                        F.max('step_id').alias('max_step'),
                        F.max('latitude').alias('max_lat'),
                        F.max('longitude').alias('max_long')
                      )
                      .filter(F.col('steps') > 1))

  input_df = input_df.withColumn(
    'distance_curved',
    F.lit(6371) * acos(sin(radians(F.col('min_lat'))) * sin(radians(F.col('max_lat'))) + cos(radians(F.col('min_lat'))) * cos(radians(F.col('max_lat'))) * cos(radians(F.col('max_long')) - radians(F.col('min_long'))))
  )

  input_df = input_df.withColumn(
    'distance_flat',
    F.lit(111) * sqrt(pow(F.col('max_lat') - F.col('min_lat'), 2) + pow(F.col('max_long') - F.col('min_long'), 2))
  )

  output_df = (input_df
               .groupBy('session_id')
               .agg(
                 round_func(F.avg(F.col('distance_curved')), 4).alias('avg_distance_curved'),
                 round_func(F.avg(F.col('distance_flat')), 4).alias('avg_distance_flat'),
                 round_func((F.avg(F.col('distance_flat')) - F.avg(F.col('distance_curved'))), 4).alias('difference')
               ).orderBy(F.col('session_id').asc()))

  return output_df