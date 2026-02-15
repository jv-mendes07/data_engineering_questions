##Determine the number of reviews for each unique review score earned by Hotel Arena. Your result should include the hotel name (always "Hotel Arena"), the review score, and the count of reviews for that score.
##
## 
##
##Dataset and Schema:
##Input Table (hotel_reviews)
##| **Column Name**  | **Data Type**  | **Description**                     |
##| ---------------- | -------------- | ----------------------------------- |
##| `hotel_name`     | `VARCHAR(100)` | Name of the hotel.                  |
##| `reviewer_score` | `FLOAT`        | Review score given by the reviewer. |
##Sample Input Table
##| **hotel_name** | **reviewer_score** |
##| -------------- | ------------------ |
##| Hotel Arena    | 8.0                |
##| Hotel Arena    | 9.0                |
##| Hotel Arena    | 6.0                |
##Output Table
##| **hotel_name** | **reviewer_score** | **review_count** |
##| -------------- | ------------------ | ---------------- |
##| Hotel Arena    | 8.0                | 1                |
##| Hotel Arena    | 9.0                | 1                |
##| Hotel Arena    | 6.0                | 1                |

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):
	# Write code here
  input_df = input_df.filter(F.col('hotel_name') == 'Hotel Arena').groupBy('hotel_name', 'reviewer_score').agg(F.count('*').alias('review_count')).orderBy(F.col('reviewer_score').desc())

  return input_df