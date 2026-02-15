#You have a table named users_followers with two columns: user_id and follower_id. This table represents the relationship between users and their followers on a social platform. Each follower_id is also a registered user of the platform.
#
#Your goal is to calculate the famous percentage for each user. The famous percentage is defined as:
#
#Famous Percentage=Number of followers a user hasTotal number of users on the platform×100Famous Percentage=Total number of users on the platformNumber of followers a user has​×100
#
#Provide the famous percentage for each user, rounded to two decimal places.
#
#TABLE FORMAT
#
#| Column Name   | Data Type | Description                                       |
#| ------------- | --------- | ------------------------------------------------- |
#| `user_id`     | INT       | The unique identifier for a user on the platform. |
#| `follower_id` | INT       | The unique identifier for a follower of the user. |
#Input Table: cup_user_percentage
#| user_id | follower_id |
#| ------- | ----------- |
#| 1       | 2           |
#| 1       | 3           |
#| 2       | 4           |
#| 5       | 1           |
#| 5       | 3           |
#| 11      | 7           |
#| 12      | 8           |
#| 13      | 5           |
#| 13      | 10          |
#| 14      | 12          |
#| 14      | 3           |
#| 15      | 14          |
#| 15      | 13          |
#Output Table:
#| user_id | famous_percentage |
#| ------- | ----------------- |
#| 1       | 15.38             |
#| 2       | 7.69              |
#| 5       | 15.38             |
#| 11      | 7.69              |
#| 12      | 7.69              |
#| 13      | 15.38             |
#| 14      | 15.38             |
#| 15      | 15.38             |

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):

  users_followers = (input_df
                     .groupBy('user_id')
                     .agg(                     F.countDistinct(F.col('follower_id')).alias('total_followers')
                     ))

  total_users = (input_df
                 .agg(
                   F.countDistinct(F.col('user_id'), F.col('follower_id')).alias('total_users')
                 ))

  users_followers = (users_followers
                     .join(total_users, on = F.lit(1) == F.lit(1), how = 'inner')
                     .withColumn(
                       'famous_percentage',
                       F.round(F.col('total_followers') / F.col('total_users') * 100, 2)  
                     )
                     .select(F.col('user_id'), F.col('famous_percentage'))
                     .orderBy(F.col('user_id').asc()))

  return users_followers