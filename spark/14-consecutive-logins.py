#Write a solution to determine the fraction of players who logged in again on the day immediately following their first login date. The fraction is calculated as the number of players who logged in on consecutive days starting from their first login date, divided by the total number of players. Round the fraction to 2 decimal places.
#
#Schema and Dataset
#Table: Activity
#| Column Name    | Type   | Description                                     |
#| -------------- | ------ | ----------------------------------------------- |
#| `player_id`    | `int`  | Unique identifier for each player.              |
#| `device_id`    | `int`  | Identifier for the device used by the player.   |
#| `event_date`   | `date` | The date the player logged in.                  |
#| `games_played` | `int`  | The number of games the player played that day. |
#Input Example:
#Activity Table:
#| player_id | device_id | event_date | games_played |
#| --------- | --------- | ---------- | ------------ |
#| 1         | 2         | 2016-03-01 | 5            |
#| 1         | 2         | 2016-03-02 | 6            |
#| 2         | 3         | 2017-06-25 | 1            |
#| 3         | 1         | 2016-03-02 | 0            |
#| 3         | 4         | 2018-07-03 | 5            |
#Output Example:
#| fraction |
#| -------- |
#| 0.33     |

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min as spark_min, lead, countDistinct, round as spark_round, datediff, to_date, when
from pyspark.sql.window import Window

def etl(input_df):

  windowSpec = Window.partitionBy('player_id').orderBy(col('event_date').asc())
  
  input_df = input_df.withColumn(
    'min_event_date', spark_min(col('event_date')).over(windowSpec)
  ).withColumn(
    'next_event_date',
    lead(col('event_date')).over(windowSpec)
  ).withColumn(
    'diff_days',
    datediff(
      to_date(col('next_event_date')),
      to_date(col('min_event_date'))
    )
  )
  
  input_df = input_df.agg(
    countDistinct('player_id').alias('all_users'),
    countDistinct(when(col('diff_days') == 1, col('player_id'))).alias('users_cons')
  )

  input_df = input_df.withColumn(
    'fraction',
    spark_round(col('users_cons') / col('all_users'), 2)
  ).select(col('fraction'))
  
  return input_df