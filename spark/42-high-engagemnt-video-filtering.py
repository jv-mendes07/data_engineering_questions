## High-Engagement Video Filtering
## Difficulty: EASY | Companies | Hints

## ---- Problem ----
## You are working at a video streaming platform. Your team wants to highlight popular,
## recent videos on the homepage to boost engagement. Given a table of video metadata,
## return the videos that have more than 1,000,000 views and were released in 2019 or later
## (release_year >= 2019).
##
## Schema columns:
## video_stream: video_id, title, genre, release_year, duration, view_count
##
## Output columns: duration, genre, release_year, title, video_id, view_count
##
## Sort the results by duration in ascending order.
##
## Schema: 1 table
## video_stream - 6 cols

## ---- Examples ----
## Example 1

## Input:
## video_stream:
## video_id | title              | genre  | release_year | duration | view_count
## 1        | Amazing Adventure  | Action | 2020         | 120      | 2500000
## 2        | Sci-fi World       | Sci-fi | 2018         | 140      | 800000
## 3        | Mysterious Island  | Drama  | 2022         | 115      | 1500000
## 4        | Uncharted Realms   | Action | 2019         | 134      | 3200000
## 5        | Journey to the Stars | Sci-fi | 2021       | 128      | 1100000

## Output:
## duration | genre  | release_year | title              | video_id | view_count
## 115      | Drama  | 2022         | Mysterious Island  | 3        | 1500000
## 120      | Action | 2020         | Amazing Adventure  | 1        | 2500000
## 128      | Sci-fi | 2021         | Journey to the Stars | 5      | 1100000
## 134      | Action | 2019         | Uncharted Realms   | 4        | 3200000

## Explanation:
## Mysterious Island (2022, 1,500,000 views), Amazing Adventure (2020, 2,500,000 views),
## Journey to the Stars (2021, 1,100,000 views), and Uncharted Realms (2019, 3,200,000
## views) each have more than 1,000,000 views and were released in 2019 or later -> included.
## Sci-fi World (2018, 800,000 views) is excluded - it was released before 2019 and has
## fewer than 1,000,000 views.
## Rows are sorted by duration in ascending order.

## ---- Constraints ----
## - Keep only videos whose view_count is greater than 1,000,000.
## - Keep only videos whose release_year is 2019 or later.
## - Sort the results by duration in ascending order.
## - Return results matching the expected output schema and order.


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(video_stream_df):

    video_stream_df = video_stream_df.filter(
        (F.col('view_count') > 1000000) &
    (F.col('release_year') >= 2019)
    ).orderBy(F.col('duration').asc())
    
    return video_stream_df