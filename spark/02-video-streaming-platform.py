##You're working as a Data Engineer at a video streaming platform. Your team wants to highlight popular and recently released videos on the homepage to boost user engagement.
##
##You’ve been provided with a DataFrame called video_stream_df, which contains metadata about all the videos on the platform.
##
##🎯 Task
##Write a function that:
##
##Accepts video_stream_df as input.
##
##Returns a filtered DataFrame that includes only the videos which:
##
##Have more than 1,000,000 views
##
##Were released in the last 6 years (relative to the current year)
##
##Reorders the columns in the following format:
##
##duration, genre, release_year, title, video_id, view_count
##Sort the data using column duration
##🗃️ Dataset Schema (video_stream_df)
##Column Name	Data Type
##video_id	Integer
##title	String
##genre	String
##release_year	Integer
##duration	Integer (in minutes)
##view_count	Integer
##🔍 Sample Input
##video_id	title	genre	release_year	duration	view_count
##1	Amazing Adventure	Action	2020	120	2,500,000
##2	Sci-fi World	Sci-fi	2018	140	800,000
##3	Mysterious Island	Drama	2022	115	1,500,000
##4	Uncharted Realms	Action	2019	134	3,200,000
##5	Journey to the Stars	Sci-fi	2021	128	1,100,000
##✅ Expected Output
##duration	genre	release_year	title	video_id	view_count
##115	Drama	2022	Mysterious Island	3	1,500,000
##120	Action	2020	Amazing Adventure	1	2,500,000
##128	Sci-fi	2021	Journey to the Stars	5	1,100,000
##134	Action	2019	Uncharted Realms	4	3,200,000

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(video_stream_df):
  
    video_stream_df = video_stream_df.filter(F.col('view_count') > 1000000)

    video_stream_df = video_stream_df.withColumn('years_since_release', datetime.datetime.now().year - F.col('release_year')).filter(F.col('years_since_release') <= 6)

    video_stream_df = video_stream_df.select(
      F.col('duration'),
      F.col('genre'),
      F.col('release_year'),
      F.col('title'),
      F.col('video_id'),
      F.col('view_count')
    ).orderBy(F.col('duration').asc())
  
    return video_stream_df