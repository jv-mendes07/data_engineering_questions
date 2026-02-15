##You're working with a table clq_city_length that contains information about different station locations. Your task is to:
##
##Retrieve the names of the shortest and longest city names, along with their respective character counts.
##If multiple cities share the shortest or longest length, select the one that appears first alphabetically.
##
##🎯 Expected Output
##Return the following columns in a single row:
##
##Shortest city name
##
##Character count of the shortest city
##
##Longest city name
##
##Character count of the longest city
##
##🧾 Table: clq_city_length (alias: STATION)
##Column Name	Data Type	Description
##NUMBER	VARCHAR2(21)	Identifier for the station
##ID	VARCHAR2(2)	Station ID
##CITY	VARCHAR2(100)	Name of the city
##STATE	VARCHAR2(100)	State identifier
##📘 Example Input
##Assume the CITY column contains the following values:
##
##CITY
##DEF
##ABC
##PQRS
##WXY
##✅ Example Output
##shortest_city	shortest_len	longest_city	longest_len
##ABC	3	PQRS	4
##💡 Explanation
##Sorted alphabetically: ABC, DEF, PQRS, WXY
##
##Shortest: ABC (length 3)
##
##Longest: PQRS (length 4)
##
##Even though DEF and WXY are also 3-letter cities, ABC comes first alphabetically.
##
##📌 Notes
##You may write two separate queries (e.g., one for shortest and one for longest), and combine them if needed.
##
##A single query is not mandatory.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):
  
  input_df = input_df.withColumn('length', F.length(F.col('city')))

  window = Window.orderBy(F.col('length').desc(), F.col('city').desc())

  input_df = input_df.withColumn('rank',
F.rank().over(window))

  top_rank = input_df.select(F.max(F.col('rank'))).collect()[0][0]

  low_rank = input_df.select(F.min(F.col('rank'))).collect()[0][0]

  input_df = input_df.filter((F.col('rank') == top_rank) | (F.col('rank') == low_rank)).select(F.col('city').alias('CITY'),
F.col('length').alias('LENGTH')).orderBy(F.col('city').asc())

  return input_df