##You are given a table named OCCUPATIONS with two columns: Name and Occupation. Write a query to pivot the Occupation column such that names are displayed under their respective occupation categories in alphabetical order.
##
##The output must have four columns, labeled:
##
##Doctor
##Professor
##Singer
##Actor
##If there are fewer names for a particular occupation compared to others, fill the remaining cells in that column with NULL.
##
##Input Table
##The OCCUPATIONS table schema is:
##
##| Column     | Type   |
##| ---------- | ------ |
##| Name       | String |
##| Occupation | String |
##The Occupation column will only contain one of these values: Doctor, Professor, Singer, or Actor.
##
##Sample Input
##| Name      | Occupation |
##| --------- | ---------- |
##| Samantha  | Doctor     |
##| Julia     | Actor      |
##| Maria     | Actor      |
##| Meera     | Singer     |
#| Ashely    | Professor  |
#| Ketty     | Professor  |
#| Christeen | Professor  |
#| Jane      | Actor      |
#| Jenny     | Doctor     |
#| Priya     | Singer     |
#Sample Output
#| Doctor   | Professor | Singer | Actor |
#| -------- | --------- | ------ | ----- |
#| Jenny    | Ashely    | Meera  | Jane  |
#| Samantha | Christeen | Priya  | Julia |
#| NULL     | Ketty     | NULL   | Maria |
#Explanation
#The Doctor column contains names sorted alphabetically: Jenny, Samantha.
#The Professor column contains names sorted alphabetically: Ashely, Christeen, Ketty.
#The Singer column contains names sorted alphabetically: Meera, Priya.
#The Actor column contains names sorted alphabetically: Jane, Julia, Maria.
#For columns with fewer entries, the missing cells are filled with NULL.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(occupations_df):

    windowSpec = Window.partitionBy(F.col('Occupation')).orderBy( F.col('Name').asc())

    occupations_df = occupations_df.withColumn('row_n',
    F.row_number().over(windowSpec))

    occupations_df = occupations_df.groupBy('row_n').pivot('Occupation').agg(F.first(F.col('Name')))

    occupations_df = occupations_df.select(
      F.col('Doctor'),
      F.col('Professor'),
      F.col('Singer'),
      F.col('Actor')
    )
    
    return occupations_df