#You are provided with a table containing recipe titles and their respective page numbers in a cookbook. Your objective is to construct a new table that represents how these recipes are distributed across pages when the book is open.
#
#The resulting table must contain the following three columns:
#
#left_page_number
#The page number on the left side of the book (even-numbered pages).
#left_title
#The recipe title located on the left page.
#right_title
#The recipe title located on the right page (odd-numbered pages that follow the left page).
#  
#Each page contains at most one recipe. If a page does not contain any recipe, the corresponding value should be NULL.
#
#It is guaranteed that page 0 (the inside of the front cover) is always empty.
#
#Input Table Description
#Column Name	Data Type	Description
#page_number	INT	Page number in the book
#title	VARCHAR(255)	Title of the recipe
#Sample Input
#page_number	title
#1	Scrambled eggs
#2	Fondue
#3	Sandwich
#4	Tomato soup
#6	Liver
#11	Fried duck
#12	Boiled duck
#15	Baked chicken
#Sample Output
#left_page_number	left_title	right_title
#0	NULL	NULL
#2	Fondue	Sandwich
#4	Tomato soup	NULL
#6	Liver	NULL
#12	Boiled duck	NULL

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

def etl(input_df):

    schema = StructType([
    StructField("left_page_number", IntegerType(), True),
    StructField("left_title", StringType(), True),
    StructField("right_title", StringType(), True)
  ])

    new_row = spark.createDataFrame([(0, None, None)], schema=schema)

    window = Window.orderBy(F.col('page_number').asc())
    
    input_df = (input_df
                .withColumn(
                  'right_title', F.when(F.lead("page_number", 1).over(window) == F.col('page_number') + 1, F.lead("title", 1).over(window)).otherwise(F.lit(None))
                )
                .select(
                  F.col('page_number').alias('left_page_number'),                  
                  F.col('title').alias('left_title'),
                  F.col('right_title')
                )
                .filter(
      F.col('left_page_number') % 2 == 0
    ))

    input_df = input_df.union(new_row).orderBy(F.col('left_page_number').asc())

    return input_df