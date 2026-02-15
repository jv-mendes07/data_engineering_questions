##Write a query to determine the classification of each triangle based on the three side lengths provided in the TRIANGLEStable. For each row, output one of the following classifications:
##
##Equilateral: All three sides are of equal length.
##Isosceles: Two sides are of equal length.
##Scalene: All three sides have different lengths.
##Not A Triangle: The side lengths AA, BB, and CC cannot form a valid triangle.
##Input Format
##
##The TRIANGLES table is structured as follows:
##
##| Column | Type |
##| ------ | ---- |
##| `A`    | INT  |
##| `B`    | INT  |
##| `C`    | INT  |
##Each row specifies the lengths of three sides of a triangle.
##
##Sample Input
##
##| A  | B  | C  |
##| -- | -- | -- |
##| 20 | 20 | 23 |
##| 20 | 20 | 20 |
##| 20 | 21 | 22 |
##| 13 | 14 | 30 |
##Sample Output
#
#| Classification |
#| -------------- |
#| Isosceles      |
#| Equilateral    |
#| Scalene        |
#| Not A Triangle |

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):
  
  input_df = input_df.withColumn(
    'Classification', 
    F.when((F.col('A') == F.col('B')) & (F.col('B') == F.col('C')), 'Equilateral').when((F.col('A') == F.col('B')) | (F.col('B') == F.col('C')), 'Isosceles').when((F.col('A') != F.col('B')) & (F.col('B') != F.col('C')) & (F.col('A') != F.col('C')) & (F.col('A') + F.col('B') > F.col('C')), 'Scalene').otherwise('Not A Triangle')).select(F.col('Classification')
        )

  return input_df