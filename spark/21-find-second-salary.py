#Write a solution to retrieve the second highest unique salary from the Employee table.
#If there is no second highest salary, return null (or None for Pandas users).
#
#Table: Employee
#| Column Name | Type |
#| ----------- | ---- |
#| id          | int  |
#| salary      | int  |
#The id column is the primary key, meaning it contains unique values.
#Each row in the table represents the salary of an employee.
#Example 1:
#Input:
#Employee Table:
#
#| id | salary |
#| -- | ------ |
#| 1  | 100    |
#| 2  | 200    |
#| 3  | 300    |
# 
#
#Output:
#
#| SecondHighestSalary |
#| ------------------- |
#| 200                 |
# 
#
#Example 2:
#Input:
#Employee Table:
#
#| id | salary |
#| -- | ------ |
#| 1  | 100    |
# 
#
#Output:
#
#| SecondHighestSalary |
#| ------------------- |
#| null                |
# 
#
#Explanation:
#In Example 1, the salaries are [100, 200, 300]. The second highest distinct salary is 200.
#In Example 2, there is only one salary, so there is no second highest value, and the result is null.

import datetime
import json
import math
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

def etl(input_df):
  w = Window.orderBy(col('salary').desc())

  input_df = input_df.withColumn(
    'salary_rank',
    dense_rank().over(w)
  ).filter(
    col('salary_rank') == 2
  ).select(
    col('salary').alias('SecondHighestSalary')
  )  

  return input_df