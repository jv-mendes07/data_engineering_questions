#Problem
#You are given two tables: an employees table and a departments table. Your task is to find the top 3 highest-paid employees in each department using dense ranking.
#
#Use DENSE_RANK to rank employees within each department by salary in descending order. If multiple employees share the same salary, they receive the same rank. Return only those employees whose dense rank is 3 or less.
#
#Schema columns:
#departments.id
#departments.name
#employees.id
#employees.name
#employees.salary
#employees.department_id
#Output columns:
#department
#employee
#salary
#Output sorted by department name ascending, then salary descending.
#
#Examples
#Example 1
#Input:
#
#departments:
#id	name
#1	Sales
#2	Engineering
#employees:
#id	name	salary	department_id
#1	Alice	100000	1
#2	Bob	90000	1
#3	Carol	85000	1
#4	David	95000	1
#5	Eve	120000	2
#Output:
#department	employee	salary
#Engineering	Eve	120000
#Engineering	Frank	110000
#Engineering	Grace	105000
#Sales	Alice	100000
#Sales	David	95000
#Sales	Bob	90000
#Explanation:
#The output shows 6 row(s) derived by applying the required transformations to the input data.
#
#Constraints
#Use DENSE_RANK to handle salary ties
#Return only employees with dense rank <= 3 per department
#Output sorted by department name ascending, then salary descending
#All employees in the output must have a valid department

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window as W
import datetime

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(departments, employees):
    
    # DataFrame operations here
    
    employees = employees.alias('e')

    departments = departments.alias('d')
    
    top_salaries = employees.join(departments, on = employees.department_id == departments.id, how = 'inner')

    window = W.partitionBy(F.col('department_id')).orderBy(F.col('salary').desc())

    top_salaries = top_salaries.withColumn(
        'rank',
        F.dense_rank().over(window)
    )

    top_salaries = top_salaries.filter(
        F.col('rank') <= 3
    )

    top_salaries = (top_salaries
                    .select(
        F.col('d.name').alias('department'),
        F.col('e.name').alias('employee'),
        F.col('salary')
    )
                    .orderBy(
                    F.col('department').asc(),
                    F.col('salary').desc()
                    ))
    
    return top_salaries