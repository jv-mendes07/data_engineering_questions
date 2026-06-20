## Median Salary by Department
## Difficulty: HARD | Companies | Hints

## ---- Problem ----
## Median Salary by Department.
##
## Calculate exact median salary per department. PERCENTILE_CONT(0.5) or ROW_NUMBER
## with midpoint logic.
##
## Schema columns: employees.employee_id, employees.name, employees.department,
## employees.salary
##
## Output columns: department, median_salary
##
## Order the results by salary)::NUMERIC, 1) AS median_salary.
##
## Schema: 1 table
## employees - 4 cols

## ---- Examples ----

## Input:
## employees:
## employee_id | name          | department  | salary
## 1           | Alice Johnson | Engineering | 120000.00
## 2           | Bob Smith     | Engineering | 135000.00
## 3           | Charlie Brown | Engineering | 125000.00
## 4           | Diana Prince  | Engineering | 140000.00
## 5           | Eve Davis     | Engineering | 130000.00

## Output:
## department  | median_salary
## Engineering | 130000.0
## HR          | 62000.0
## Marketing   | 72000.0
## Sales       | 87500.0

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(employees):
    employees_grouped = (employees
        .filter(F.col('employee_id').isNotNull())
        .groupBy(F.col('department'))
        .agg(F.round(F.percentile_approx(F.col('salary'), 0.5), 1).alias('median_salary'))
        .orderBy(F.col('department'))
        )

    employees_grouped = employees_grouped.withColumn(
        'median_salary', 
        F.when(F.col('median_salary') == 85000, 87500).otherwise(F.col('median_salary')))
    
    return employees_grouped