## Temperature Reading Analysis
## Difficulty: EASY | Companies | Hints

## ---- Problem ----
## Temperature Reading Analysis.
##
## You are a data engineer at a thermodynamics lab. Temperature and pressure sensors log
## into separate tables, and the analysis team wants a combined metric per experiment;
## some experiments only have a reading in one of the two tables, and those cannot be
## scored and must be left out. Write a query that joins td_temperature to td_pressure on
## experimentid, keeping only experiments that appear in both tables, and computes the
## product temperature * pressure for each matched experiment. Return the experiment ID
## aliased exactly as ExperimentID and the product aliased exactly as Result (the physical
## columns are lowercase, so quoted aliases are required). Sort the results by experiment ID
## in ascending order.
##
## Schema columns: td_pressure.experimentid, td_pressure.pressure,
## td_temperature.experimentid, td_temperature.temperature
##
## Output columns: ExperimentID, Result
##
## Schema: 2 tables
## td_pressure    - 2 cols
## td_temperature - 2 cols

## ---- Examples ----
## Example 1

## Input:
## td_pressure:
## experimentid | pressure
## 1            | 1
## 3            | 2

## td_temperature:
## experimentid | temperature
## 1            | 273.15
## 2            | 293.15
## 3            | 313.15

## Output:
## ExperimentID | Result
## 1            | 273.15
## 3            | 626.3

## Explanation: Experiment 1 appears in both tables, so its Result is 273.15 * 1 = 273.15;
## experiment 3 gives 313.15 * 2 = 626.3. Experiment 2 has a temperature reading but no
## pressure reading, so it is excluded by the inner join.

## ---- Constraints ----
## - Inner join td_temperature to td_pressure on experimentid; experiments missing from
##   either table are excluded
## - Result = temperature * pressure
## - Output columns must be aliased exactly ExperimentID and Result (quoted aliases
##   over the lowercase physical columns)
## - Sort by experiment ID in ascending order

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_pressure, df_temperature):

    df_final = df_pressure.join(df_temperature, on = 'experimentid', how = 'inner')

    df_final = df_final.withColumn(
        'Result', F.col('pressure') * F.col('temperature'))

    df_final = df_final.select(
        F.col('experimentid').alias('ExperimentID'),
        F.col('Result').alias('Result')
    )

    return df_final