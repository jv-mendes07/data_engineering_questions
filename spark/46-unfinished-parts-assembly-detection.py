## Unfinished Parts Assembly Detection
## Difficulty: EASY | Companies

## ---- Problem ----
## Tesla is investigating production bottlenecks and needs your help to extract the relevant
## data. Your task is to identify which parts have started the assembly process but are not yet
## finished. Assumptions: The ia_parts table contains all components currently in production,
## each moving through multiple assembly_stages.
##
## Schema columns:
## ia_parts: component, completion_date, assembly_stage
##
## Output columns: component, assembly_stage

## ---- Examples ----
## Example 1

## Input:
## ia_parts:
## component | completion_date     | assembly_stage
## engine    | 2023-03-10 12:00:00 | 1
## engine    | 2023-03-11 12:00:00 | 2
## chassis   | 2023-03-10 12:00:00 | 1
## chassis   | (null)              | 2

## Output:
## component | assembly_stage
## chassis   | 2
## chassis   | 3
## dashboard | 3

## ---- Constraints ----
## - Handle NULL values appropriately
## - Return results matching the expected output schema and order


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def etl(ia_parts):

    ia_parts = ia_parts.filter(
        F.col('completion_date').isNull()
    )

    ia_parts = ia_parts.select(
        F.col('assembly_stage'),
        F.col('component')
    )

    return ia_parts