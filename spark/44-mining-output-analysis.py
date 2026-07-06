# ==============================================================================
# Mining Output Analysis
# Dificuldade: EASY | Tags: Companies
# ==============================================================================
#
# Problem:
# Total mineral output by location for the operations dashboard.
#
# You are a data engineer at Jio, supporting an industrial analytics engagement 
# for a mining company that extracts rare minerals at sites around the world. 
# Daily extraction volumes land in one table and the mine registry lives in 
# another. Operations wants a rollup of total output per location and mineral.
#
# Write a query that joins mc_extraction to mc_mines, matching 
# mc_extraction.mine_id to mc_mines.id. For each combination of mine location 
# and mineral, sum the extracted quantity across all dates and return it as 
# total_quantity. Only mines that have extraction records should appear. 
# Sort the results by location in ascending order, then by mineral in 
# ascending order.
#
# Schema columns: 
# - mc_extraction.mine_id
# - mc_extraction.date
# - mc_extraction.mineral
# - mc_extraction.quantity
# - mc_mines.id
# - mc_mines.name
# - mc_mines.location
#
# Output columns: 
# - location
# - mineral
# - total_quantity
#
# ==============================================================================
# Examples
# ==============================================================================
# Example 1
#
# Input:
#
# mc_extraction:
# +---------+------------+---------+----------+
# | mine_id | date       | mineral | quantity |
# +---------+------------+---------+----------+
# | 1       | 2023-06-30 | Gold    | 1000     |
# | 2       | 2023-06-30 | Silver  | 1200     |
# | 3       | 2023-06-30 | Diamond | 800      |
# | 1       | 2023-06-29 | Gold    | 900      |
# | 2       | 2023-06-29 | Silver  | 1300     |
# | 3       | 2023-06-29 | Diamond | 750      |
# +---------+------------+---------+----------+
#
# mc_mines:
# +----+------------+--------------+
# | id | name       | location     |
# +----+------------+--------------+
# | 1  | Mine Alpha | Australia    |
# | 2  | Mine Beta  | Canada       |
# | 3  | Mine Gamma | South Africa |
# +----+------------+--------------+
#
# Output:
# +--------------+---------+----------------+
# | location     | mineral | total_quantity |
# +--------------+---------+----------------+
# | Australia    | Gold    | 1900           |
# | Canada       | Silver  | 2500           |
# | South Africa | Diamond | 1550           |
# +--------------+---------+----------------+
#
# Explanation: 
# Mine Alpha (id 1) in Australia extracted 1000 + 900 = 1900 units of Gold 
# across the two days, so Australia/Gold shows 1900. Canada/Silver sums 
# 1200 + 1300 = 2500, and South Africa/Diamond sums 800 + 750 = 1550. 
# Rows are sorted alphabetically by location, then mineral.
#
# ==============================================================================
# Constraints
# ==============================================================================
# - Match rows using mc_extraction.mine_id = mc_mines.id; only mines with 
#   extraction records appear (inner join)
# - total_quantity is the sum of quantity per location and mineral combination 
#   across all dates
# - Output columns must be exactly location, mineral, total_quantity
# - Sort results by location ascending, then mineral ascending
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(mc_extraction, mc_mines):
    mc_mines_extraction = mc_extraction.join(mc_mines, how = 'inner', on = mc_extraction['mine_id'] == mc_mines['id'])

    mc_mines_extraction = mc_mines_extraction.groupBy('location', 'mineral').agg(
        F.sum('quantity').alias('total_quantity')
    ).orderBy(F.col('location').asc(), F.col('mineral').asc())

    return mc_mines_extraction