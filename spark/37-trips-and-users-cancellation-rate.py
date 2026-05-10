# ============================================================
# Trips and Users Cancellation Rate
# ============================================================
# Difficulty: HARD
# Tags: Companies, Hints
#
# ------------------------------------------------------------
# Problem
# ------------------------------------------------------------
# You are analyzing ride-hailing trip data. For each day,
# calculate the cancellation rate of trips after excluding any
# trips where either the client or the driver is banned.
#
# First, filter out all trips where the client_id or driver_id
# corresponds to a banned user. Then, for each remaining
# request_date, count the total number of trips and the number
# of cancelled trips (status containing 'cancelled'). The
# cancellation rate is the ratio of cancelled trips to total
# trips for that day.
#
# Round the cancellation rate to 2 decimal places.
#
# Schema columns:
#   trips.id, trips.client_id, trips.driver_id, trips.city_id,
#   trips.status, trips.request_date,
#   users.users_id, users.banned, users.role
#
# Output columns:
#   request_date, total_trips, cancelled_trips, cancellation_rate
#
# Sort by request_date ascending
#
# Schema (2 tables):
#   trips -> 6 cols
#   users -> 3 cols
#
# ------------------------------------------------------------
# Examples
# ------------------------------------------------------------
# Example 1
#
# Input: trips
# +----+-----------+-----------+---------+---------------------+--------------+
# | id | client_id | driver_id | city_id | status              | request_date |
# +----+-----------+-----------+---------+---------------------+--------------+
# | 1  | 1         | 10        | 1       | completed           | 2020-01-01   |
# | 2  | 2         | 11        | 1       | cancelled_by_driver | 2020-01-01   |
# | 3  | 3         | 12        | 1       | cancelled_by_client | 2020-01-01   |
# | 4  | 1         | 13        | 1       | completed           | 2020-01-02   |
# | 5  | 4         | 14        | 2       | cancelled_by_driver | 2020-01-02   |
# +----+-----------+-----------+---------+---------------------+--------------+
#
# Input: users
# +----------+--------+--------+
# | users_id | banned | role   |
# +----------+--------+--------+
# | 1        | No     | client |
# | 2        | Yes    | client |
# | 3        | No     | client |
# | 4        | No     | client |
# | 5        | No     | client |
# +----------+--------+--------+
#
# Output:
# +--------------+-------------+-----------------+-------------------+
# | request_date | total_trips | cancelled_trips | cancellation_rate |
# +--------------+-------------+-----------------+-------------------+
# | 2020-01-01   | 2           | 1               | 0.5               |
# | 2020-01-02   | 2           | 1               | 0.5               |
# | 2020-01-03   | 1           | 1               | 1.0               |
# +--------------+-------------+-----------------+-------------------+
#
# Explanation: The output shows 3 row(s) derived by applying the
# required transformations to the input data.
#
# ------------------------------------------------------------
# Constraints
# ------------------------------------------------------------
# - Exclude any trip where the client OR the driver is banned
# - Count cancelled trips as those with status containing 'cancelled'
# - Round cancellation rate to 2 decimal places
# - Sort by request_date ascending
# - Output sorted by request_date
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import datetime

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(trips, users):
    # DataFrame operations here

    users = users.alias('u')
    trips = trips.alias('t')
    
    users = users.filter(F.col('banned') == 'No')

    trips = (trips
             .join(users, how = 'inner', on = (F.col('t.client_id') == F.col('u.users_id')))
             .select(
                 F.col('t.client_id'),
                 F.col('t.driver_id'),
                 F.col('t.city_id'),
                 F.col('t.status'),
                 F.col('t.request_date'))
            )
    
    trips = (trips
             .join(users, how = 'inner', on = (F.col('t.driver_id') == F.col('users_id')))
             .select(
                 F.col('t.client_id'),
                 F.col('t.driver_id'),
                 F.col('t.city_id'),
                 F.col('t.status'),
                 F.col('t.request_date'))
             )

    trips_cancelled = (trips
                        .groupBy('request_date')
                        .agg(
                            F.count("*").alias('total_trips'),
                            F.sum(F.when(((F.col('status') == 'cancelled_by_client') | (F.col('status') == 'cancelled_by_driver')), 1).otherwise(0)).alias('cancelled_trips')
                        )
                        .withColumn(
                            'cancellation_rate',
                            F.round(F.col('cancelled_trips') / F.col('total_trips'), 2)
                        )
                        .orderBy(F.col('request_date'))
                       )

    return trips_cancelled