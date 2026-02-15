#As a Data Analyst at a major airline, you are presented with three DataFrames. The first, flights, contains the flight data including the flight_id, origin_airport, and destination_airport. The second, airports, consists of details of each airport including airport_id and airport_name. The third, planes, contains details about planes, including plane_id and plane_model.
#
#The data is arranged as follows:
#
#flights
#+---------------------+--------+
#|       Column        |  Type  |
#+---------------------+--------+
#|      flight_id      |  int   |
#|   origin_airport    | string |
#| destination_airport | string |
#+---------------------+--------+
#airports
#+--------------+--------+
#|    Column    |  Type  |
#+--------------+--------+
#|  airport_id  | string |
#| airport_name | string |
#+--------------+--------+
#planes
#+-------------+--------+
#|   Column    |  Type  |
#+-------------+--------+
#|  plane_id   |  int   |
#| plane_model | string |
#+-------------+--------+
#Output Schema:
#+---------------------------------+------+
#|             Column              | Type |
#+---------------------------------+------+
#|            flight_id            | int  |
#|   origin_airport_name_length    | int  |
#| destination_airport_name_length | int  |
#|       plane_model_length        | int  |
#+---------------------------------+------+
#Write a function that combines these DataFrames to return the desired schema above.
#
#In the output, the flight_id column corresponds to the id of each flight in the flights DataFrame. origin_airport_name_length and destination_airport_name_length columns represent the lengths of the names of the origin and destination airports respectively. plane_model_length is the length of the plane model's name. Each row in the output DataFrame corresponds to one flight.
#
#Please note that the airport_id in the flights DataFrame corresponds to the airport_id in the airports DataFrame. The flight_id in the flights DataFrame corresponds to the plane_id in the planes DataFrame. Also, remember that the lengths of the strings should be calculated after removing leading and trailing whitespaces.
#
#Constraints:
#
#The flights DataFrame will have at least 1 and at most 1,000,000 entries.
#The airports and planes DataFrames will each have at least 1 and at most 10,000 entries.
#The flight_id, airport_id, and plane_id fields are unique across each respective DataFrame.
#String fields will have at most length 100 and will not contain any leading or trailing whitespaces.
#String fields will not contain null values.
#Example
#
#flights
#+-----------+----------------+---------------------+
#| flight_id | origin_airport | destination_airport |
#+-----------+----------------+---------------------+
#|     1     |       A1       |         B1          |
#|     2     |       A2       |         B2          |
#|     3     |       A3       |         B3          |
#+-----------+----------------+---------------------+
#
#airports
#+------------+---------------+
#| airport_id | airport_name  |
#+------------+---------------+
#|     A1     | San Francisco |
#|     B1     |  Los Angeles  |
#|     A2     |   New York    |
#|     B2     |    Boston     |
#|     A3     |     Miami     |
#|     B3     |    Orlando    |
#+------------+---------------+
#
#planes
#+----------+-------------+
#| plane_id | plane_model |
#+----------+-------------+
#|    1     | Airbus A320 |
#|    2     | Boeing 737  |
#|    3     | Airbus A380 |
#+----------+-------------+
#
#Expected
#+---------------------------------+-----------+----------------------------+--------------------+
#| destination_airport_name_length | flight_id | origin_airport_name_length | plane_model_length |
#+---------------------------------+-----------+----------------------------+--------------------+
#|               11                |     1     |             13             |         11         |
#|                6                |     2     |             8              |         10         |
#|                7                |     3     |             5              |         11         |
#+---------------------------------+-----------+----------------------------+--------------------+

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(airports, flights, planes):

  airport_origin = airports.alias('a_o')
  airport_destination = airports.alias('a_d')

  summary_flights = (flights
                     .join(planes, how = 'inner', on = flights.flight_id == planes.plane_id)
                     .join(airport_origin, how = 'inner', on = flights.origin_airport == F.col('a_o.airport_id'))
                     .join(airport_destination, how = 'inner', on = flights.destination_airport == F.col('a_d.airport_id'))
                     .select(
                       F.col('flight_id'),
                       F.col('plane_model'),
                       F.col('a_o.airport_name').alias('origin_airport_name'),
                       F.col('a_d.airport_name').alias('destination_airport_name')
                     )
)

  summary_flights = summary_flights.select(
    F.col('flight_id'),
F.length(F.trim(F.col('destination_airport_name'))).alias('destination_airport_name_length'),    F.length(F.trim(F.col('origin_airport_name'))).alias('origin_airport_name_length'),  F.length(F.trim(F.col('plane_model'))).alias('plane_model_length')
  )
  
  return summary_flights