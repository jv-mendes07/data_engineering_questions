#Two points on a 2D plane are defined as follows:
#
#Point 1: p1(x1, y1) where x1 is the minimum Northern Latitude (LAT_N) and y1 is the minimum Western Longitude (LONG_W) from the STATION table.
#Point 2: p2(x2, y2) where x2 is the maximum Northern Latitude (LAT_N) and y2 is the maximum Western Longitude (LONG_W) from the STATION table.
#Write a query to calculate the Euclidean distance between these two points and display the result rounded to 4 decimal places.
#
#Input Format
#
#The STATION table is described as follows:
#
#Here:
#
#LAT_N represents the northern latitude.
#LONG_W represents the western longitude.
#| FIELD    | TYPE    |
#| -------- | ------- |
#| `ID`     | NUMBER  |
#| `CITY`   | VARCHAR |
#| `STATE`  | VARCHAR |
#| `LAT_N`  | NUMBER  |
#| `LONG_W` | NUMBER  |
#SAMPLE INPUT
#
#| ID | CITY  | STATE  | LAT_N | LONG_W |
#| -- | ----- | ------ | ----- | ------ |
#| 1  | CityA | State1 | 23.5  | 78.5   |
#| 2  | CityB | State2 | 45.3  | 89.6   |
#| 3  | CityC | State3 | 12.4  | 67.4   |
#| 4  | CityD | State4 | 56.7  | 92.3   |
#| 5  | CityE | State5 | 34.5  | 80.1   |
#OUPUT
#
#| Euclidean Distance |
#| ------------------ |
#| 50.8183            |

import pandas as pd
import numpy as np
import datetime
import json
import math
import re

def etl(input_df):

  x1 = min(input_df['LAT_N'])
  y1 = min(input_df['LONG_W'])

  x2 = max(input_df['LAT_N'])
  y2 = max(input_df['LONG_W'])

  euclidean_distance = math.sqrt(math.pow((x2 - x1), 2) + math.pow((y2 - y1), 2))

  df = pd.DataFrame({'Euclidean Distance': [euclidean_distance]})

  df['Euclidean Distance'] = df['Euclidean Distance'].round(4)

  return df