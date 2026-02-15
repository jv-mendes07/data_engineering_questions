You are working as an Economist and need to compute the annual growth rate of GDP from multiple economic data sources. The GDP growth rate is defined as the percentage increase in a country's GDP from one year to the next, using the formula:



📦 Input
You are given two DataFrames with economic data:

gdp_df1
Column Name	Data Type
Country	String
Year	Integer
GDP	Double
gdp_df2
Column Name	Data Type
Country	String
Year	Integer
GDP	Double
These DataFrames may contain overlapping or distinct countries and years.

✅ Your Task
Write a function that:

Combines both DataFrames.

Calculates the GDP growth rate for each country and year.

Returns the result in the format below.

📤 Output Schema
Column Name	Data Type
Country	String
Year	Integer
GDP_growth_rate	Double
#📌 Constraints
#The output must be sorted by Country and then by Year (both ascending).
#
#GDP growth rate should be rounded to two decimal places.
#
#If the previous year’s GDP is not available for a country, the GDP growth rate should be null/None.
#
#Assume all GDP values are clean and non-negative.
#
#🧪 Example
#Input
#gdp_df1
#
#Country	Year	GDP
#USA	2018	20544.34
#USA	2019	21427.70
#China	2018	13894.04
#gdp_df2
#
#Country	Year	GDP
#China	2019	14402.72
#India	2018	2713.61
#India	2019	2868.93
#✅ Expected Output
#Country	Year	GDP_growth_rate
#China	2018	 
#China	2019	3.66
#India	2018	 
#India	2019	5.72
#USA	2018	 
#USA	2019	4.3
#💡 Note: The empty values represent cases where previous year's GDP is not available.

import pandas as pd
import numpy as np
import datetime
import json
import math
import re

def etl(gdp_df1, gdp_df2):
  
  gdp_df = pd.concat([gdp_df1, gdp_df2], axis = 0)

  gdp_df = gdp_df.sort_values(['Country', 'Year'], ascending = True)

  gdp_df['GDP_previous_year'] = gdp_df.groupby('Country')['GDP'].shift(1)

  gdp_df['GDP_growth_rate'] = round(((gdp_df['GDP'] - gdp_df['GDP_previous_year']) / gdp_df['GDP_previous_year']) * 100, 2)

  gdp_df = gdp_df[['Country', 'Year', 'GDP_growth_rate']]
  
  return gdp_df