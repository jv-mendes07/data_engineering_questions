#You are given a table named monthly_purchases containing transaction details. Your goal is to calculate the month-over-month percentage change in revenue. The output should include the year-month (YYYY-MM) and the percentage change in revenue for each month. The percentage change is calculated as:
#
#Percentage Change=This Month’s Revenue−Last Month’s RevenueLast Month’s Revenue×100Percentage Change=Last Month’s RevenueThis Month’s Revenue−Last Month’s Revenue​×100
#
#Round the percentage change to two decimal places.
#Ensure the data is sorted chronologically from the earliest to the latest month.
#The percentage change for the first month should be NULL since there is no prior month for comparison.
#Input Table: monthly_purchases
#| Column Name   | Data Type | Description                                 |
#| ------------- | --------- | ------------------------------------------- |
#| `id`          | INT       | Unique identifier for each transaction.     |
#| `created_at`  | DATETIME  | The date and time the transaction was made. |
#| `value`       | INT       | Revenue generated from the transaction.     |
#| `purchase_id` | INT       | Identifier for the specific purchase.       |
#Example Input Data
#| id | created_at          | value  | purchase_id |
#| -- | ------------------- | ------ | ----------- |
#| 1  | 2019-01-01 00:00:00 | 172692 | 43          |
#| 2  | 2019-01-05 00:00:00 | 177194 | 36          |
#| 3  | 2019-02-02 00:00:00 | 140032 | 25          |
#| 4  | 2019-03-02 00:00:00 | 157548 | 19          |
#Output Table: revenue_change
#| year_month | percentage_change |
#| ---------- | ----------------- |
#| 2019-01    | N/A              |
#| 2019-02    | -59.98           |
#| 2019-03    | 12.51             |

import pandas as pd
import numpy as np
import datetime
import json
import math
import re

def etl(input_df):

  input_df['created_at'] = input_df.created_at.astype('datetime64[ns]')

  input_df = input_df.sort_values(['created_at'], ascending = True)
  
  input_df['year_month'] = input_df.created_at.dt.strftime('%Y/%m')

  input_df = input_df.groupby('year_month').agg(
    total_revenue = ('value', 'sum')
  ).reset_index()

  input_df['previous_revenue'] = input_df['total_revenue'].shift(1)

  input_df['percentage_change'] = round(((input_df['total_revenue'] - input_df['previous_revenue']) / input_df['previous_revenue']) * 100, 2)

  input_df = input_df[['year_month', 'percentage_change']]
  
  return input_df