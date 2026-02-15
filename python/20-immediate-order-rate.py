#You are given a table named Orders, which contains details about food deliveries, including the order date and the customer's preferred delivery date.
#
#An immediate order is when the order date and preferred delivery date are the same.
#A scheduled order is when the preferred delivery date is later than the order date.
#Each customer places multiple orders, but their first order is defined as the one with the earliest order_date.
#Your task is to calculate the percentage of immediate first orders among all customers, rounded to two decimal places.
#
#Schema & Sample Data
#Orders Table
#| Column Name             | Data Type | Description                                          |
#| ----------------------- | --------- | ---------------------------------------------------- |
#| delivery_id             | INT       | Unique identifier for the order                      |
#| customer_id             | INT       | Unique identifier for the customer                   |
#| order_date              | DATE      | The date when the order was placed                   |
#| preferred_delivery_date | DATE      | The date when the customer wants the order delivered |
#The delivery_id column uniquely identifies each record.
#Each customer has exactly one first order, which is the order with the earliest order_date.
#Example Input & Output
#Input
#| delivery_id | customer_id | order_date | preferred_delivery_date |
#| ----------- | ----------- | ---------- | ----------------------- |
#| 1           | 1           | 2019-08-01 | 2019-08-02              |
#| 2           | 2           | 2019-08-02 | 2019-08-02              |
#| 3           | 1           | 2019-08-11 | 2019-08-12              |
#| 4           | 3           | 2019-08-24 | 2019-08-24              |
#| 5           | 3           | 2019-08-21 | 2019-08-22              |
#| 6           | 2           | 2019-08-11 | 2019-08-13              |
#| 7           | 4           | 2019-08-09 | 2019-08-09              |
#Output
#| immediate_percentage |
#| -------------------- |
#| 50.00                |
#Explanation
#Customer 1 → First order (ID 1) is scheduled.
#Customer 2 → First order (ID 2) is immediate.
#Customer 3 → First order (ID 5) is scheduled.
#Customer 4 → First order (ID 7) is immediate.
#Total customers: 4
#Immediate first orders: 2
#Percentage = (2 / 4) * 100 = 50.00%
#
#Expected Output Format
#| immediate_percentage |
#| -------------------- |
#| X.XX                 |
#Where X.XX represents the percentage rounded to two decimal places.

import pandas as pd
import numpy as np
import datetime
import json
import math
import re

def etl(input_df):

  input_df['order_date'] = input_df.order_date.astype('datetime64[ns]')

  input_df['preferred_delivery_date'] = input_df.preferred_delivery_date.astype('datetime64[ns]')

  input_df['first_order_date'] = input_df.groupby('customer_id')['order_date'].transform('min')

  input_df['immediate'] = (input_df['first_order_date'] == input_df['preferred_delivery_date']).astype('int')

  percentage = round(input_df['immediate'].sum() / input_df.customer_id.nunique(), 2)

  output_df = pd.DataFrame(data = {'immediate_percentage':[percentage * 100.00]})

  return output_df