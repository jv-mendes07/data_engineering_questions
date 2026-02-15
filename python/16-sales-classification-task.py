#After launching a new advertising campaign, your company wants to assess its success by evaluating the total units sold for each product and categorizing the ad performance based on the number of units sold.
#
#Task: Write an SQL query to calculate the total number of units sold for each product. Additionally, classify the advertising performance into the following categories:
#
#Outstanding: 30+ units
#Satisfactory: 20 - 29 units
#Unsatisfactory: 10 - 19 units
#Poor: 1 - 9 units
#The output should include the product ID, the total number of units sold (in descending order), and the classified ad performance.
#
#Input Table Format
#
#| **Column Name** | **Data Type** | **Description**                                                |
#| --------------- | ------------- | -------------------------------------------------------------- |
#| `user_id`       | `INT`         | Unique identifier for each user participating in the campaign. |
#| `created_at`    | `DATETIME`    | Date and time when the transaction occurred.                   |
#| `product_id`    | `INT`         | Identifier for the product involved in the transaction.        |
#| `quantity`      | `INT`         | Number of units sold in the transaction.                       |
#| `price`         | `INT`         | Price of the product per unit during the transaction.          |
#Sample Input
#
#| **user_id** | **created_at** | **product_id** | **quantity** | **price** |
#| ----------- | -------------- | -------------- | ------------ | --------- |
#| 1           | 2020-01-01     | 101            | 25           | 200       |
#| 2           | 2020-01-01     | 102            | 5            | 150       |
#| 3           | 2020-01-02     | 103            | 15           | 300       |
#| 4           | 2020-01-03     | 101            | 10           | 200       |
#| 5           | 2020-01-04     | 102            | 22           | 150       |
#| 6           | 2020-01-05     | 104            | 8            | 120       |
#| 7           | 2020-01-06     | 105            | 18           | 250       |
#| 8           | 2020-01-07     | 101            | 30           | 200       |
#| 9           | 2020-01-08     | 103            | 20           | 300       |
#| 10          | 2020-01-09     | 104            | 9            | 120       |
#Sample Output
#
#| **product_id** | **total_units_sold** | **ad_performance** |
#| -------------- | -------------------- | ------------------ |
#| 101            | 65                   | Outstanding        |
#| 102            | 27                   | Satisfactory       |
#| 103            | 35                   | Outstanding        |
#| 104            | 17                   | Unsatisfactory     |
#| 105            | 18                   | Unsatisfactory     |
 

import pandas as pd
import numpy as np

def etl(input_df):

  input_df = input_df.groupby('product_id').agg({'quantity': 'sum'}).reset_index().rename(columns = {'quantity':'total_units_sold'}).sort_values('total_units_sold', ascending = False)

  conditions = [
    (input_df['total_units_sold'] >= 30),
    ((input_df['total_units_sold'] >= 20) & (input_df['total_units_sold'] < 30)),
    ((input_df['total_units_sold'] >= 10) & (input_df['total_units_sold'] < 20)),
    ((input_df['total_units_sold'] >= 0) & (input_df['total_units_sold'] < 10))
  ]

  values = ['Outstanding', 'Satisfactory', 'Unsatisfactory', 'Poor']

  input_df['ad_performance'] = np.select(conditions, values)
  
  return input_df