#Identify the top 3 companies with the highest profits globally. Arrange the results in descending order based on their profits. If two or more companies have the same profit, assign them the same rank and ensure all tied companies are included in the results. Display the company names along with their profits.
#
#Input Table Description
#
#| Column Name  | Data Type    | Description                             |
#| ------------ | ------------ | --------------------------------------- |
#| company_name | VARCHAR(100) | The name of the company.                |
#| profit       | INT          | The profit of the company (in dollars). |
#Sample Input (Table Data):
#| company_name | profit |
#| ------------ | ------ |
#| Amazon       | 1000   |
#| Apple        | 2000   |
#| Google       | 2000   |
#| Microsoft    | 1500   |
#| Tesla        | 800    |
#| Facebook     | 1500   |
#| Netflix      | 700    |
#Sample Output (Top 3 Profitable Companies):
#| rank | company_name | profit |
#| ---- | ------------ | ------ |
#| 1    | Apple        | 2000   |
#| 1    | Google       | 2000   |
#| 2    | Microsoft    | 1500   |
#| 2    | Facebook     | 1500   |
#| 3    | Amazon       | 1000   |

import pandas as pd

def etl(input_df):

  input_df = input_df.sort_values('profit', ascending = False)
  
  input_df['rank'] = input_df['profit'].rank(method = 'dense', ascending = False)

  input_df = input_df[input_df['rank'] <= 3][['rank', 'company_name', 'profit']]
  
  return input_df
