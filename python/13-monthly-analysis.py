#Write a query to summarize transaction data by month and country. For each month and country, calculate:
#
#The total number of transactions (trans_count).
#The total transaction amount (trans_total_amount).
#The number of approved transactions (approved_count).
#The total amount of approved transactions (approved_total_amount).
# 
#
#Table: Transactions
#| Column Name  | Data Type | Description                                    |
#| ------------ | --------- | ---------------------------------------------- |
#| `id`         | `int`     | Unique identifier for each transaction.        |
#| `country`    | `varchar` | Country where the transaction occurred.        |
#| `state`      | `enum`    | Transaction state: `["approved", "declined"]`. |
#| `amount`     | `int`     | Amount of the transaction in dollars.          |
#| `trans_date` | `date`    | Date when the transaction occurred.            |
#Example Input:
#Transactions Table:
#
#| id  | country | state    | amount | trans_date |
#| --- | ------- | -------- | ------ | ---------- |
#| 121 | US      | approved | 1000   | 2018-12-18 |
#| 122 | US      | declined | 2000   | 2018-12-19 |
#| 123 | US      | approved | 2000   | 2019-01-01 |
#| 124 | DE      | approved | 2000   | 2019-01-07 |
#Expected Output:
#| month   | country | trans_count | approved_count | trans_total_amount | approved_total_amount |
#| ------- | ------- | ----------- | -------------- | ------------------ | --------------------- |
#| 2018-12 | US      | 2           | 1              | 3000               | 1000                  |
#| 2019-01 | US      | 1           | 1              | 2000               | 2000                  |
#| 2019-01 | DE      | 1           | 1              | 2000               | 2000                  |

import pandas as pd

def etl(input_df):
    input_df['trans_date'] = input_df.trans_date.astype('datetime64[ns]')

    input_df['month'] = input_df.trans_date.dt.to_period('M').astype('string')

    output_df_grouped = input_df.groupby(['month', 'country', ]).agg(
      trans_count = ('id', 'count'),
      approved_count = ('id', lambda x : (input_df.loc[x.index, 'state'] == 'approved').sum()),
      trans_total_amount = ('amount', 'sum'),
      approved_total_amount = ('amount', lambda x : (input_df.loc[x.index, 'amount'][input_df.loc[x.index, 'state'] == 'approved']).sum())
    ).reset_index().sort_values(['month', 'country'], ascending = True)

    return output_df_grouped