#You are given a dataset that tracks user activity dates. Your task is to write a SQL query to identify all users who were active for three or more consecutive days.
#
#📘 Table Schema: ca_consecutive_act
#Column Name	Data Type	Description
#date	DATETIME	The date when the user was active
#account_id	VARCHAR(10)	Unique identifier for the user's account
#user_id	VARCHAR(10)	Unique identifier for the user
#📥 Sample Input
#date	account_id	user_id
#2021-01-01	A1	U1
#2021-01-01	A1	U2
#2021-01-06	A1	U3
#2021-01-02	A1	U1
#2020-12-24	A1	U2
#2020-12-08	A1	U1
#2020-12-09	A1	U1
#2021-01-10	A2	U4
#2021-01-11	A2	U4
#2021-01-12	A2	U4
#2021-01-15	A2	U5
#2020-12-17	A2	U4
#2020-12-25	A3	U6
#2020-12-06	A3	U7
#2020-12-06	A3	U6
#2021-01-14	A3	U6
#2021-02-07	A1	U1
#2021-02-10	A1	U2
#2021-02-01	A2	U4
#2021-02-01	A2	U5
#2020-12-05	A1	U8
#✅ Expected Output
#Return the list of users who were active for at least 3 consecutive days.
#
#user_id
#U4

import pandas as pd

def etl(input_df):

  input_df['date'] = input_df['date'].astype('datetime64[ns]')

  input_df = input_df.sort_values(['user_id', 'account_id', 'date'],
                                 ascending = True)
  
  input_df['diff_days'] = input_df.groupby(['user_id', 'account_id'])['date'].diff().dt.days

  input_df['seq_group_days'] = (input_df['diff_days'] != 1).cumsum()

  input_df = input_df.groupby(['user_id', 'seq_group_days']).agg(
    consecutive_days = ('seq_group_days', 'count')
  ).reset_index()

  input_df = input_df[input_df['consecutive_days'] == 3][['user_id']]
  
  return input_df