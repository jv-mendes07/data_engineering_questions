#In some cases, payment transactions may be unintentionally repeated due to user error, API failures, or retry mechanisms. This can result in the same credit card being charged multiple times at the same merchant for the same amount.
#
#🎯 Objective
#Write a SQL query to count the number of repeated payments that meet all of the following conditions:
#
#Same merchant_id
#
#Same credit_card_id
#
#Same amount
#
#Occurred within 10 minutes of a previous transaction
#
#⚠️ Important: The first transaction in any such pair should not be considered a repeated payment. Only the follow-up transaction(s) should be counted.
#
#📘 Table Schema: dpd_duplicate
#Column Name	Type	Description
#transaction_id	INTEGER	Unique ID for the transaction
#merchant_id	INTEGER	ID of the merchant
#credit_card_id	INTEGER	ID of the credit card used
#amount	INTEGER	Transaction amount
#transaction_timestamp	DATETIME	Time of the transaction
#📥 Example Input
#transaction_id	merchant_id	credit_card_id	amount	transaction_timestamp
#1	101	1	100	2022-09-25 12:00:00
#2	101	1	100	2022-09-25 12:08:00
#3	101	1	100	2022-09-25 12:28:00
#4	102	2	300	2022-09-25 12:00:00
#6	102	2	400	2022-09-25 14:00:00
#✅ Expected Output
#payment_count
#1
#💡 Explanation
#Transactions 1 and 2 occurred at the same merchant, using the same card, for the same amount, and within 10 minutes. So transaction 2 is a repeated payment.
#
#Transaction 3 is more than 10 minutes apart from both previous transactions — so not counted.
#
#Transactions 4 and 6 have different amounts or timestamps — also not counted.

import pandas as pd
import numpy as np
import datetime
import json
import math
import re

def etl(input_df):

  input_df['transaction_timestamp'] = input_df['transaction_timestamp'].astype('datetime64[ns]')
  
  input_df['prev_transaction_timestamp'] = input_df.groupby(['merchant_id', 'credit_card_id', 'amount'])['transaction_timestamp'].shift(1)

  input_df['diff_minutes'] = (input_df['transaction_timestamp'] - input_df['prev_transaction_timestamp']).dt.total_seconds() / 60

  input_df = input_df[input_df['diff_minutes'] <= 10].groupby(['merchant_id', 'credit_card_id', 'amount']).agg(
    payment_count = ('transaction_id', 'count')
  )

  return input_df