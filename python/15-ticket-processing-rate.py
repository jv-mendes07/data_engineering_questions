#We have a dataset that tracks customer complaints and their processing status. Your task is to determine the processing rate for each complaint type.
#
#The processing rate is defined as:
#
#
#
#Round the final result to two decimal places.
#
#🗃️ Dataset
#DataFrame name (for Python & PySpark): input_df
#
#SQL/DBT table name: tpr_ticket_processing
#
#Schema
#Column Name	Data Type
#complaint_id	Integer
#type	Integer
#processed	Boolean
#📥 Sample Input (input_df or tpr_ticket_processing)
#complaint_id	type	processed
#0	0	True
#1	0	True
#2	0	False
#3	1	True
#4	1	True
#5	1	False
#6	2	True
#7	2	False
#8	2	False
#✅ Expected Output
#type	processed_rate
#0	0.67
#1	0.67
#2	0.33


import pandas as pd

def etl(input_df):

  input_df = input_df.groupby('type').agg({'complaint_id': 'size', 'processed': 'sum'}).reset_index()

  input_df = input_df.assign(processed_rate = lambda x : round(x.processed / x.complaint_id, 2))

  input_df = input_df[['type', 'processed_rate']]

  return input_df
