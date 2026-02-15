##Identify all email records from days where the number of distinct users receiving emails is greater than the number of distinct users sending emails. This task will help you understand aggregation and filtering techniques in relational data processing.
##
##Input Table Schema
##| **Column Name** | **Data Type** | **Description**                                  |
##| --------------- | ------------- | ------------------------------------------------ |
##| `id`            | `INT`         | Unique identifier for each email record.         |
##| `from_user`     | `VARCHAR(50)` | Sender of the email.                             |
##| `to_user`       | `VARCHAR(50)` | Recipient of the email.                          |
##| `day`           | `INT`         | The day (as an integer) when the email was sent. |
##Sample Input Data
##| **id** | **from_user**      | **to_user**        | **day** |
##| ------ | ------------------ | ------------------ | ------- |
##| 0      | 6edf0be4b2267df1fa | 75d295377a46f83236 | 10      |
##| 1      | 6edf0be4b2267df1fa | 32ded68d89443e808  | 6       |
##| 2      | 6edf0be4b2267df1fa | 55e60cfcc9dc49c17e | 10      |
##| 3      | 6edf0be4b2267df1fa | e0e0defbb9ec47f6f7 | 6       |
##| 4      | 6edf0be4b2267df1fa | 47be2887786891367e | 1       |
##Sample Output Data
##| **id** | **from_user**      | **to_user**        | **day** |
##| ------ | ------------------ | ------------------ | ------- |
##| 0      | 6edf0be4b2267df1fa | 75d295377a46f83236 | 10      |
##| 2      | 6edf0be4b2267df1fa | 55e60cfcc9dc49c17e | 10      |
##| 18     | 75d295377a46f83236 | 47be2887786891367e | 10      |
##| 19     | 75d295377a46f83236 | 5b8754928306a18b68 | 10      |

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('filter-valid-email-days').getOrCreate()

def etl(input_df):
  
  filter_days = (input_df
                 .groupBy('day')
                 .agg(
    F.countDistinct(F.col('from_user')).alias('user_sending'),
    F.countDistinct(F.col('to_user')).alias('user_receiving')
                  )
                 .filter(
                F.col('user_receiving') > F.col('user_sending'))
                 .select(
                   F.col('day')
                 ))

  input_df = (input_df
              .join(filter_days, on = 'day', how = 'inner')
              .select(
                F.col('id'), 
                F.col('from_user'), 
                F.col('to_user'), 
                F.col('day')
              ))

  return input_df