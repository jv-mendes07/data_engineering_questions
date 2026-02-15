#Write a solution to determine the count of unique subjects taught by each teacher in a university. Each teacher may teach the same subject in multiple departments, but it should only be counted once per teacher.
#
#Return the result table in any order.
#
#The result format is as follows:
#
#Example:
#Input:
#| teacher_id | subject_id | dept_id |
#| ---------- | ---------- | ------- |
#| 1          | 2          | 3       |
#| 1          | 2          | 4       |
#| 1          | 3          | 3       |
#| 2          | 1          | 1       |
#| 2          | 2          | 1       |
#| 2          | 3          | 1       |
#| 2          | 4          | 1       |
#Teacher Table:
#
#Output:
#| teacher_id | unique_subject_count |
#| ---------- | -------------------- |
#| 1          | 2                    |
#| 2          | 4                    |

import pandas as pd

def etl(input_df):
    input_df = input_df.groupby('teacher_id').agg({'subject_id':'nunique'}).reset_index().rename(columns = {'subject_id':'unique_subject_count'})

    input_df = input_df[input_df['unique_subject_count'] > 1]

    return input_df

  