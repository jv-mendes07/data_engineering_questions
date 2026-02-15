#Determine the average score for each project, but only include projects where scores have been submitted by more than one unique team member. Ignore duplicate scores from the same member for the same project.
#
#Your output should include the project ID and the calculated average score for each qualifying project.
#
#Input Table
#Project Scores Table
#| project_id | team_member_id | score | date       |
#| ---------- | -------------- | ----- | ---------- |
#| 101        | 1              | 5     | 2024-07-25 |
#| 101        | 2              | 8     | 2024-09-22 |
#| 101        | 2              | 3     | 2024-09-24 |
#| 101        | 2              | 5     | 2024-10-14 |
#| 101        | 6              | 6     | 2024-10-14 |
#| 101        | 6              | 5     | 2024-09-13 |
#| 102        | 6              | 3     | 2024-09-04 |
#| 102        | 6              | 4     | 2024-08-17 |
#| 102        | 6              | 3     | 2024-08-22 |
#| 102        | 3              | 3     | 2024-08-26 |
#| 102        | 1              | 5     | 2024-08-04 |
#| 102        | 1              | 2     | 2024-10-12 |
#| 102        | 1              | 1     | 2024-08-17 |
#| 102        | 4              | 2     | 2024-10-07 |
#| 102        | 4              | 2     | 2024-08-24 |
#| 102        | 5              | 4     | 2024-07-16 |
#| 102        | 5              | 4     | 2024-08-06 |
#| 103        | 6              | 4     | 2024-10-14 |
#| 103        | 6              | 6     | 2024-08-23 |
#| 103        | 4              | 4     | 2024-08-23 |
#Output Table
#Expected Output
#| project_id | average_score |
#|------------|---------------|
#| 101        | 5.3           |
#| 102        | 3             |
#| 103        | 4.7           |

import pandas as pd

def etl(input_df):
  input_df = input_df.drop_duplicates(subset=['team_member_id', 'project_id'])

  input_df_grouped = input_df.groupby('project_id').agg({'team_member_id': 'nunique'}).reset_index()

  project_id_filter = input_df_grouped.project_id.unique()

  input_df = input_df[input_df.project_id.isin(project_id_filter)]

  input_df = input_df.drop_duplicates(subset=['project_id', 'score'])

  input_df = input_df.groupby('project_id').agg({'score':'mean'}).reset_index().rename(columns = {'score':'average_score'})

  return input_df