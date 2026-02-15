#You are given a table or DataFrame named csm_candidate_skills that contains information about candidates and their skills. Your task is to identify candidates who are most suitable for a Data Science job, specifically those who possess all of the following required skills:
#
#Python
#
#Tableau
#
#PostgreSQL
#
#📦 Table / DataFrame: csm_candidate_skills
#Column Name	Type	Description
#candidate_id	INTEGER	Unique ID for each candidate
#skill	VARCHAR	A skill that the candidate has
#Each row represents one skill per candidate.
#
#The table contains no duplicate rows.
#
#📋 Sample Input
#candidate_id	skill
#123	Python
#123	Tableau
#123	PostgreSQL
#234	R
#234	PowerBI
#234	SQL Server
#345	Python
#345	Tableau
#✅ Expected Output
#candidate_id
#123
#🧾 Explanation:
#Candidate 123 is included because they have all 3 required skills.
#
#Candidate 345 is missing PostgreSQL, so they are excluded.
#
#Candidate 234 does not have any of the required skills.

import pandas as pd
import numpy as np
import datetime
import json
import math
import re

def etl(input_df):

  skills = ['Python', 'Tableau', 'PostgreSQL']
  candidate_id = []
  
  for candidate in input_df.candidate_id.unique():
    
    skills_candidate = input_df[input_df['candidate_id'] == candidate].skill.unique()
    
    if all(elem in skills_candidate for elem in skills):

      candidate_id.append(candidate)

  output_df = input_df[input_df['candidate_id'].isin(candidate_id)]

  output_df = output_df.drop_duplicates('candidate_id')

  output_df = output_df[['candidate_id']]

  return output_df