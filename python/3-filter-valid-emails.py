#You are given a table or DataFrame named fve_sample that contains information about users, including their email addresses. Some of these emails may be invalid.
#
#Your task is to write a query to return all users with valid email addresses based on the following rules.
#
#✅ Valid Email Rules:
#The local part (portion before @) must:
#
#Start with a letter (A–Z or a–z)
#
#Contain only letters, digits, underscores (_), periods (.), or hyphens (-)
#
#The domain must be exactly: @validemail.com
#
#📦 Table / DataFrame: fve_sample
#Column Name	Data Type	Description
#user_id	INT	Unique ID of the user (Primary Key)
#name	VARCHAR	Name of the user
#mail	VARCHAR	Email address of the user
#📋 Sample Input (fve_sample)
#user_id	name	mail
#1	Alice	alice@validemail.com
#2	Bob	bob_123
#3	Charlie	charlie-@validemail.com
#4	Daisy	daisy.work@validemail.com
#5	Edward	edward#2023@validemail.com
#6	Frank	frank789@gmail.com
#7	George	.george@validemail.com
#✅ Expected Output
#user_id	name	mail
#1	Alice	alice@validemail.com
#3	Charlie	charlie-@validemail.com
#4	Daisy	daisy.work@validemail.com
#💡 Explanation:
#Only users whose emails follow the format:
#
#Start with a letter
#
#Only allowed characters before @
#
#Domain is exactly @validemail.com
#
#Invalid cases include:
#
#Missing domain (User 2)
#
#Illegal character like # (User 5)
#
#Wrong domain (User 6)
#
#Starts with a non-letter (User 7)


import pandas as pd
import numpy as np
import datetime
import json
import math
import re

def etl(input_df):
  
  input_df = input_df[(input_df['email'].str.match(r'^[A-Za-z][A-Za-z0-9._-]+@')) & (input_df['email'].str.contains('@dataplatform'))]

  return input_df