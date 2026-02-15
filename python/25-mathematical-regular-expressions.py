#You're given a DataFrame containing strings, each of which may or may not be a valid mathematical expression. Your task is to filter and return only those rows which contain valid mathematical expressions.
#
#✅ Valid Expression Criteria:
#A valid mathematical expression must include:
#
#One or more numbers (0-9),
#
#Separated by one or more arithmetic operators: +, -, *, /.
#
#Strings like "5+3", "6/3", or "2*3+1" are valid.
#
#Strings like "hello" or "world" are invalid.
#
#🔧 Schema
#df_math_expr
#Column Name	Data Type
#id	integer
#expression	string
#🧾 Output
#The output should include the same two columns:
#
#id
#
#expression
#Rows should be ordered in the same order as in the original DataFrame.
#
#📘 Example
#Input:
#id	expression
#1	5+3
#2	hello
#3	6/3
#4	world
#5	2*3+1
#Output:
#id	expression
#1	5+3
#3	6/3
#5	2*3+1


import pandas as pd
import numpy as np
import datetime
import json
import math
import re

def etl(df_math_expr):

  df_math_expr = df_math_expr[df_math_expr['expression'].str.contains(r'^\s*\d+(?:\.\d+)?(?:\s*[+\-*/]\s*\d+(?:\.\d+)?)', regex=True)]

  return df_math_expr