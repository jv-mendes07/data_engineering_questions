# ============================================================
# Social Media Text Correction
# ============================================================
# Difficulty: EASY
# Tags: Companies, Hints
#
# ------------------------------------------------------------
# Problem
# ------------------------------------------------------------
# You are given a table named correct_social_media_post that
# contains posts from various social media platforms. Each post
# includes metadata like likes, shares, and the post content.
# Write a SQL query that performs the following operations:
# Replace every case-sensitive occurrence of "Python" with
# "PySpark" in the text column.
#
# Schema columns:
#   correct_social_media_post: id, text, date, likes, comments,
#                              shares, platform
#
# Output columns: comments, date, id, likes, platform, shares, text
#
# Sort the results by comments.
#
# Schema (1 table):
#   correct_social_media_post -> 7 cols
#
# ------------------------------------------------------------
# Examples
# ------------------------------------------------------------
# Example 1
#
# Input: correct_social_media_post
# +----+--------------------------------+------------+-------+----------+--------+-----------+
# | id | text                           | date       | likes | comments | shares | platform  |
# +----+--------------------------------+------------+-------+----------+--------+-----------+
# | 1  | This is a Python post.         | 2022-03-01 | 10    | 3        | 2      | Twitter   |
# | 2  | Another post about Python.     | 2022-03-02 | 20    | 5        | 3      | Instagram |
# | 3  | Python is great for data analy | 2022-03-03 | 30    | 2        | 4      | Facebook  |
# | 4  | I am learning Python for machi | 2022-03-04 | 40    | 7        | 5      | Twitter   |
# +----+--------------------------------+------------+-------+----------+--------+-----------+
#
# Output:
# +----------+------------+----+-------+----------+--------+--------------------------------+
# | comments | date       | id | likes | platform | shares | text                           |
# +----------+------------+----+-------+----------+--------+--------------------------------+
# | 1        | 2022-03-06 | 6  | 60    | Facebook | 1      | PySpark web development is awe |
# | 2        | 2022-03-03 | 3  | 30    | Facebook | 4      | PySpark is great for data anal |
# | 3        | 2022-03-01 | 1  | 10    | Twitter  | 2      | This is a PySpark post.        |
# | 3        | 2022-03-09 | 9  | 90    | Facebook | 1      | Why PySpark is the best progra |
# +----------+------------+----+-------+----------+--------+--------------------------------+
#
# ------------------------------------------------------------
# Constraints
# ------------------------------------------------------------
# - Handle NULL values appropriately
# - Return results matching the expected output schema and order
# ============================================================

import pandas as pd
import numpy as np
import datetime
import json
import math
import re

def etl(social_media):
    social_media.text = social_media.text.str.replace('Python', 'PySpark')

    social_media = social_media[['comments', 'date', 'id', 'likes', 'platform', 'shares', 'text']].sort_values('comments')

    return social_media