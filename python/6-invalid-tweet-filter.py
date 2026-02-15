#Write an SQL query to retrieve the tweet_id of all tweets that exceed 15 characters in length, making them invalid.
#
#Return the result in any order.
#
#
#Table: Tweets
#| Column Name | Type    |
#| ----------- | ------- |
#| tweet_id    | int     |
#| content     | varchar |
#tweet_id is the primary key and uniquely identifies each tweet.
#content contains text composed of characters from an American keyboard, without any special characters.
#This table stores all tweets posted on a social media platform.
#Example:
#Input - Tweets Table
#| tweet_id | content                           |
#| -------- | --------------------------------- |
#| 1        | Let us Code                       |
#| 2        | More than fifteen chars are here! |
#Expected Output
#| tweet_id |
#| -------- |
#| 2        |
#Explanation:
#Tweet 1 contains 11 characters, so it is valid.
#Tweet 2 contains 33 characters, which is greater than 15, so it is invalid and should be included in the result.

import pandas as pd
import numpy as np
import datetime
import json
import math
import re

def etl(input_df):
  
  input_df['length_tweet'] = input_df['content'].str.len()

  input_df = input_df[input_df['length_tweet'] > 15]

  input_df = input_df[['tweet_id']]

  return input_df
