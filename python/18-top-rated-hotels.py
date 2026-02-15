#Identify the top three hotels with the highest average ratings from the provided dataset. Return the hotel name along with their corresponding average score. Ensure the results are sorted in descending order based on the average score.
#
# 
#
#Table Description
#| **Column Name**   | **Data Type**  | **Description**                        |
#| ----------------- | -------------- | -------------------------------------- |
#| **hotel_name**    | `VARCHAR(255)` | The name of the hotel.                 |
#| **average_score** | `FLOAT`        | The average rating score of the hotel. |
#Sample Input:
#| hotel_name         | average_score |
#| ------------------ | ------------- |
#| Ocean View         | 4.2           |
#| Mountain Lodge     | 3.9           |
#| Central Park Hotel | 4.7           |
#| Lakeside Inn       | 4.0           |
#| Riverside          | 4.5           |
#Sample Output:
#| hotel_name         | average_score |
#| ------------------ | ------------- |
#| Central Park Hotel | 4.7           |
#| Riverside          | 4.5           |
#| Ocean View         | 4.2           |

import pandas as pd

def etl(input_df):
    
    input_df = input_df.sort_values('average_score', ascending = False).head(3)

    return input_df