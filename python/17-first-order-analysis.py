#An order is classified as:
#
#Immediate: When the order_date matches the customer_pref_delivery_date.
#Scheduled: When the customer_pref_delivery_date is after the order_date.
#For each customer, determine their first order (the order with the earliest order_date). Then, calculate the percentage of customers whose first order is immediate. Round the percentage to 2 decimal places.
#
#Table: Delivery
#| Column Name                   | Data Type | Description                                       |
#| ----------------------------- | --------- | ------------------------------------------------- |
#| `delivery_id`                 | `int`     | Unique identifier for each delivery.              |
#| `customer_id`                 | `int`     | Unique identifier for each customer.              |
#| `order_date`                  | `date`    | Date when the customer placed the order.          |
#| `customer_pref_delivery_date` | `date`    | Customer's preferred delivery date for the order. |
#Example Input:
#Delivery Table:
#| delivery_id | customer_id | order_date | customer_pref_delivery_date |
#| ----------- | ----------- | ---------- | --------------------------- |
#| 1           | 1           | 2019-08-01 | 2019-08-02                  |
#| 2           | 2           | 2019-08-02 | 2019-08-02                  |
#| 3           | 1           | 2019-08-11 | 2019-08-12                  |
#| 4           | 3           | 2019-08-24 | 2019-08-24                  |
#| 5           | 3           | 2019-08-21 | 2019-08-22                  |
#| 6           | 2           | 2019-08-11 | 2019-08-13                  |
#| 7           | 4           | 2019-08-09 | 2019-08-09                  |
#Expected Output:
#| immediate_percentage |
#| -------------------- |
#| 50.00                |

import pandas as pd

def etl(input_df):

  input_df['order_date'] = input_df['order_date'].astype('datetime64[ns]')

  input_df['customer_pref_delivery_date'] = input_df['customer_pref_delivery_date'].astype('datetime64[ns]')

  input_df['first_order_date'] = input_df.groupby('customer_id')['order_date'].transform('min')

  input_df['immediate'] = (input_df['first_order_date'] == input_df['customer_pref_delivery_date']).astype(int)

  percentage = round(input_df['immediate'].sum() / input_df.customer_id.nunique(), 2)

  output_df = pd.DataFrame(data={'immediate_percentage': [percentage * 100.00]})

  return output_df