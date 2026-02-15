#You’ve been hired by a leading pizza chain to analyze costs for an upcoming promotion offering all 3-topping pizzas at a fixed price. Your job is to calculate the total cost of every possible 3-topping pizza based on ingredient costs and sort the results.
#
#Requirements:
#Calculate the total cost for each 3-topping combination.
#Sort the results:
#First by the highest total cost (descending order).
#For ties, list the toppings alphabetically, considering the first, second, and third ingredients in sequence.
#Ensure no topping is repeated within a single pizza.
#Display topping combinations in alphabetical order for each pizza. For example, 'Chicken,Extra Cheese,Pepperoni' is acceptable, but 'Pepperoni,Chicken,Extra Cheese' is not.
#Table Schema: pizza_toppings
#| **Column Name** | **Type**      |
#| --------------- | ------------- |
#| topping_name    | varchar(255)  |
#| ingredient_cost | decimal(10,2) |
#Example Input:
#| **topping_name** | **ingredient_cost** |
#| ---------------- | ------------------- |
#| Pepperoni        | 0.50                |
#| Sausage          | 0.70                |
#| Chicken          | 0.55                |
#| Extra Cheese     | 0.40                |
#Example Output:
#| **pizza**                      | **total_cost** |
#| ------------------------------ | -------------- |
#| Chicken,Pepperoni,Sausage      | 1.75           |
#| Chicken,Extra Cheese,Sausage   | 1.65           |
#| Extra Cheese,Pepperoni,Sausage | 1.60           |
#| Chicken,Extra Cheese,Pepperoni | 1.45           |
#Explanation:
#Combinations:
#
#For 3 toppings, all unique combinations are considered. For example:
#Chicken,Pepperoni,Sausage
#Chicken,Extra Cheese,Sausage
#Extra Cheese,Pepperoni,Sausage
#Chicken,Extra Cheese,Pepperoni
#Each topping appears only once in a combination.
#Alphabetical Ordering:
#
#Ingredients within each combination are sorted alphabetically:
#Example: Chicken,Pepperoni,Sausage is valid; Pepperoni,Chicken,Sausage is not.
#Sorting:
#
#First, by total cost in descending order.
#Ties are resolved by sorting the toppings alphabetically.
#Calculation:
#
#For example, the total cost of Chicken,Pepperoni,Sausage is:
#0.55 + 0.50 + 0.70 = 1.75.
 

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from itertools import permutations
import datetime

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):

  input_df = input_df.orderBy(F.col('topping_name').asc())

  input_df = input_df.alias('a').join(input_df.alias('b'), F.col('a.topping_name') != F.col('b.topping_name')).join(input_df.alias('c'),
(F.col('c.topping_name') != F.col('b.topping_name')) & (F.col('c.topping_name') != F.col('a.topping_name'))).select(
    F.col('a.topping_name').alias('topping_1'),
    F.col('a.ingredient_cost').alias('cost_topping_1'),
    F.col('b.topping_name').alias('topping_2'),
    F.col('b.ingredient_cost').alias('cost_topping_2'),
    F.col('c.topping_name').alias('topping_3'),
    F.col('c.ingredient_cost').alias('cost_topping_3')
  )

  input_df = input_df.withColumn(
    'pizza',
  F.concat_ws(',',  F.array_sort(
      F.array(F.col('topping_1'), F.col('topping_2'), F.col('topping_3')))
  ))

  input_df = input_df.dropDuplicates(['pizza'])

  input_df = input_df.withColumn('total_cost',
                                F.round(F.col('cost_topping_1') + F.col('cost_topping_2') + F.col('cost_topping_3'), 2))

  input_df = input_df.select(
    F.col('pizza'),
    F.col('total_cost')
  ).orderBy(F.col('total_cost').desc())

  return input_df
  
    

  