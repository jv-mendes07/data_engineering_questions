##Problem
##Each row in cra_business_information is a single order, with the customer who placed it, the date it was placed, and its cost. Report the total revenue for each customer whose orders fall in March 2019, counting only orders placed during that month. Return the customer ID (cust_id) and their total March 2019 revenue (total_revenue), and list the highest-revenue customers first.
##
##Output columns: cust_id, total_revenue
##
##Sort the results by total_revenue DESC.
##
##
##Schema
##1 table
##Expand all
##
##cra_business_information
##5 cols
##Examples
##Example 1
##
##Input:
##
##cra_business_information:
##
##id	cust_id	order_date	order_details	total_order_cost
##1	3	2019-03-04	Coat	100
##2	3	2019-03-01	Shoes	80
##4	7	2019-02-01	Coat	25
##5	7	2019-03-10	Shoes	80
##8	15	2019-03-11	Slipper	20
##11	5	2019-02-01	Shoes	80
##Output:
##
##cust_id	total_revenue
##3	180
##7	80
##15	20
##Explanation: Customer 3 placed two March orders (100 + 80 = 180), the top revenue. Customer 7's February order for 25 is excluded, leaving only their March order of 80. Customer 5 has no March order, so they do not appear.
##
##Constraints
##Include only orders placed between 2019-03-01 and 2019-03-31 (inclusive).
##A customer appears only if they have at least one order in that window.
##Ties in total_revenue may appear in any order.
##Return results matching the expected output schema and order.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

def etl(input_df):

    input_df = input_df.filter(
        (col('order_date') >= '2019-03-01') & (col('order_date') <= '2019-03-31')
    )

    input_df = input_df.groupBy(col('cust_id')).agg(
        _sum('total_order_cost').alias('total_revenue')
    ).orderBy(col('total_revenue').desc())

    return input_df