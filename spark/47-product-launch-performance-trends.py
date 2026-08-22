"""
============================================================
 Product Launch Performance Trends (MEDIUM)
============================================================

Problem:
  A table records every product a company launched in a given
  year. For each company, compute how many more (or fewer)
  products it launched in 2020 than in 2019. Report the company
  name and this net difference (its 2020 launch count minus its
  2019 launch count).

Schema:
  car_launches (3 columns)
    - year          : year the product was launched
    - company_name  : name of the company
    - product_name  : name of the launched product

Output columns:
  - company_name
  - net_difference

Sort order:
  - Results sorted by net_difference DESC

Example 1:
  Input (car_launches):
    year | company_name | product_name
    2019 | Chevrolet    | Traverse
    2020 | Chevrolet    | Trailblazer
    2020 | Chevrolet    | Trax
    2020 | Chevrolet    | Blazer
    2020 | Jeep         | Wrangler
    2019 | Toyota       | Avalon
    2019 | Toyota       | Camry
    2020 | Toyota       | Corolla

  Output:
    company_name | net_difference
    Chevrolet    | 2
    Jeep         | 1
    Toyota       | -1

  Explanation:
    Chevrolet launched 3 products in 2020 (Trailblazer, Trax,
    Blazer) and 1 in 2019 (Traverse), so its net difference is
    3 - 1 = 2. Toyota launched 1 product in 2020 (Corolla)
    versus 2 in 2019 (Avalon, Camry), giving 1 - 2 = -1.

Constraints:
  - A company with no 2019 launches counts as 0 for that year
    (Jeep: 1 - 0 = 1).
  - Ties may appear in any order.
  - Return results matching the expected output schema and order.
============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):
    df = input_df.groupBy(['company_name', 'year']).agg(
        F.count('product_name').alias('products')
    )

    window = W.partitionBy('company_name').orderBy(F.col('year').asc())

    df = (df.withColumn(
        'net_difference',
        F.col('products') - F.coalesce(F.lag('products').over(window), F.lit(0))
    )
            .filter(F.col('year') == 2020)
            .select(F.col('company_name'), F.col('net_difference'))
            .orderBy(F.col('net_difference').desc()))
    
    return df