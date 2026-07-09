## Private Equity Portfolio Valuation
## Difficulty: MEDIUM | Companies | Hints

## ---- Problem ----
## Private Equity Portfolio Valuation.
##
## You are a data engineer at a fund administration company serving private equity clients.
## Each firm's share holdings sit in a portfolio table, and a separate table records daily
## closing prices for the underlying companies; the reporting team needs the total daily value
## of each firm's portfolio. Write a query that joins pep_portfolio to pep_prices on company
## and, for each PE firm and each price date, computes the portfolio value as the sum of
## shares * closing_price across all of that firm's holdings. The firm column must be
## returned with the exact name PE_firm (the physical column is pe_firm, so alias it), and
## date must be returned as text formatted YYYY-MM-DD. Sort the results by PE_firm in
## ascending order, then by date in ascending order.
##
## Schema columns: pep_portfolio.pe_firm, pep_portfolio.company,
## pep_portfolio.shares, pep_prices.date, pep_prices.company, pep_prices.closing_price
##
## Output columns: PE_firm, date, portfolio_value

## ---- Examples ----
## Example 1

## Input:
## pep_portfolio:
## pe_firm | company | shares
## Alpha   | A       | 1000
## Alpha   | B       | 2000
## Beta    | A       | 1500
## Beta    | C       | 2500
## Gamma   | B       | 1200
## Gamma   | C       | 1300

## pep_prices:
## date       | company | closing_price
## 2023-01-01 | A       | 50
## 2023-01-01 | B       | 20
## 2023-01-01 | C       | 30
## 2023-01-02 | A       | 52
## 2023-01-02 | B       | 21
## 2023-01-02 | C       | 31

## Output:
## PE_firm | date       | portfolio_value
## Alpha   | 2023-01-01 | 90000
## Alpha   | 2023-01-02 | 94000
## Beta    | 2023-01-01 | 150000
## Beta    | 2023-01-02 | 155500
## Gamma   | 2023-01-01 | 63000

## Explanation: On 2023-01-01 Alpha holds 1000 shares of A at 50 (50,000) and 2000 shares
## of B at 20 (40,000), totaling 90000. Gamma's 2023-01-01 value is 1200 * 20 + 1300 * 30 =
## 63000. Results are grouped per firm per date and sorted by firm, then date.

## ---- Constraints ----
## - Join pep_portfolio to pep_prices on company
## - portfolio_value = SUM(shares * closing_price) grouped by firm and date
## - The firm column must be returned with the exact name PE_firm (alias the lowercase
##   pe_firm column)
## - date must be returned as text formatted YYYY-MM-DD
## - Exact output column names: PE_firm, date, portfolio_value
## - Sort by PE_firm ascending, then date ascending

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(portfolio, prices):
    
    daily_equity_prices = portfolio.join(prices, how = 'inner', on = 'company')

    daily_equity_prices = (daily_equity_prices
                           .groupBy('date', 'pe_firm')
                           .agg(
                    F.sum(F.col('shares') * F.col('closing_price')).alias('portfolio_value')
                            )
                           .select(
                               F.col('pe_firm').alias('PE_firm'), F.date_format(F.col('date'), 'yyyy-MM-dd').alias('date'), F.col('portfolio_value')
                           )
                           .orderBy(F.col('pe_firm').asc(), F.col('date').asc())
)
    
    return daily_equity_prices