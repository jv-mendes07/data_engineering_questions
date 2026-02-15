--You are working as a Data Analyst tasked with analyzing historical stock performance for major tech companies. Your goal is to identify the highest and lowest monthly open prices for each stock symbol in the dataset.
--
--🧠 Objective
--For each stock:
--
--Display the stock's ticker symbol.
--
--Identify the month and year (formatted as 'Mon-YYYY') when the stock had:
--
--The highest open price
--
--The lowest open price
--
--Include the corresponding open prices for these months.
--
--Sort the final result by the ticker symbol in ascending order.
--
--🛢 Table: faang_stock_prices
--Column Name	Data Type	Description
--date	date	The date of the stock record
--ticker	varchar	The stock ticker (e.g., AAPL, GOOG)
--open	float	Opening price of the stock on date
--high	float	Highest price of the day
--low	float	Lowest price of the day
--close	float	Closing price of the stock on date
--📥 Sample Input
--date	ticker	open	high	low	close
--2023-01-31	AAPL	142.28	144.34	142.70	144.29
--2023-05-31	AAPL	176.76	179.35	177.33	177.25
--2023-01-31	GOOG	100.34	101.50	100.20	101.45
--2023-05-31	GOOG	115.67	118.00	115.00	116.00
--📤 Expected Output
--ticker	highest_mth	highest_open	lowest_mth	lowest_open
--AAPL	May-2023	176.76	Jan-2023	142.28
--GOOG	May-2023	115.67	Jan-2023	100.34
--🔍 Explanation
--For AAPL:
--
--Highest open: 176.76 in May 2023
--
--Lowest open: 142.28 in January 2023
--
--For GOOG:
--
--Highest open: 115.67 in May 2023
--
--Lowest open: 100.34 in January 2023
--
--The output presents a clear snapshot of monthly high and low open prices for each stock.
--
--✅ Output Columns
--ticker: The stock symbol
----
--highest_mth: Month-Year with the highest open price (formatted as Mon-YYYY)
--
--highest_open: Corresponding highest open price
--
--lowest_mth: Month-Year with the lowest open price (formatted as Mon-YYYY)
--
--lowest_open: Corresponding lowest open price--

-- Write query here
-- TABLE NAME: faang_stock_prices
WITH 
  open_price AS (
SELECT 
  ticker,
  MAX(open) AS highest_open,
  MIN(open) AS lowest_open
FROM faang_stock_prices
GROUP BY 
  ticker
  ),
  highest_open AS (
SELECT 
  fsp.ticker,
  TO_CHAR(date, 'Mon-YYYY') AS highest_mth,
  op.highest_open
FROM faang_stock_prices AS fsp
INNER JOIN open_price AS op  
ON fsp.ticker = op.ticker
  AND fsp.open = op.highest_open
  ),
  lowest_open AS (
SELECT 
  fsp.ticker,
  TO_CHAR(date, 'Mon-YYYY') AS lowest_mth,
  op.lowest_open
FROM faang_stock_prices AS fsp
INNER JOIN open_price AS op  
ON fsp.ticker = op.ticker
  AND fsp.open = op.lowest_open
  )
SELECT 
  ho.ticker,
  ho.highest_mth,
  ho.highest_open,
  lo.lowest_mth,
  lo.lowest_open
FROM highest_open AS ho 
INNER JOIN lowest_open AS lo 
ON ho.ticker = lo.ticker
ORDER BY ticker ASC