--The Bank of Ireland is analyzing transactions made in December 2022 to ensure they occurred during official business hours.
--
--Your task is to identify all transaction_ids that occurred outside business hours, based on the following rules:
--
--🕒 Business Hour Rules:
--Monday to Friday:
--Open from 09:00 to 16:00 (inclusive of start and exclusive of end)
--
--Saturday and Sunday:
--The bank is closed
--
--Public Holidays (Closed):
--
--December 25th
--
--December 26th
--
--🧾 Table Schema: bhv_bank_hour
--Column Name	Data Type	Description
--transaction_id	INT	Unique identifier for each transaction.
--time_stamp	DATETIME	Date and time of the transaction (in UTC).
--📥 Sample Input
--transaction_id	time_stamp
--1	2022-12-03 10:15
--2	2022-12-05 09:30
--3	2022-12-06 08:50
--4	2022-12-07 16:10
--5	2022-12-25 12:00
--📤 Expected Output
--transaction_id
--1
--3
--4
--5
--✅ Explanation:
--Transaction 1: Occurred on Saturday (closed) → Invalid
--
--Transaction 2: Occurred on Monday within business hours → Valid
--
--Transaction 3: Too early (before 09:00 on Tuesday) → Invalid
--
--Transaction 4: Too late (after 16:00 on Wednesday) → Invalid
--
--Transaction 5: Occurred on December 25 (public holiday) → Invalid

-- Write query here
-- TABLE NAME: `bhv_bank_hour`

SELECT 
  transaction_id
FROM bhv_bank_hour
WHERE
   EXTRACT(YEAR FROM time_stamp) = 2022
AND EXTRACT(MONTH FROM time_stamp) = 12
AND (
  (time_stamp::time <= '09:00'::time OR time_stamp::time > '16:00'::time)
OR  (EXTRACT(DOW FROM time_stamp) = 0 OR EXTRACT(DOW FROM time_stamp) = 6)
OR ((EXTRACT(DAY FROM time_stamp) = 25 OR EXTRACT(DAY FROM time_stamp) = 26)
   AND EXTRACT(MONTH FROM time_stamp) = 12)
  )
