--You work as a data analyst for a company that tracks employee salary payouts. Your goal is to calculate the average salary paid in March 2024 for each department and compare it to the company-wide average for the same month.
--
--For each department that had salary activity in March 2024, determine whether its average salary was:
--
--'higher' than the company average
--
--'lower' than the company average
--
--'same' as the company average
--
--🧾 Output Requirements:
--Return a table with the following columns:
--
--comparison	department_id	payment_month
--'higher' / 'lower' / 'same'	INT	Format: MM-YYYY
--The result must be based only on March 2024 salaries.
--
--Use actual float comparison (don’t round until after comparison).
--
--The format of payment_month should be exactly 03-2024.
--
--🗃 Table: sc_employee
--employee_id	name	salary	department_id	manager_id
--1	Emma Thompson	3800	1	6
--2	Daniel Rodriguez	2230	1	7
--3	Olivia Smith	7000	1	8
--4	Ethan Brown	6800	1	9
--5	Sophia Martinez	1750	1	11
--🧾 Table: sc_salary
--salary_id	employee_id	amount	payment_date
--1	1	3800	2024-03-01 00:00:00
--2	2	2230	2024-03-01 00:00:00
--3	3	7000	2024-03-01 00:00:00
--4	4	6800	2024-03-01 00:00:00
--5	5	1750	2024-03-01 00:00:00
--✅ Expected Output:
--comparison	department_id	payment_month
--same	1	03-2024

-- Write query here
-- TABLE NAME: `sc_employee` & `sc_salary`

WITH 
  avg_salary_department AS (
SELECT 
  department_id,
  TO_CHAR(ss.payment_date, 'MM-YYYY') AS payment_month,
  AVG(amount) AS average_salary_dept
FROM sc_salary AS ss 
INNER JOIN sc_employee AS se 
ON ss.employee_id = se.employee_id
WHERE 
  payment_date >= '2024-03-01'
AND payment_date <= '2024-03-31'
GROUP BY 
  se.department_id,
  payment_month
  ),
  avg_salary AS (
SELECT 
  TO_CHAR(payment_date, 'MM-YYYY') AS payment_month,
  AVG(amount) AS average_salary
FROM sc_salary AS ss 
WHERE 
  payment_date >= '2024-03-01'
AND payment_date <= '2024-03-31'
GROUP BY payment_month
  )
SELECT 
  asd.department_id,
  asd.payment_month,
  CASE 
    WHEN asd.average_salary_dept > ass.average_salary THEN 'higher'
    WHEN asd.average_salary_dept < ass.average_salary THEN 'lower'
    ELSE 'same'
  END AS comparison
FROM avg_salary_department AS asd 
INNER JOIN avg_salary AS ass
ON asd.payment_month = ass.payment_month