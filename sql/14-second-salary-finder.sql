--You're given a dataset of employees with their salaries and the departments they belong to. Your task is to:
--
--Identify the second-highest salary for each department.
--
--Return the employee ID, department name, and the second-highest salary.
--
--If the highest salary is held by multiple employees, the second-highest must be the next distinct lower salary.
--
--Do not skip ranks when assigning them — the rankings should be sequential.
--
--🧾 Input Table: ssf_employees
--employee_id	department_name	salary
--1	HR	5000
--2	IT	7000
--3	IT	6000
--4	HR	4000
--5	IT	7000
--6	HR	4500
--7	Finance	3000
--8	Finance	3500
--9	IT	5500
--✅ Expected Output
--department_name	second_highest_salary	employee_id
--HR	4500	6
--IT	6000	3
--Finance	3000	7
--💡 Notes
--Use ranking functions like DENSE_RANK or RANK with caution — in this case, do not eliminate duplicates.
--
--The result should show one row per department with the employee who earns the second-highest distinct salary.
--
--In case of ties for the second-highest, return any one employee with that salary.

-- Write query here
-- TABLE NAME: `ssf_employees`

WITH 
  rank_salaries AS (
SELECT 
  employee_id,
  department_name,
  salary,
  DENSE_RANK() OVER(PARTITION BY department_name ORDER BY salary DESC) AS rank
FROM ssf_employees
  )
SELECT 
  department_name,
  salary AS second_highest_salary,
  employee_id
FROM rank_salaries
WHERE rank = 2
