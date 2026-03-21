--As an HR analyst at a tech company, you need to analyze employee salary data to find insights about pay distribution. One of your tasks is to determine the second highest salary among all employees.
--
--If multiple employees have the same second highest salary, it should be displayed only once.
--
--salary_insights_employee table
----
----
----| employee_id | name              | salary | department_id | manager_id |
----|-------------|-------------------|--------|---------------|------------|
--| 1           | Emma Thompson     | 3800   | 1             | 6          |
--| 2           | Daniel Rodriguez  | 2230   | 1             | 7          |
--| 3           | Olivia Smith      | 2000   | 1             | 8          |
--| 4           | Liam Brown        | 2230   | 2             | 9          |
--| 5           | Sophia Lee        | 4000   | 2             | 10         |
--
--output
--
--| second_highest_salary |
--|-----------------------|
--| 2230                  |

-- Write query here
-- TABLE NAME: `salary_insights_employee`

WITH 
  rank_salary AS (
SELECT 
  employee_id,
  salary,
  DENSE_RANK() OVER(ORDER BY salary DESC) AS rank
FROM salary_insights_employee
  )
SELECT 
  DISTINCT 
  salary AS second_highest_salary
FROM rank_salary 
WHERE rank = 2;