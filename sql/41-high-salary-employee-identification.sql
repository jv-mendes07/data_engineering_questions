-- High Salary Employee Identification
-- Difficulty: MEDIUM | Companies

-- ---- Problem ----
-- As part of your company's ongoing analysis of salary distribution, you are tasked to
-- identify the top three highest-paid employees in each department. High earners are
-- defined as employees with a salary ranked among the top three within their department. If
-- there are ties, include all employees with the same salary even if it exceeds three
-- employees in total for that department.
--
-- Schema columns:
-- he_department: department_id, department_name
-- he_employee: employee_id, name, salary, department_id, manager_id
--
-- Output columns: department_name, name, salary

-- ---- Examples ----
-- Example 1

-- Input:
-- he_department:
-- department_id | department_name
-- 1             | Data Analytics
-- 2             | Data Science
-- 3             | Engineering

-- he_employee:
-- employee_id | name             | salary | department_id | manager_id
-- 1           | Emma Thompson    | 3800   | 1             | 6.0
-- 2           | Daniel Rodriguez | 2230   | 1             | 7.0
-- 3           | Olivia Smith     | 2000   | 1             | 8.0
-- 4           | Noah Johnson     | 6800   | 2             | 9.0

-- Output:
-- department_name | name             | salary
-- Data Analytics  | James Anderson   | 4000
-- Data Analytics  | Emma Thompson    | 3800
-- Data Analytics  | Daniel Rodriguez | 2230
-- Data Science    | Noah Johnson     | 6800

-- ----

-- Write query here
-- TABLE NAME: `he_employee` and `he_department`

WITH 
    he_rank_salary AS (
SELECT 
    hd.department_name,
    he.name, 
    he.salary,
    RANK() OVER (PARTITION BY hd.department_id ORDER BY he.salary DESC) rank
FROM he_employee AS he 
INNER JOIN he_department AS hd 
ON he.department_id = hd.department_id
    )
SELECT 
    department_name,
    name,
    salary
FROM he_rank_salary 
WHERE rank <= 3
ORDER BY department_name ASC, salary DESC, name ASC