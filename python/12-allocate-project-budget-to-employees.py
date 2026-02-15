#Given a list of projects and the employees assigned to each project, calculate the budget allocated to each employee for every project. Your output should include the project title, its total budget, and the budget allocated to each employee, rounded to the nearest integer. Order the results so that projects with the highest budget per employee appear first.
#
#Dataset and Schema:
#Below are the tables used for this analysis:
#
#Table Format
#
#Project Table
#
#| **Column Name** | **Data Type** | **Description**                         |
#| --------------- | ------------- | --------------------------------------- |
#| `id`            | `INT`         | Unique identifier for the project.      |
#| `title`         | `VARCHAR(15)` | Name/title of the project.              |
#| `budget`        | `INT`         | Total budget allocated for the project. |
#Employee Table
#
#| **Column Name** | **Data Type** | **Description**                                    |
#| --------------- | ------------- | -------------------------------------------------- |
#| `emp_id`        | `INT`         | Unique identifier for the employee.                |
#| `project_id`    | `INT`         | Foreign key referencing the `id` in `ms_projects`. |
# 
#
#Sample Input Table
#
#Project Table 
#
#| **id** | **title** | **budget** |
#| ------ | --------- | ---------- |
#| 1      | Project1  | 29498      |
#| 2      | Project2  | 32487      |
#| 3      | Project3  | 43909      |
#| 4      | Project4  | 15776      |
#| 5      | Project5  | 36268      |
#| 6      | Project6  | 41611      |
#| 7      | Project7  | 34003      |
#| 8      | Project8  | 49284      |
#| 9      | Project9  | 32341      |
#| 10     | Project10 | 47587      |
#| 11     | Project11 | 11705      |
#| 12     | Project12 | 10468      |
#| 13     | Project13 | 43238      |
#| 14     | Project14 | 30014      |
#| 15     | Project15 | 48116      |
#| 16     | Project16 | 19922      |
#| 17     | Project17 | 19061      |
#| 18     | Project18 | 10302      |
#| 19     | Project19 | 44986      |
#| 20     | Project20 | 19497      |
#Employee Table
#
#| **emp_id** | **project_id** |
#| ---------- | -------------- |
#| 10592      | 1              |
#| 10593      | 2              |
#| 10594      | 3              |
#| 10595      | 4              |
#| 10596      | 5              |
#| 10597      | 6              |
#| 10598      | 7              |
#| 10599      | 8              |
#| 10600      | 9              |
#| 10601      | 10             |
#| 10602      | 11             |
#| 10603      | 12             |
#| 10604      | 13             |
#| 10605      | 14             |
#| 10606      | 15             |
#| 10607      | 16             |
#| 10608      | 17             |
#| 10609      | 18             |
#| 10610      | 19             |
#| 10611      | 20             |
# 
#
#Sample Ouput Table
#
#| **title** | **budget** | **budget_per_employee** |
#| --------- | ---------- | ----------------------- |
#| Project8  | 49284      | 49284                   |
#| Project15 | 48116      | 48116                   |
#| Project10 | 47587      | 47587                   |
#| Project19 | 44986      | 44986                   |
#| Project3  | 43909      | 43909                   |
#| Project13 | 43238      | 43238                   |
#| Project6  | 41611      | 41611                   |
#| Project5  | 36268      | 36268                   |
#| Project7  | 34003      | 34003                   |
#| Project2  | 32487      | 32487                   |
#| Project9  | 32341      | 32341                   |
#| Project14 | 30014      | 30014                   |
#| Project1  | 29498      | 29498                   |
#| Project16 | 19922      | 19922                   |
#| Project20 | 19497      | 19497                   |
#| Project17 | 19061      | 19061                   |
#| Project4  | 15776      | 15776                   |
#| Project11 | 11705      | 11705                   |
#| Project12 | 10468      | 10468                   |
#| Project18 | 10302      | 10302                   |

import pandas as pd

def etl(apb_employee_table,apb_project_table):

  employee_budget = pd.merge(apb_employee_table, apb_project_table, left_on = 'project_id', right_on = 'id')

  employee_budget_sum = employee_budget.groupby(['project_id', 'title']).agg({'budget':'sum'}).reset_index()

  per_employee_budget = employee_budget.groupby(['project_id', 'title', 'emp_id']).agg({'budget': 'sum'}).reset_index().rename(columns = {'budget': 'budget_per_employee'})

  employee_budget_final = pd.merge(per_employee_budget, employee_budget_sum, on = 'project_id', suffixes = ('_left', '_right'))

  employee_budget_final = employee_budget_final[['title_left', 'budget', 'budget_per_employee']].rename(columns = {'title_left': 'title'}).sort_values('budget_per_employee', ascending = False)
  
  return employee_budget_final