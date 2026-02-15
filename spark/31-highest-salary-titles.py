##Your task is to retrieve the job titles of employees who earn the highest salary in the company.
##
##If multiple employees earn the same maximum salary, your result should include the job titles for all such employees from the hst_position table.
##
##🗃️ Tables
##hst_employee
##Column Name	Data Type	Description
##employee_id	BIGINT	Unique ID for employee
##first_name	TEXT	First name
##last_name	TEXT	Last name
##salary	BIGINT	Employee's salary
##joining_date	DATE	Date of joining
##department	TEXT	Department name
##Sample Data:
##employee_id	first_name	last_name	salary	joining_date	department
##1	Monika	Arora	100000	2014-02-20	HR
##2	Niharika	Verma	80000	2014-06-11	Admin
##3	Vishal	Singhal	300000	2014-02-20	HR
##4	Amitah	Singh	500000	2014-02-20	Admin
##5	Vivek	Bhati	500000	2014-06-11	Admin
##6	Vipul	Diwan	200000	2014-06-11	Account
##7	Satish	Kumar	75000	2014-01-20	Account
##8	Geetika	Chauhan	90000	2014-04-11	Admin
##9	Agepi	Argon	90000	2015-04-10	Admin
##10	Moe	Acharya	65000	2015-04-11	HR
##11	Nayah	Laghari	75000	2014-03-20	Account
##12	Jai	Patel	85000	2014-03-21	HR
##hst_position
##Column Name	Data Type	Description
##employee_ref_id	BIGINT	References hst_employee.employee_id
##job_title	TEXT	Job title of the employee
##effective_from	DATE	When the position became effective
##Sample Data:
##employee_ref_id	job_title	effective_from
##1	Manager	2016-02-20
##2	Executive	2016-06-11
##8	Executive	2016-06-11
##5	Manager	2016-06-11
##4	Asst. Manager	2016-06-11
##7	Executive	2016-06-11
##6	Lead	2016-06-11
##3	Lead	2016-06-11
##🎯 Expected Output
##If the highest salary in the company is 500000, and both employees 4 and 5 earn it, the output should be:
#
#job_title
#Asst. Manager
#Manager


from pyspark.sql import SparkSession
from pyspark.sql.functions import max as spark_max

def etl(hst_employee, hst_position):

  hst_employee = hst_employee.filter(
    F.col('salary') == hst_employee.select(spark_max(F.col('salary'))).first()[0]
  )

  hst_employee_position = hst_employee.join(hst_position,
  how = 'inner', on = hst_employee.employee_id ==hst_position.employee_ref_id).select(F.col('job_title')).distinct().orderBy(F.col('employee_id').desc())

  return hst_employee_position