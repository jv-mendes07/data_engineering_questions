--You are given three tables: epc_students, epc_subjects, and epc_examinations. Your task is to count how many times each student has attended exams for each subject.
--
--Return a result showing every student–subject combination, even if the student has never taken an exam for that subject.
--
--🛢️ Table Schemas
--📘 Table: epc_students
--Column Name	Data Type
--student_id	int
--student_name	varchar
--student_id is the primary key.
--
--Each row represents a unique student.
--
--📗 Table: epc_subjects
--Column Name	Data Type
--subject_name	varchar
--subject_name is the primary key.
--
--Each row represents a unique subject.
--
--📙 Table: epc_examinations
--Column Name	Data Type
--student_id	int
--subject_name	varchar
--This table logs each instance of a student attending a subject exam.
--
--No primary key; duplicate entries may exist.
--
--🧾 Sample Input
--epc_students
--student_id	student_name
--1	Alice
--2	Bob
--13	John
--6	Alex
--epc_subjects
--subject_name
--Math
--Physics
--Programming
--epc_examinations
--student_id	subject_name
--1	Math
--1	Physics
--1	Programming
--2	Programming
--1	Physics
--1	Math
--13	Math
--13	Programming
--13	Physics
--2	Math
--1	Math
--📤 Expected Output
--student_id	student_name	subject_name	attended_exams
--1	Alice	Math	3
--1	Alice	Physics	2
--1	Alice	Programming	1
--2	Bob	Math	1
--2	Bob	Physics	0
--2	Bob	Programming	1
--6	Alex	Math	0
--6	Alex	Physics	0
--6	Alex	Programming	0
--13	John	Math	1
--13	John	Physics	1
--13	John	Programming	1
--🔍 Explanation
--The result must include every combination of student and subject.
--
--The attended_exams column indicates the count of times a student appeared for that subject’s exam.

--If a student never took the exam for a subject, the count should be 0.

-- Write query here
-- TABLE NAME: `epc_students` and `epc_examinations` and `epc_subjects`

SELECT 
  es.student_id,
  es.student_name,
  eps.subject_name,
  COUNT(ee.*) AS attended_exams
FROM epc_students AS es
CROSS JOIN epc_subjects AS eps 
LEFT JOIN epc_examinations AS ee 
ON es.student_id = ee.student_id
  AND eps.subject_name = ee.subject_name
GROUP BY 
  es.student_id,
  es.student_name,
  eps.subject_name
ORDER BY es.student_id