create database jobs;
use jobs;
 
CREATE TABLE job_data
 (
     ds DATE,
     job_id INT NOT NULL,
     actor_id INT NOT NULL,
     event VARCHAR(15) NOT NULL,
     language VARCHAR(15) NOT NULL,
     time_spent INT NOT NULL,
     org CHAR(2)
 );

 INSERT INTO job_data (ds, job_id, actor_id, event, language, time_spent, org)
 VALUES 
	 ('2020-11-30', 21, 1001, 'skip', 'English', 15, 'A'),
     ('2020-11-30', 22, 1006, 'transfer', 'Arabic', 25, 'B'),
     ('2020-11-29', 23, 1003, 'decision', 'Persian', 20, 'C'),
     ('2020-11-28', 23, 1005,'transfer', 'Persian', 22, 'D'),
     ('2020-11-28', 25, 1002, 'decision', 'Hindi', 11, 'B'),
     ('2020-11-27', 11, 1007, 'decision', 'French', 104, 'D'),
     ('2020-11-26', 23, 1004, 'skip', 'Persian', 56, 'A'),
     ('2020-11-25', 20, 1003, 'transfer', 'Italian', 45, 'C');

select * from job_data;

# Case Study 1 : Job Data Analysis
# A) Number of Jobs Reviewed 

SELECT 
    COUNT(DISTINCT job_id) / (30 * 24) AS num_jobs_reviewed
FROM
    job_data
WHERE
    ds BETWEEN '2020-11-01' AND '2020-11-30';
    
# B) Throughput Analysis

with CTE as (
	select ds, count(job_id) as num_jobs, sum(time_spent) as total_time
	from job_data
	where event NOT IN ('skip')
	AND ds between '2020-11-01' AND '2020-11-30'
	GROUP BY ds
    )
select ds, 
ROUND(SUM(num_jobs) OVER (ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
/ SUM(total_time) OVER (ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),2) AS throughput_7d
FROM CTE;

# C) Language Share Analysis

select language, 
count(language) as "Language Count", 
count(*)*100/sum(count(*))
over() as percentage
from job_data
group by language
order by percentage desc;

# D) Duplicate Rows Detection

WITH cte AS 
(
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_id) AS row_num
    FROM job_data
)
SELECT * FROM cte WHERE row_num > 1;

