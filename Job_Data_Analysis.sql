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

-- CASE 2 INVESTIGATING METRIC SPIKE

use jobs;

#Users Tables

show variables like 'secure_file_priv';

# Creating table Email_events

CREATE TABLE EMAIL_EVENTS(
USER_ID INT NOT NULL, 
OCCURRED_AT Varchar(100),
ACTION VARCHAR(100),
USER_TYPE INT);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv'
INTO TABLE EMAIL_EVENTS
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS; 

# Creating table events

CREATE TABLE EVENTS(
user_id	int,
occurred_at	Varchar(100),
event_type	VARCHAR(100),
event_name	VARCHAR(100),
location	VARCHAR(100),
device	VARCHAR(100),
user_type int);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv'
INTO TABLE EVENTS
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS; 

# Creating table Users

create table users(
user_id	int, 
created_at varchar(100),	
company_id int,	
language varchar(100),
activated_at varchar(100),
state varchar(100));

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv'
INTO TABLE USERS
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS; 

-- Users - created_at 

ALTER TABLE USERS ADD COLUMN temp_created_at datetime;

# select * from users;

UPDATE users SET temp_created_at = STR_TO_DATE(created_at,'%d-%m-%Y %H:%i');

ALTER TABLE users DROP COLUMN created_at;

ALTER TABLE users CHANGE COLUMN temp_created_at created_at DATETIME;

-- Users - activated_at 

ALTER TABLE USERS ADD COLUMN temp_activated_at datetime;

UPDATE users SET temp_activated_at = STR_TO_DATE(activated_at,'%d-%m-%Y %H:%i');

ALTER TABLE users DROP COLUMN activated_at;

ALTER TABLE users CHANGE COLUMN temp_activated_at activated_at DATETIME;

# select * from users;

-- Events - occurred_at

ALTER TABLE EVENTS ADD COLUMN temp_occurred_at datetime;

UPDATE EVENTS SET temp_occurred_at = STR_TO_DATE(occurred_at,'%d-%m-%Y %H:%i');

ALTER TABLE EVENTS DROP COLUMN occurred_at;

ALTER TABLE EVENTS CHANGE COLUMN temp_occurred_at occurred_at DATETIME;

-- Email_Events - occurred_at

ALTER TABLE Email_Events ADD COLUMN temp_occurred_at datetime;

UPDATE Email_Events SET temp_occurred_at = STR_TO_DATE(OCCURRED_AT,'%d-%m-%Y %H:%i');

ALTER TABLE Email_Events DROP COLUMN occurred_at;

ALTER TABLE Email_Events CHANGE COLUMN temp_occurred_at occurred_at DATETIME;

# SELECT * FROM Email_EVENTS;

# Case Study 2: Investigating Metric Spike

# A) Weekly User Enagement

select week(occurred_at) as week_num,
count(distinct user_id) as user_count
from events where event_type='Engagement'
group by week_num
order by week_num;

# B) Weekly User Growth

WITH weekly_user_counts AS (
  SELECT 
   EXTRACT(year FROM created_at) AS year,
     week(created_at) AS week_num, state, user_id,
        COUNT(DISTINCT user_id) AS user_count
  FROM users
  -- WHERE state = "active"
  GROUP BY year, week_num,state, user_id
)
SELECT year, week_num, user_count, SUM(user_count) OVER (ORDER BY year, week_num) AS cumulative_users
FROM weekly_user_counts
ORDER BY year, week_num;

# C) Weekly Retention 

SELECT COUNT(user_id) AS total_users, SUM(CASE WHEN retention_week = 1 THEN 1 ELSE 0 END) AS per_week_retention
FROM (
    SELECT a.user_id, a.sign_up_week, b.engagement_week, b.engagement_week - a.sign_up_week AS retention_week
    FROM (
        (SELECT DISTINCT user_id, EXTRACT(week FROM occurred_at) AS sign_up_week
         FROM events
         WHERE event_type = 'signup_flow' AND event_name = 'complete_signup' AND EXTRACT(week FROM occurred_at) = 18) a
        LEFT JOIN
        (SELECT DISTINCT user_id, EXTRACT(week FROM occurred_at) AS engagement_week
         FROM events
         WHERE event_type = 'engagement') b
        ON a.user_id = b.user_id
    )
    GROUP BY a.user_id, a.sign_up_week, b.engagement_week 
    ORDER BY a.user_id, a.sign_up_week 
) subquery;

# D) Weekly Engagement Per Device

select 
extract(year from occurred_at) as year_num,
extract(week from occurred_at) as week_num, 
device,
count(distinct user_id) as no_of_users
from events
where event_type = 'Engagement'
group by year_num,week_num,device
order by no_of_users desc;


# E) Email Engament 

with CTE as (
    SELECT *,
    CASE
        WHEN action IN ('sent_weekly_digest', 'sent_reengagement_email') THEN 'email_sent'
        WHEN action IN ('email_open') THEN 'email_opened'
        WHEN action IN ('email_clickthrough') THEN 'email_clicked'
    END AS email_cat
    FROM email_events
)  select 100.0 * SUM(email_cat = 'email_opened') / SUM(email_cat = 'email_sent') AS email_opening_rate,
    100.0 * SUM(email_cat = 'email_clicked') / SUM(email_cat = 'email_sent') AS email_clicking_rate
FROM CTE;