#Creating a Database
drop database jobs;
Create database jobs;

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

-- CASE 2 INVESTIGATING METRIC SPIKE
-- Weekly User Enagement

-- select extract(week from occurred_at) as week_num,
select week(occurred_at) as week_num,
count(distinct user_id) as user_count
from events where event_type='Engagement'
group by week_num
order by week_num;


