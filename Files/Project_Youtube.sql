
-----------------------------------
-- SETUP Tables for Project_Youtube
-----------------------------------


-- Create database
DROP DATABASE IF EXISTS project_youtube CASCADE;
create database project_youtube;

-- Use database
use project_youtube;


--
--Question 1
---
-- Create table for storing category information
create table categoriesInfo(Category STRING,VideoCount int) row format delimited fields terminated by '\t';

-- Get description of newly created table
describe categoriesInfo;

-- LOAD DATA from output of MAPREDUCE
LOAD DATA INPATH '/project/youtube/1/output/part-r-00000' into Table categoriesInfo;

-- Select top 5 categories
INSERT OVERWRITE DIRECTORY '/project/youtube/1/Hive_Final_Output/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
select * from categoriesInfo sort by videocount desc  limit 5;

---
-- Question 2 [ Answer from MapReduce program is already sorted. This is just a hive example ] 
---
create table ratingsTable(videoid STRING,rating float) row format delimited fields terminated by '\t';

LOAD DATA INPATH '/project/youtube/2/output/part-r-00000' into Table ratingstable;

INSERT OVERWRITE DIRECTORY '/project/youtube/2/Hive_Final_Output/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
select * from ratingstable sort by rating desc limit 10;

---
-- Question 3 [ Answer from MapReduce program is already sorted. This is just a hive example ] 
---

create table viewcountTable(videoid STRING,viewcount int) row format delimited fields terminated by '\t';

LOAD DATA INPATH '/project/youtube/3/output/part-r-00000' into Table viewcountTable;

INSERT OVERWRITE DIRECTORY '/project/youtube/3/Hive_Final_Output/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
select * from viewcountTable;
