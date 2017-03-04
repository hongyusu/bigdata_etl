
-- use databaseuse test
create database test;
use test;

-- drop table 
drop table if exists ratings;

-- create table
create table ratings (
    userID int,
    movieID int,
    rating int,
    ts int)
    row format delimited
    fields terminated by ','
    stored as textfile;

-- load data from file
load data local inpath './ml-latest-small/ratings.csv' overwrite into table ratings;

