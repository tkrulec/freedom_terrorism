--raw table of all the cleaned gtd data
drop table if exists gtd_raw;
create external table gtd_raw (event_id string, year int, month int, day int, country string, region string, doubt_terrorist int, success int, suicide int, attack_type string, target_type string, nationality string, num_kill int)
row format delimited fields terminated by ','
location '/user/jk5804/impala_gtd/';
select * from gtd_raw order by event_id limit 10;

--master event count per year
drop table if exists gtd_master_count;
create external table gtd_master_count (year int, country string, region string, doubt_terrorist int, success int, suicide int, attack_type string, target_type string, nationality string, num_kill int, event_count int);
insert into gtd_master_count select year, country, region, doubt_terrorist, success, suicide, attack_type, target_type, nationality, num_kill, cast(count(event_id) as int) from gtd_raw group by year, country, region, doubt_terrorist, success, suicide, attack_type, target_type, nationality, num_kill;
--all
select * from gtd_master_count order by year, country, attack_type, target_type limit 10;
-------------------------------------------------------------------------------------
--specific tables
--summary event count per year
drop table if exists gtd_summary_count;
create external table gtd_summary_count(year int, country string, region string, attack_type string, target_type string, nationality string, event_count int, sum_doubt_terrorist int, sum_success int, sum_suicide int, sum_num_kill int);
truncate table gtd_summary_count;
insert into gtd_summary_count select year, country, region, attack_type, target_type, nationality, cast(sum(event_count) as int), cast(sum(doubt_terrorist) as int), cast(sum(success) as int), cast(sum(suicide) as int), cast(sum(num_kill) as int) from gtd_master_count group by year, country, region, attack_type, target_type, nationality order by year, country, attack_type;
--all
select * from gtd_summary_count order by year, country, attack_type, target_type limit 10;

--stats per country
select year, country, region, cast(sum(event_count) as int), cast(sum(sum_doubt_terrorist) as int), cast(sum(sum_success) as int), cast(sum(sum_suicide) as int), cast(sum(sum_num_kill) as int) from gtd_summary_count group by year, country, region order by year, country limit 10;
--create table
drop table if exists gtd_stat_country;
create external table gtd_stat_country(year int, country string, region string, event_count int, sum_doubt_terrorist int, sum_success int, sum_suicide int, sum_num_kill int);
truncate table gtd_stat_country;
insert into gtd_stat_country select year, country, region, cast(sum(event_count) as int), cast(sum(sum_doubt_terrorist) as int), cast(sum(sum_success) as int), cast(sum(sum_suicide) as int), cast(sum(sum_num_kill) as int) from gtd_summary_count group by year, country, region;
--check content
select * from gtd_stat_country order by year, country limit 10;

--stats per region
select year, region, cast(sum(event_count) as int), cast(sum(sum_doubt_terrorist) as int), cast(sum(sum_success) as int), cast(sum(sum_suicide) as int), cast(sum(sum_num_kill) as int) from gtd_summary_count group by year, region order by year, region limit 10;
--create table
drop table if exists gtd_stat_region;
create external table gtd_stat_region(year int, region string, event_count int, sum_doubt_terrorist int, sum_success int, sum_suicide int, sum_num_kill int);
truncate table gtd_stat_region;
insert into gtd_stat_region select year, region, cast(sum(event_count) as int), cast(sum(sum_doubt_terrorist) as int), cast(sum(sum_success) as int), cast(sum(sum_suicide) as int), cast(sum(sum_num_kill) as int) from gtd_summary_count group by year, region;
--check content
select * from gtd_stat_region order by year, region limit 10;

--attack_type per country
select year, country, region, attack_type, cast(sum(event_count) as int), cast(sum(sum_doubt_terrorist) as int),
cast(sum(sum_success) as int), cast(sum(sum_suicide) as int), cast(sum(sum_num_kill) as int) from gtd_summary_count
group by year, country, region, attack_type order by year, country, attack_type limit 10;

--attack_type per region
select year, region, attack_type, cast(sum(event_count) as int), cast(sum(sum_doubt_terrorist) as int),
cast(sum(sum_success) as int), cast(sum(sum_suicide) as int), cast(sum(sum_num_kill) as int) from gtd_summary_count
group by year, region, attack_type order by year, region, attack_type limit 10;

--target_type per country
select year, country, region, target_type, cast(sum(event_count) as int), cast(sum(sum_doubt_terrorist) as int),
cast(sum(sum_success) as int), cast(sum(sum_suicide) as int), cast(sum(sum_num_kill) as int) from gtd_summary_count
group by year, country, region, target_type order by year, country, target_type limit 10;

--target_type per region
select year, region, target_type, cast(sum(event_count) as int), cast(sum(sum_doubt_terrorist) as int),
cast(sum(sum_success) as int), cast(sum(sum_suicide) as int), cast(sum(sum_num_kill) as int) from gtd_summary_count
group by year, region, target_type order by year, region, target_type limit 10;

--nationality per country
select year, country, region, nationality, cast(sum(event_count) as int), cast(sum(sum_doubt_terrorist) as int),
cast(sum(sum_success) as int), cast(sum(sum_suicide) as int), cast(sum(sum_num_kill) as int) from gtd_summary_count
group by year, country, region, nationality order by year, country, nationality limit 10;

--nationality per region
select year, region, nationality, cast(sum(event_count) as int), cast(sum(sum_doubt_terrorist) as int),
cast(sum(sum_success) as int), cast(sum(sum_suicide) as int), cast(sum(sum_num_kill) as int) from gtd_summary_count
group by year, region, nationality order by year, region, nationality limit 10;

-------------------------------------------------------------------------------------
--average
--average summary event in 2008-2017
drop table if exists gtd_avg_summary_count;
create external table gtd_avg_summary_count(country string, region string, attack_type string, target_type string, nationality string, avg_event_count double, avg_sum_doubt_terrorist double, avg_sum_success double, avg_sum_suicide double, avg_sum_num_kill double);
truncate table gtd_avg_summary_count;
insert into gtd_avg_summary_count select country, region, attack_type, target_type, nationality, avg(event_count), avg(sum_doubt_terrorist), avg(sum_success), avg(sum_suicide), avg(sum_num_kill) from gtd_summary_count group by country, region, attack_type, target_type, nationality;
--all
select * from gtd_avg_summary_count order by country, attack_type, target_type limit 10;

--average stats per country
select country, region, cast(avg(avg_event_count) as double), cast(avg(avg_sum_doubt_terrorist) as double), cast(avg(avg_sum_success) as double), cast(avg(avg_sum_suicide) as double), cast(avg(avg_sum_num_kill) as double) from gtd_avg_summary_count group by country, region order by country limit 10;
--create table
drop table if exists gtd_avg_stat_country;
create external table gtd_avg_stat_country(country string, region string, avg_event_count double, avg_sum_doubt_terrorist double, avg_sum_success double, avg_sum_suicide double, avg_sum_num_kill double);
truncate table gtd_avg_stat_country;
insert into gtd_avg_stat_country select country, region, cast(avg(avg_event_count) as double), cast(avg(avg_sum_doubt_terrorist) as double), cast(avg(avg_sum_success) as double), cast(avg(avg_sum_suicide) as double), cast(avg(avg_sum_num_kill) as double) from gtd_avg_summary_count group by country, region;
--check content
select * from gtd_avg_stat_country order by country limit 10;

--average stats per region
select region, cast(avg(avg_event_count) as double), cast(avg(avg_sum_doubt_terrorist) as double), cast(avg(avg_sum_success) as double), cast(avg(avg_sum_suicide) as double), cast(avg(avg_sum_num_kill) as double) from gtd_avg_summary_count group by region order by region limit 10;
--create table
drop table if exists gtd_avg_stat_region;
create external table gtd_avg_stat_region(region string, avg_event_count double, avg_sum_doubt_terrorist double, avg_sum_success double, avg_sum_suicide double, avg_sum_num_kill double);
truncate table gtd_avg_stat_region;
insert into gtd_avg_stat_region select region, cast(avg(avg_event_count) as double), cast(avg(avg_sum_doubt_terrorist) as double), cast(avg(avg_sum_success) as double), cast(avg(avg_sum_suicide) as double), cast(avg(avg_sum_num_kill) as double) from gtd_avg_summary_count group by region;
--check content
select * from gtd_avg_stat_region order by region limit 10;

--average attack_type per country
select country, region, attack_type, cast(avg(avg_event_count) as double), cast(avg(avg_sum_doubt_terrorist) as double),
cast(avg(avg_sum_success) as double), cast(avg(avg_sum_suicide) as double), cast(avg(avg_sum_num_kill) as double) from gtd_avg_summary_count
group by country, region, attack_type order by country, attack_type limit 10;

--average attack_type per region
select region, attack_type, cast(avg(avg_event_count) as double), cast(avg(avg_sum_doubt_terrorist) as double),
cast(avg(avg_sum_success) as double), cast(avg(avg_sum_suicide) as double), cast(avg(avg_sum_num_kill) as double) from gtd_avg_summary_count
group by region, attack_type order by region, attack_type limit 10;

--average target_type per country
select country, region, target_type, cast(avg(avg_event_count) as double), cast(avg(avg_sum_doubt_terrorist) as double),
cast(avg(avg_sum_success) as double), cast(avg(avg_sum_suicide) as double), cast(avg(avg_sum_num_kill) as double) from gtd_avg_summary_count
group by country, region, target_type order by country, target_type limit 10;

--average target_type per region
select region, target_type, cast(avg(avg_event_count) as double), cast(avg(avg_sum_doubt_terrorist) as double),
cast(avg(avg_sum_success) as double), cast(avg(avg_sum_suicide) as double), cast(avg(avg_sum_num_kill) as double) from gtd_avg_summary_count
group by region, target_type order by region, target_type limit 10;

--average nationality per country
select country, region, nationality, cast(avg(avg_event_count) as double), cast(avg(avg_sum_doubt_terrorist) as double),
cast(avg(avg_sum_success) as double), cast(avg(avg_sum_suicide) as double), cast(avg(avg_sum_num_kill) as double) from gtd_avg_summary_count
group by country, region, nationality order by country, nationality limit 10;

--average nationality per region
select region, nationality, cast(avg(avg_event_count) as double), cast(avg(avg_sum_doubt_terrorist) as double),
cast(avg(avg_sum_success) as double), cast(avg(avg_sum_suicide) as double), cast(avg(avg_sum_num_kill) as double) from gtd_avg_summary_count
group by region, nationality order by region, nationality limit 10;

-------------------------------------------------------------------------------------
