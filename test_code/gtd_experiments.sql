--raw table of all the cleaned gtd data
drop table if exists gtd_raw;
create external table gtd_raw (event_id string, year int, month int, day int, country string, region string, doubt_terrorist int, success int, suicide int, attack_type string, target_type string, nationality string, num_kill int)
row format delimited fields terminated by ','
location '/user/jk5804/impala_gtd/';
select * from gtd_raw order by event_id order by year, country, attack_type, target_type limit 10;

--Tina - local
--drop table if exists gtd_raw;
--truncate gtd_raw;
--create external table gtd_raw (event_id string, year int, month int, day int, country string, region string, doubt_terrorist int, success int, suicide int, attack_type string, target_type string, nationality string, num_kill int)
--row format delimited fields terminated by ','
--location '/user/kk3506/impala-gtd/';
--select * from gtd_raw;

--test--1: doubt in terrorist with whether or not terrorist is a national: moderate correlation

--general
--select year, nationality, country, region, case when country = nationality then 0 else 1 end as mismatch, doubt_terrorist from gtd_raw order by year, nationality limit 10;
drop table if exists gtd_doubt_nat;
create external table gtd_doubt_nat (year int, nationality string, country string, region string, mismatch int, doubt_terrorist int);
insert into gtd_doubt_nat select year, nationality, country, region, case when country = nationality then 0 else 1 end as mismatch, doubt_terrorist from gtd_raw;

--sum
--select year, country, region, sum(mismatch), sum(doubt_terrorist) from gtd_doubt_nat group by year, country, region limit 10;
drop table if exists gtd_sum_doubt_nat;
create external table gtd_sum_doubt_nat (year int, country string, region string, sum_mismatch int, sum_doubt_terrorist int);
insert into gtd_sum_doubt_nat select year, country, region, cast(sum(mismatch) as int), cast(sum(doubt_terrorist) as int) from gtd_doubt_nat group by year, country, region;

--Pearson correlation coefficient calculation -->  0.5404245028519427
--source: (https://en.wikipedia.org/wiki/Pearson_correlation_coefficient)
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_mismatch as x, sum_doubt_terrorist as y from gtd_sum_doubt_nat) gtd_sum_doubt_nat;

--average
--select country, region, avg(sum_mismatch), avg(sum_doubt_terrorist) from gtd_sum_doubt_nat group by country, region order by country;
drop table if exists gtd_avg_doubt_nat;
create external table gtd_avg_doubt_nat (country string, region string, avg_mismatch double, avg_doubt_terrorist double);
insert into gtd_avg_doubt_nat select country, region, avg(sum_mismatch), avg(sum_doubt_terrorist) from gtd_sum_doubt_nat group by country, region;

--Pearson correlation coefficient calculation -->  0.6120213583731644
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select avg_mismatch as x, avg_doubt_terrorist as y from gtd_avg_doubt_nat) gtd_avg_doubt_nat;


--test--2: success in terrorist attacks with whether or not terrorist is a national: moderate correlation (~0.443)

--general
--select year, nationality, country, region, case when country = nationality then 0 else 1 end as mismatch, success from gtd_raw order by year, nationality limit 10;
drop table if exists gtd_success_nat_1;
create external table gtd_success_nat_1 (year int, nationality string, country string, region string, mismatch int, success int);
insert into gtd_success_nat_1 select year, nationality, country, region, case when country = nationality then 0 else 1 end as mismatch, success from gtd_raw;

--sum
--select year, country, region, sum(mismatch), sum(success) from gtd_success_nat_1 group by year, country, region limit 10;
drop table if exists gtd_sum_success_nat;
create external table gtd_sum_success_nat (year int, country string, region string, sum_mismatch int, sum_success int);
insert into gtd_sum_success_nat select year, country, region, cast(sum(mismatch) as int), cast(sum(success) as int) from gtd_success_nat_1 group by year, country, region;

--Pearson correlation coefficient calculation -->  0.4431266209339677
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_mismatch as x, sum_success as y from gtd_sum_success_nat) gtd_sum_success_nat;

--average
--select country, region, avg(sum_mismatch), avg(sum_success) from gtd_sum_success_nat group by country, region order by country;
drop table if exists gtd_avg_success_nat;
create external table gtd_avg_success_nat (country string, region string, avg_mismatch double, avg_success double);
insert into gtd_avg_success_nat select country, region, avg(sum_mismatch), avg(sum_success) from gtd_sum_success_nat group by country, region;

--Pearson correlation coefficient calculation -->  0.4431266209339677
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_mismatch as x, sum_success as y from gtd_sum_success_nat) gtd_sum_success_nat;


--test--3: suicide in terrorist with whether or not terrorist is a national: weak/moderate correlation (~0.367)

--general
--select year, nationality, country, region, case when country = nationality then 0 else 1 end as mismatch, suicide from gtd_raw order by year, nationality limit 10;
drop table if exists gtd_suicide_nat_1;
create external table gtd_suicide_nat_1 (year int, nationality string, country string, region string, mismatch int, suicide int);
insert into gtd_suicide_nat_1 select year, nationality, country, region, case when country = nationality then 0 else 1 end as mismatch, suicide from gtd_raw;

--sum
--select year, country, region, sum(mismatch), sum(suicide) from gtd_suicide_nat_1 group by year, country, region limit 10;
drop table if exists gtd_sum_suicide_nat;
create external table gtd_sum_suicide_nat (year int, country string, region string, sum_mismatch int, sum_suicide int);
insert into gtd_sum_suicide_nat select year, country, region, cast(sum(mismatch) as int), cast(sum(suicide) as int) from gtd_suicide_nat_1 group by year, country, region;

--Pearson correlation coefficient calculation -->  0.3669000227788159
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_mismatch as x, sum_suicide as y from gtd_sum_suicide_nat) gtd_sum_suicide_nat;

--average
--select country, region, avg(sum_mismatch), avg(sum_suicide) from gtd_sum_suicide_nat group by country, region order by country;
drop table if exists gtd_avg_suicide_nat;
create external table gtd_avg_suicide_nat (country string, region string, avg_mismatch double, avg_suicide double);
insert into gtd_avg_suicide_nat select country, region, avg(sum_mismatch), avg(sum_suicide) from gtd_sum_suicide_nat group by country, region;

--Pearson correlation coefficient calculation -->  0.4903229634624934
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select avg_mismatch as x, avg_suicide as y from gtd_avg_suicide_nat) gtd_avg_suicide_nat;


--test--4: nkill with whether or not terrorist is a national: weak/moderate correlation (~0.395)

--general
--select year, nationality, country, region, case when country = nationality then 0 else 1 end as mismatch, num_kill from gtd_raw order by year, nationality limit 10;
drop table if exists gtd_num_kill_nat_1;
create external table gtd_num_kill_nat_1 (year int, nationality string, country string, region string, mismatch int, num_kill int);
insert into gtd_num_kill_nat_1 select year, nationality, country, region, case when country = nationality then 0 else 1 end as mismatch, num_kill from gtd_raw;

--sum
--select year, country, region, sum(mismatch), sum(num_kill) from gtd_num_kill_nat_1 group by year, country, region limit 10;
drop table if exists gtd_sum_num_kill_nat;
create external table gtd_sum_num_kill_nat (year int, country string, region string, sum_mismatch int, sum_num_kill int);
insert into gtd_sum_num_kill_nat select year, country, region, cast(sum(mismatch) as int), cast(sum(num_kill) as int) from gtd_num_kill_nat_1 group by year, country, region;

--Pearson correlation coefficient calculation -->  0.3954374389555557
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_mismatch as x, sum_num_kill as y from gtd_sum_num_kill_nat) gtd_sum_num_kill_nat;

--average
--select country, region, avg(sum_mismatch), avg(sum_num_kill) from gtd_sum_num_kill_nat group by country, region order by country;
drop table if exists gtd_avg_num_kill_nat;
create external table gtd_avg_num_kill_nat (country string, region string, avg_mismatch double, avg_num_kill double);
insert into gtd_avg_num_kill_nat select country, region, avg(sum_mismatch), avg(sum_num_kill) from gtd_sum_num_kill_nat group by country, region;

--Pearson correlation coefficient calculation -->  0.5264440611809612
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select avg_mismatch as x, avg_num_kill as y from gtd_avg_num_kill_nat) gtd_avg_num_kill_nat;


--test--5: attack_type with whether or not terrorist is a national

--general
--select year, nationality, country, region, case when country = nationality then 0 else 1 end as mismatch, attack_type from gtd_raw order by year, nationality, attack_type limit 10;
drop table if exists gtd_attack_nat;
create external table gtd_attack_nat (year int, nationality string, country string, region string, mismatch int, attack_type string);
insert into gtd_attack_nat select year, nationality, country, region, case when country = nationality then 0 else 1 end as mismatch, attack_type from gtd_raw;

--sum
--select year, country, region, sum(mismatch), attack_type from gtd_attack_nat group by year, country, region, attack_type limit 10;
drop table if exists gtd_sum_attack_nat;
create external table gtd_sum_attack_nat (year int, country string, region string, sum_mismatch int, attack_type string);
insert into gtd_sum_attack_nat select year, country, region, cast(sum(mismatch) as int), attack_type from gtd_attack_nat group by year, country, region, attack_type;

--test--6: target_type with whether or not terrorist is a national

--general
--select year, nationality, country, region, case when country = nationality then 0 else 1 end as mismatch, target_type from gtd_raw order by year, nationality, target_type limit 10;
drop table if exists gtd_target_nat;
create external table gtd_target_nat (year int, nationality string, country string, region string, mismatch int, target_type string);
insert into gtd_target_nat select year, nationality, country, region, case when country = nationality then 0 else 1 end as mismatch, target_type from gtd_raw;

--sum
--select year, country, region, sum(mismatch), target_type from gtd_target_nat group by year, country, region, target_type limit 10;
drop table if exists gtd_sum_target_nat;
create external table gtd_sum_target_nat (year int, country string, region string, sum_mismatch int, target_type string);
insert into gtd_sum_target_nat select year, country, region, cast(sum(mismatch) as int), target_type from gtd_target_nat group by year, country, region, target_type;


--test--7: nkill with attack_type

--general
--select year, country, region, sum(num_kill), attack_type from gtd_raw group by year, country, region, attack_type order by year, country, attack_type limit 10;
drop table if exists gtd_attack_nkill;
create external table gtd_attack_nkill (year int, country string, region string, sum_num_kill int, attack_type string);
insert into gtd_attack_nkill select year, country, region, cast(sum(num_kill) as int), attack_type from gtd_raw group by year, country, region, attack_type;
select * from gtd_attack_nkill order by year, country, attack_type limit 10;


--test--8: nkill with target_type

--general
--select year, country, region, sum(num_kill), target_type from gtd_raw group by year, country, region, target_type order by year, target_type limit 10;
drop table if exists gtd_target_nkill_1;
create external table gtd_target_nkill_1 (year int, country string, region string, sum_num_kill int, target_type string);
insert into gtd_target_nkill_1 select year, country, region, cast(sum(num_kill) as int), target_type from gtd_raw group by year, country, region, target_type;
select * from gtd_target_nkill_1 order by year, target_type limit 10;


--test--9: nkill with terrorist was successful: high correlation

--general
--select year, country, region, sum(num_kill), sum(success) from gtd_raw group by year, country, region limit 10;
drop table if exists gtd_num_kill_success;
create external table gtd_num_kill_success (year int, country string, region string, sum_num_kill int, sum_success int);
insert into gtd_num_kill_success select year, country, region, cast(sum(num_kill) as int), cast(sum(success) as int) from gtd_raw group by year, country, region;
select * from gtd_num_kill_success order by year, country;

--Pearson correlation coefficient calculation -->  0.8861599669231645
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_num_kill as x, sum_success as y from gtd_num_kill_success) gtd_num_kill_success;

--average
--select country, region, avg(sum_num_kill), avg(sum_success) from gtd_num_kill_success group by country, region order by country;
drop table if exists gtd_avg_num_kill_success;
create external table gtd_avg_num_kill_success (country string, region string, avg_sum_num_kill double, avg_sum_success double);
insert into gtd_avg_num_kill_success select country, region, avg(sum_num_kill), avg(sum_success) from gtd_num_kill_success group by country, region;
select * from gtd_avg_num_kill_success order by country;

--Pearson correlation coefficient calculation --> 0.9319519524323717
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select avg_sum_num_kill as x, avg_sum_success as y from gtd_avg_num_kill_success) gtd_avg_num_kill_success;


--test--10: nkill with suicide in terrorist: high correlation

--general
--select year, country, region, sum(num_kill), sum(suicide) from gtd_raw group by year, country, region limit 10;
drop table if exists gtd_num_kill_suicide;
create external table gtd_num_kill_suicide (year int, country string, region string, sum_num_kill int, sum_suicide int);
insert into gtd_num_kill_suicide select year, country, region, cast(sum(num_kill) as int), cast(sum(suicide) as int) from gtd_raw group by year, country, region;
select * from gtd_num_kill_suicide order by year, country;

--Pearson correlation coefficient calculation -->  0.9185796246253335
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_num_kill as x, sum_suicide as y from gtd_num_kill_suicide) gtd_num_kill_suicide;

--average
--select country, region, avg(sum_num_kill), avg(sum_suicide) from gtd_num_kill_suicide group by country, region order by country;
drop table if exists gtd_avg_num_kill_suicide;
create external table gtd_avg_num_kill_suicide (country string, region string, avg_sum_num_kill double, avg_sum_suicide double);
insert into gtd_avg_num_kill_suicide select country, region, avg(sum_num_kill), avg(sum_suicide) from gtd_num_kill_suicide group by country, region;
select * from gtd_avg_num_kill_suicide order by country;

--Pearson correlation coefficient calculation --> 0.9807898386596263
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select avg_sum_num_kill as x, avg_sum_suicide as y from gtd_avg_num_kill_suicide) gtd_avg_num_kill_suicide;


--test--11: success in terrorist with suicide in terrorist: high correlation

--general
--select year, country, region, sum(success), sum(suicide) from gtd_raw group by year, country, region limit 10;
drop table if exists gtd_success_suicide;
create external table gtd_success_suicide (year int, country string, region string, sum_success int, sum_suicide int);
insert into gtd_success_suicide select year, country, region, cast(sum(success) as int), cast(sum(suicide) as int) from gtd_raw group by year, country, region;
select * from gtd_success_suicide order by year, country;

--Pearson correlation coefficient calculation -->  0.846994275637135
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_success as x, sum_suicide as y from gtd_success_suicide) gtd_success_suicide;

--average
--select country, region, avg(sum_success), avg(sum_suicide) from gtd_success_suicide group by country, region order by country;
drop table if exists gtd_avg_success_suicide;
create external table gtd_avg_success_suicide (country string, region string, avg_sum_success double, avg_sum_suicide double);
insert into gtd_avg_success_suicide select country, region, avg(sum_success), avg(sum_suicide) from gtd_success_suicide group by country, region;
select * from gtd_avg_success_suicide order by country;

--Pearson correlation coefficient calculation --> 0.9152265534328691
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select avg_sum_success as x, avg_sum_suicide as y from gtd_avg_success_suicide) gtd_avg_success_suicide;


--test--12: doubt_terrorist with nkill: high correlation

--general
--select year, country, region, sum(doubt_terrorist), sum(num_kill) from gtd_raw group by year, country, region limit 10;
drop table if exists gtd_doubt_nkill;
create external table gtd_doubt_nkill (year int, country string, region string, sum_doubt_terrorist int, sum_num_kill int);
insert into gtd_doubt_nkill select year, country, region, cast(sum(doubt_terrorist) as int), cast(sum(num_kill) as int) from gtd_raw group by year, country, region;
select * from gtd_doubt_nkill order by year, country;

--Pearson correlation coefficient calculation -->  0.7498245674711961
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_doubt_terrorist as x, sum_num_kill as y from gtd_doubt_nkill) gtd_doubt_nkill;

--average
--select country, region, avg(doubt_terrorist), avg(num_kill) from gtd_doubt_nkill group by country, region order by country;
drop table if exists gtd_avg_doubt_nkill;
create external table gtd_avg_doubt_nkill (country string, region string, avg_sum_doubt_terrorist double, avg_sum_num_kill double);
insert into gtd_avg_doubt_nkill select country, region, avg(sum_doubt_terrorist), avg(sum_num_kill) from gtd_doubt_nkill group by country, region;
select * from gtd_avg_doubt_nkill order by country;

--Pearson correlation coefficient calculation --> 0.8611292800167428
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select avg_sum_doubt_terrorist as x, avg_sum_num_kill as y from gtd_avg_doubt_nkill) gtd_avg_doubt_nkill;


--test--13: doubt_terrorist with attack_type

--general
--select year, country, region, sum(doubt_terrorist), attack_type from gtd_raw group by year, country, region limit 10;
drop table if exists gtd_doubt_attack;
create external table gtd_doubt_attack (year int, country string, region string, sum_doubt_terrorist int, attack_type string);
insert into gtd_doubt_attack select year, country, region, cast(sum(doubt_terrorist) as int), attack_type from gtd_raw group by year, country, region, attack_type;
select * from gtd_doubt_attack order by year, country;

--test--14: doubt_terrorist with target_type

--general
--select year, country, region, sum(doubt_terrorist), target_type from gtd_raw group by year, country, region limit 10;
drop table if exists gtd_doubt_target;
create external table gtd_doubt_target (year int, country string, region string, sum_doubt_terrorist int, target_type string);
insert into gtd_doubt_target select year, country, region, cast(sum(doubt_terrorist) as int), target_type from gtd_raw group by year, country, region, target_type;
select * from gtd_doubt_target order by year, country;


--test--15: doubt_terrorist with success in terrorist attacks: high correlation

--general
--select year, country, region, sum(doubt_terrorist), sum(success) from gtd_raw group by year, country, region limit 10;
drop table if exists gtd_doubt_success;
create external table gtd_doubt_success (year int, country string, region string, sum_doubt_terrorist int, sum_success int);
insert into gtd_doubt_success select year, country, region, cast(sum(doubt_terrorist) as int), cast(sum(success) as int) from gtd_raw group by year, country, region;
select * from gtd_doubt_success order by year, country;

--Pearson correlation coefficient calculation -->  0.8573977520415678
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_doubt_terrorist as x, sum_success as y from gtd_doubt_success) gtd_doubt_success;

--average
--select country, region, avg(doubt_terrorist), avg(success) from gtd_doubt_success group by country, region order by country;
drop table if exists gtd_avg_doubt_success;
create external table gtd_avg_doubt_success (country string, region string, avg_sum_doubt_terrorist double, avg_sum_success double);
insert into gtd_avg_doubt_success select country, region, avg(sum_doubt_terrorist), avg(sum_success) from gtd_doubt_success group by country, region;
select * from gtd_avg_doubt_success order by country;

--Pearson correlation coefficient calculation --> 0.9300325324929846
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select avg_sum_doubt_terrorist as x, avg_sum_success as y from gtd_avg_doubt_success) gtd_avg_doubt_success;


--test--16: doubt_terrorist with suicide in terrorist: high correlation

--general
--select year, country, region, sum(doubt_terrorist), sum(suicide) from gtd_raw group by year, country, region limit 10;
drop table if exists gtd_doubt_suicide;
create external table gtd_doubt_suicide (year int, country string, region string, sum_doubt_terrorist int, sum_suicide int);
insert into gtd_doubt_suicide select year, country, region, cast(sum(doubt_terrorist) as int), cast(sum(suicide) as int) from gtd_raw group by year, country, region;
select * from gtd_doubt_suicide order by year, country;

--Pearson correlation coefficient calculation -->  0.7164236098630858
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_doubt_terrorist as x, sum_suicide as y from gtd_doubt_suicide) gtd_doubt_suicide;

--average
--select country, region, avg(doubt_terrorist), avg(suicide) from gtd_doubt_suicide group by country, region order by country;
drop table if exists gtd_avg_doubt_suicide;
create external table gtd_avg_doubt_suicide (country string, region string, avg_sum_doubt_terrorist double, avg_sum_suicide double);
insert into gtd_avg_doubt_suicide select country, region, avg(sum_doubt_terrorist), avg(sum_suicide) from gtd_doubt_suicide group by country, region;
select * from gtd_avg_doubt_suicide order by country;

--Pearson correlation coefficient calculation --> 0.8259077318920429
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select avg_sum_doubt_terrorist as x, avg_sum_suicide as y from gtd_avg_doubt_suicide) gtd_avg_doubt_suicide;
