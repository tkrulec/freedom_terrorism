-- further on religion --> not significant of a indicator to explore

-- hfi_religion_ts
drop table if exists hfi_religion_ts;
truncate table hfi_religion_ts;
create external table hfi_religion_ts (year int, country string, region string, pf_religion_estop float, pf_religion_harassment float, pf_religion_restrictions float, pf_religion float);
insert into hfi_religion_ts select year, country, region, pf_religion_estop, pf_religion_harassment, pf_religion_restrictions, pf_religion from hfi_master;
select * from hfi_religion_ts limit 10;

drop table if exists agg_religion_focus_ts;
create external table agg_religion_focus_ts (year int, country string, region string, sum_event int, sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, year1 int, country1 string, region1 string, pf_religion_estop float, pf_religion_harassment float, pf_religion_restrictions float, pf_religion float);
truncate table agg_religion_focus_ts;
insert into agg_religion_focus_ts select * from agg_gtd_master INNER join hfi_religion_ts on agg_gtd_master.year=hfi_religion_ts.year and agg_gtd_master.country=hfi_religion_ts.country and agg_gtd_master.region=hfi_religion_ts.region;
select * from agg_religion_focus_ts order by region, country, year limit 10;

--pf_religion and sum_event -0.1723316802507039
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_religion as x, sum_event as y from agg_religion_focus_ts) agg_religion_focus_ts where x!=-1;

--pf_religion_estop and sum_event -0.09530281300557404
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_religion_estop as x, sum_event as y from agg_religion_focus_ts) agg_religion_focus_ts where x!=-1;

--pf_religion_harassment and sum_event -0.05017403932901739
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_religion_harassment as x, sum_event as y from agg_religion_focus_ts) agg_religion_focus_ts where x!=-1;

--pf_religion_restrictions and sum_event  -0.08583140513706998
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_religion_restrictions as x, sum_event as y from agg_religion_focus_ts) agg_religion_focus_ts where x!=-1;

--pf_religion and sum_num_kill -0.1575603648289945
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_religion as x, sum_num_kill as y from agg_religion_focus_ts) agg_religion_focus_ts where x!=-1;

--pf_religion_estop and sum_num_kill -0.04844957909590308
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_religion_estop as x, sum_num_kill as y from agg_religion_focus_ts) agg_religion_focus_ts where x!=-1;

--pf_religion_harassment and sum_num_kill -0.09136268330456034
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_religion_harassment as x, sum_num_kill as y from agg_religion_focus_ts) agg_religion_focus_ts where x!=-1;

--pf_religion_restrictions and sum_num_kill -0.04195426730661301
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_religion_restrictions as x, sum_num_kill as y from agg_religion_focus_ts) agg_religion_focus_ts where x!=-1;
