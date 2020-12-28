--all cols for HFI
--year int,country string, region string, hf_score float, pf_rol float,pf_ss_disappearances_disap float,pf_ss_disappearances_violent float,pf_ss_disappearances_organized float,pf_ss_disappearances_fatalities float,pf_ss_disappearances_injuries float,pf_ss_disappearances float,pf_ss_women_fgm float,pf_ss_women_inheritance_widows float,pf_ss_women_inheritance_daughters float,pf_ss_women_inheritance float,pf_ss_women float,pf_ss float,pf_movement_domestic float,pf_movement_foreign float,pf_movement float,pf_religion_estop_establish float,pf_religion_estop_operate float,pf_religion_estop float,pf_religion_harassment float,pf_religion_restrictions float,pf_religion float,pf_association float,pf_expression_killed float,pf_expression_jailed float,pf_expression_influence float,pf_expression_control float,pf_expression_cable float,pf_expression_newspapers float, pf_expression_internet float,pf_expression float,pf_identity float,pf_score float, ef_trade_tariffs_revenue float, ef_trade_tariffs float,ef_trade_black float,ef_trade_movement_foreign float,ef_trade_movement_capital float,ef_trade_movement_visit float,ef_trade_movement float,ef_trade float, ef_regulation_labor float, ef_regulation_business float,ef_regulation float,ef_score float)

drop table if exists hfi_raw;
create external table hfi_raw (year int,country string, region string, hf_score float, pf_rol float,pf_ss_disappearances_disap float,pf_ss_disappearances_violent float,pf_ss_disappearances_organized float,pf_ss_disappearances_fatalities float,pf_ss_disappearances_injuries float,pf_ss_disappearances float,pf_ss_women_fgm float,pf_ss_women_inheritance_widows float,pf_ss_women_inheritance_daughters float,pf_ss_women_inheritance float,pf_ss_women float,pf_ss float,pf_movement_domestic float,pf_movement_foreign float,pf_movement float,pf_religion_estop_establish float,pf_religion_estop_operate float,pf_religion_estop float,pf_religion_harassment float,pf_religion_restrictions float,pf_religion float,pf_association float,pf_expression_killed float,pf_expression_jailed float,pf_expression_influence float,pf_expression_control float,pf_expression_cable float,pf_expression_newspapers float, pf_expression_internet float,pf_expression float,pf_identity float,pf_score float, ef_trade_tariffs_revenue float, ef_trade_tariffs float,ef_trade_black float,ef_trade_movement_foreign float,ef_trade_movement_capital float,ef_trade_movement_visit float,ef_trade_movement float,ef_trade float, ef_regulation_labor float, ef_regulation_business float,ef_regulation float,ef_score float)
row format delimited fields terminated by ';'
location '/user/kk3506/project/impala-hfi/';
-- location '/user/jk5804/impala_hfi/';


--getting averages
select avg(hf_score),year from hfi_raw where hf_score!=-1 group by year order by avg(hf_score) desc limit 5;
select avg(pf_score),year from hfi_raw where pf_score!=-1 group by year order by avg(pf_score) desc limit 5;
select avg(hf_score),region, year from hfi_raw where hf_score!=-1 group by region, year order by avg(hf_score) desc limit 5;
select avg(pf_score),avg(ef_score),region, year from hfi_raw where hf_score!=-1 group by region, year order by avg(ef_score) desc limit 5;
select avg(pf_score),avg(ef_score),region, year from hfi_raw where hf_score!=-1 group by region, year order by avg(pf_score)  desc limit 5;

--create hfi_main
drop table if exists hfi_main;
create external table hfi_main (year int, country string, region string, hf_score float, pf_score float, ef_score float, pf_rol float, pf_ss float, pf_movement float, pf_expression float, pf_religion float, pf_identity float);
truncate table hfi_main;
insert into hfi_main select year, country, region, hf_score, pf_score, ef_score, pf_rol, pf_ss, pf_movement, pf_expression, pf_religion, pf_identity from hfi_raw;

select * from hfi_main limit 2;

--create average by year of hf, pf, ef by region
drop table if exists hfi_region;
create external table hfi_region (region string, hf_avg_score double, pf_avg_score double, ef_avg_score double, year int);
truncate table hfi_region;
insert into hfi_region select region, avg(hf_score), avg(pf_score), avg(ef_score), year from hfi_raw where hf_score!=-1 and pf_score!=-1 and ef_score!=-1 group by region, year;

select * from hfi_region order by year, hf_avg_score desc limit 5;

--create for women
drop table if exists hfi_women;
create external table hfi_women (year int, country string, region string,pf_ss_women_fgm float,pf_ss_women_inheritance_widows float, pf_ss_women_inheritance_daughters float, pf_ss_women_inheritance float, pf_ss_women float,pf_ss float);
truncate table hfi_women;
insert into hfi_women select year,country, region,pf_ss_women_fgm,pf_ss_women_inheritance_widows, pf_ss_women_inheritance_daughters, pf_ss_women_inheritance, pf_ss_women,pf_ss from hfi_raw;

select * from hfi_women limit 2;

--create for religion
drop table if exists hfi_religion;
create external table hfi_religion (year int, country string, region string, pf_religion_estop_establish float, pf_religion_estop_operate float, pf_religion_estop float, pf_religion_harassment float, pf_religion_restrictions float, pf_religion float);
truncate table hfi_religion;
insert into hfi_religion select year, country, region, pf_religion_estop_establish, pf_religion_estop_operate, pf_religion_estop, pf_religion_harassment, pf_religion_restrictions, pf_religion from hfi_raw;

select * from hfi_religion limit 2;

--create for freedom of media
drop table if exists hfi_expression;
create external table hfi_expression (year int, country string, region string, pf_expression_killed float, pf_expression_jailed float, pf_expression_influence float, pf_expression_control float, pf_expression_cable float, pf_expression_newspapers float, pf_expression_internet float, pf_expression float);
truncate table hfi_expression;
insert into hfi_expression select year, country, region, pf_expression_killed, pf_expression_jailed, pf_expression_influence, pf_expression_control, pf_expression_cable, pf_expression_newspapers, pf_expression_internet, pf_expression from hfi_raw;

select * from hfi_expression limit 2;

-- create for security and safety pf_ss
drop table if exists hfi_ss;
create external table hfi_ss(year int, country string, region string,pf_ss_disappearances_disap float,pf_ss_disappearances_violent float,pf_ss_disappearances_organized float,pf_ss_disappearances_fatalities float,pf_ss_disappearances_injuries float,pf_ss_disappearances float,pf_ss_women_inheritance float,pf_ss_women float,pf_ss float);
truncate table hfi_ss;
insert into hfi_ss select year,country,region,pf_ss_disappearances_disap,pf_ss_disappearances_violent,pf_ss_disappearances_organized,pf_ss_disappearances_fatalities,pf_ss_disappearances_injuries,pf_ss_disappearances,pf_ss_women_inheritance,pf_ss_women,pf_ss from hfi_raw;

select * from hfi_ss limit 2;

--create for movement
drop table if exists hfi_movement;
create external table hfi_movement (year int, country string, region string, pf_movement_domestic float,pf_movement_foreign float,pf_movement float);
truncate table hfi_movement;
insert into hfi_movement select year, country, region, pf_movement_domestic,pf_movement_foreign,pf_movement from hfi_raw;

select * from hfi_movement limit 2;