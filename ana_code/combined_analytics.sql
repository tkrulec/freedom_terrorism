--all columns for HFI:
--year,country, region, hf_score, pf_rol,pf_ss_disappearances_disap,pf_ss_disappearances_violent,pf_ss_disappearances_organized,pf_ss_disappearances_fatalities,pf_ss_disappearances_injuries,pf_ss_disappearances,pf_ss_women_fgm,pf_ss_women_inheritance_widows,pf_ss_women_inheritance_daughters,pf_ss_women_inheritance,pf_ss_women,pf_ss,pf_movement_domestic,pf_movement_foreign,pf_movement,pf_religion_estop_establish,pf_religion_estop_operate,pf_religion_estop,pf_religion_harassment,pf_religion_restrictions,pf_religion,pf_association,pf_expression_killed,pf_expression_jailed,pf_expression_influence,pf_expression_control,pf_expression_cable,pf_expression_newspapers, pf_expression_internet,pf_expression,pf_identity,pf_score, ef_trade_tariffs_revenue, ef_trade_tariffs,ef_trade_black,ef_trade_movement_foreign,ef_trade_movement_capital,ef_trade_movement_visit,ef_trade_movement,ef_trade, ef_regulation_labor, ef_regulation_business,ef_regulation,ef_score
--all columns with data types:
--year int,country string, region string, hf_score float, pf_rol float,pf_ss_disappearances_disap float,pf_ss_disappearances_violent float,pf_ss_disappearances_organized float,pf_ss_disappearances_fatalities float,pf_ss_disappearances_injuries float,pf_ss_disappearances float,pf_ss_women_fgm float,pf_ss_women_inheritance_widows float,pf_ss_women_inheritance_daughters float,pf_ss_women_inheritance float,pf_ss_women float,pf_ss float,pf_movement_domestic float,pf_movement_foreign float,pf_movement float,pf_religion_estop_establish float,pf_religion_estop_operate float,pf_religion_estop float,pf_religion_harassment float,pf_religion_restrictions float,pf_religion float,pf_association float,pf_expression_killed float,pf_expression_jailed float,pf_expression_influence float,pf_expression_control float,pf_expression_cable float,pf_expression_newspapers float, pf_expression_internet float,pf_expression float,pf_identity float,pf_score float, ef_trade_tariffs_revenue float, ef_trade_tariffs float,ef_trade_black float,ef_trade_movement_foreign float,ef_trade_movement_capital float,ef_trade_movement_visit float,ef_trade_movement float,ef_trade float, ef_regulation_labor float, ef_regulation_business float,ef_regulation float,ef_score float)
drop table if exists hfi_raw;
create external table hfi_raw (year int,country string, region string, hf_score float, pf_rol float,pf_ss_disappearances_disap float,pf_ss_disappearances_violent float,pf_ss_disappearances_organized float,pf_ss_disappearances_fatalities float,pf_ss_disappearances_injuries float,pf_ss_disappearances float,pf_ss_women_fgm float,pf_ss_women_inheritance_widows float,pf_ss_women_inheritance_daughters float,pf_ss_women_inheritance float,pf_ss_women float,pf_ss float,pf_movement_domestic float,pf_movement_foreign float,pf_movement float,pf_religion_estop_establish float,pf_religion_estop_operate float,pf_religion_estop float,pf_religion_harassment float,pf_religion_restrictions float,pf_religion float,pf_association float,pf_expression_killed float,pf_expression_jailed float,pf_expression_influence float,pf_expression_control float,pf_expression_cable float,pf_expression_newspapers float, pf_expression_internet float,pf_expression float,pf_identity float,pf_score float, ef_trade_tariffs_revenue float, ef_trade_tariffs float,ef_trade_black float,ef_trade_movement_foreign float,ef_trade_movement_capital float,ef_trade_movement_visit float,ef_trade_movement float,ef_trade float, ef_regulation_labor float, ef_regulation_business float,ef_regulation float,ef_score float)
row format delimited fields terminated by ';'
location '/user/kk3506/project/impala-hfi/';
-- location '/user/jk5804/impala_hfi/';

drop table if exists gtd_raw;
create external table gtd_raw (event_id string, year int, month int, day int, country string, region string, doubt_terrorist int, success int, suicide int, attack_type string, target_type string, nationality string, num_kill int)
row format delimited fields terminated by ','
location '/user/kk3506/project/impala_gtd/';
--location '/user/jk5804/impala_gtd/';

--aggregate events by country.
select count(event_id), year, country, region from gtd_raw group by country, year, region order by country, year;
select sum(doubt_terrorist), year, country from gtd_raw group by year, country order by count(doubt_terrorist) desc;
select sum(success), year, country from gtd_raw group by year, country order by sum(success) desc;
select sum(suicide), year, country from gtd_raw group by year, country order by sum(suicide) desc;

--countries that don't match
--distinct countries
drop table if exists gtd_distinct_country;
create external table gtd_distinct_country (gtd_country string);
truncate table gtd_distinct_country;
insert into gtd_distinct_country select distinct(country) from gtd_raw;

drop table if exists hfi_distinct_country;
create external table hfi_distinct_country (hfi_country string);
truncate table hfi_distinct_country;
insert into hfi_distinct_country select distinct(country) from hfi_raw;

--returns the match between the two + whats in the first and not in the second
select * from gtd_distinct_country GTD left join hfi_distinct_country HFI on GTD.gtd_country=HFI.hfi_country;
select * from hfi_distinct_country HFI left join gtd_distinct_country GTD on HFI.hfi_country=GTD.gtd_country;

--just showing those that don't match.
drop table if exists distinct_country;
create external table distinct_country (country string);
truncate table distinct_country;
insert into distinct_country select gtd_country from(select distinct gtd_country from gtd_distinct_country union ALL select distinct hfi_country from hfi_distinct_country) as tbl group by gtd_country having count(*) =1;

--see what's in hfi
select * from distinct_country DD left join hfi_distinct_country HFI on DD.country=HFI.hfi_country;

--see what's in gtd
select * from distinct_country DD left join gtd_distinct_country GTD on DD.country=GTD.gtd_country;

--manually update HFI
drop table if exists hfi_master1;
create external table hfi_master1 (year int,country string, region string, hf_score float, pf_rol float,pf_ss_disappearances_disap float,pf_ss_disappearances_violent float,pf_ss_disappearances_organized float,pf_ss_disappearances_fatalities float,pf_ss_disappearances_injuries float,pf_ss_disappearances float,pf_ss_women_fgm float,pf_ss_women_inheritance_widows float,pf_ss_women_inheritance_daughters float,pf_ss_women_inheritance float,pf_ss_women float,pf_ss float,pf_movement_domestic float,pf_movement_foreign float,pf_movement float,pf_religion_estop_establish float,pf_religion_estop_operate float,pf_religion_estop float,pf_religion_harassment float,pf_religion_restrictions float,pf_religion float,pf_association float,pf_expression_killed float,pf_expression_jailed float,pf_expression_influence float,pf_expression_control float,pf_expression_cable float,pf_expression_newspapers float, pf_expression_internet float,pf_expression float,pf_identity float,pf_score float, ef_trade_tariffs_revenue float, ef_trade_tariffs float,ef_trade_black float,ef_trade_movement_foreign float,ef_trade_movement_capital float,ef_trade_movement_visit float,ef_trade_movement float,ef_trade float, ef_regulation_labor float, ef_regulation_business float,ef_regulation float,ef_score float);
truncate table hfi_master1;
insert into hfi_master1 (year,country, region, hf_score, pf_rol,pf_ss_disappearances_disap,pf_ss_disappearances_violent,pf_ss_disappearances_organized,pf_ss_disappearances_fatalities,pf_ss_disappearances_injuries,pf_ss_disappearances,pf_ss_women_fgm,pf_ss_women_inheritance_widows,pf_ss_women_inheritance_daughters,pf_ss_women_inheritance,pf_ss_women,pf_ss,pf_movement_domestic,pf_movement_foreign,pf_movement,pf_religion_estop_establish,pf_religion_estop_operate,pf_religion_estop,pf_religion_harassment,pf_religion_restrictions,pf_religion,pf_association,pf_expression_killed,pf_expression_jailed,pf_expression_influence,pf_expression_control,pf_expression_cable,pf_expression_newspapers, pf_expression_internet,pf_expression,pf_identity,pf_score, ef_trade_tariffs_revenue, ef_trade_tariffs,ef_trade_black,ef_trade_movement_foreign,ef_trade_movement_capital,ef_trade_movement_visit,ef_trade_movement,ef_trade, ef_regulation_labor, ef_regulation_business,ef_regulation,ef_score)
select year, (case
	when country='Yemen, Rep.' Then 'Yemen'
	when country='Slovak Rep.' Then 'Slovak Republic'
	when country='Pap. New Guinea' Then 'Papua New Guinea'
	when country='Kyrgyz Republic' Then 'Kyrgyzstan'
	when country='Timor-Leste' Then 'East Timor'
	when country='Korea, South' Then 'South Korea'
	when country='Bosnia and Herzegovina' Then 'Bosnia-Herzegovina'
	when country='Dominican Rep.' Then 'Dominican Republic'
	when country='Central Afr. Rep.' Then 'Central African Republic'
	when country='Czech Rep.' Then 'Czech Republic'
	when country='Congo, Dem. R.' Then 'Democratic Republic of the Congo'
	when country='Congo, Rep. Of' Then 'Republic of the Congo' else country end) as country, (case
	when country= 'Greece' Then 'Western Europe'
	when country='Cyprus' Then 'Western Europe' else region end) as region, hf_score, pf_rol,pf_ss_disappearances_disap,pf_ss_disappearances_violent,pf_ss_disappearances_organized,pf_ss_disappearances_fatalities,pf_ss_disappearances_injuries,pf_ss_disappearances,pf_ss_women_fgm,pf_ss_women_inheritance_widows,pf_ss_women_inheritance_daughters,pf_ss_women_inheritance,pf_ss_women,pf_ss,pf_movement_domestic,pf_movement_foreign,pf_movement,pf_religion_estop_establish,pf_religion_estop_operate,pf_religion_estop,pf_religion_harassment,pf_religion_restrictions,pf_religion,pf_association,pf_expression_killed,pf_expression_jailed,pf_expression_influence,pf_expression_control,pf_expression_cable,pf_expression_newspapers, pf_expression_internet,pf_expression,pf_identity,pf_score, ef_trade_tariffs_revenue, ef_trade_tariffs,ef_trade_black,ef_trade_movement_foreign,ef_trade_movement_capital,ef_trade_movement_visit,ef_trade_movement,ef_trade, ef_regulation_labor, ef_regulation_business,ef_regulation,ef_score from hfi_raw;

--manually update GTD
drop table if exists gtd_master1;
create external table gtd_master1 (event_id string, year int, month int, day int, country string, region string, doubt_terrorist int, success int, suicide int, attack_type string, target_type string, nationality string, num_kill int);
truncate table gtd_master1;
insert into gtd_master1 (event_id, year, month, day, country, region, doubt_terrorist, success, suicide, attack_type, target_type, nationality, num_kill)
select event_id, year, month, day, country, (case when region='Southeast Asia' Then 'South Asia'
		when region ='Australasia & Oceania' Then 'Oceania'
		when region = 'Central America & Caribbean' Then 'Latin America & the Caribbean'
		when region='South America' Then 'Latin America & the Caribbean'
		when region='Central Asia' Then 'Caucasus & Central Asia'
		when country ='Mexico' Then 'Latin America & the Caribbean' else region end) as region, doubt_terrorist, success, suicide, attack_type, target_type, nationality, num_kill from gtd_raw;


--remove anything that's not in the other table;
drop table if exists cy_intersection_gtd;
create table cy_intersection_gtd (year int, country string, region string);
truncate table cy_intersection_gtd;
insert into cy_intersection_gtd select distinct year, country, region from gtd_master1;

drop table if exists cy_intersection_hfi;
create table cy_intersection_hfi (year int, country string, region string);
truncate table cy_intersection_hfi;
insert into cy_intersection_hfi select distinct year, country, region from hfi_master1;

--matching
select * from cy_intersection_gtd left join cy_intersection_hfi on cy_intersection_gtd.year=cy_intersection_hfi.year and cy_intersection_gtd.country=cy_intersection_hfi.country and cy_intersection_gtd.region=cy_intersection_hfi.region;
select * from cy_intersection_hfi left join cy_intersection_gtd on cy_intersection_gtd.year=cy_intersection_hfi.year and cy_intersection_gtd.country=cy_intersection_hfi.country and cy_intersection_gtd.region=cy_intersection_hfi.region order by cy_intersection_hfi.region, cy_intersection_hfi.country;

--create the main one
drop table if exists cy_intersection;
create table cy_intersection (year int, country string, region string, year2 int, country2 string, region2 string);
truncate table cy_intersection;
insert into cy_intersection select * from cy_intersection_gtd inner join cy_intersection_hfi on cy_intersection_gtd.year=cy_intersection_hfi.year and cy_intersection_gtd.country=cy_intersection_hfi.country and cy_intersection_gtd.region=cy_intersection_hfi.region;

--what doesn't macth
drop table if exists distinct_match;
create external table distinct_match (year int, country string, region string);
truncate table distinct_match;
insert into distinct_match select * from cy_intersection_hfi UNION ALL select * from cy_intersection_gtd;

select year, country, region, count(*) as cnt from distinct_match group by country, year, region having count(*)=1;

--check notes on what doesn't match - has been corrected above
select distinct country, region from hfi_master1 where country in ('Greece', 'Cyprus', 'Mexico');
select distinct country, region from gtd_master1 where country in ('Greece', 'Cyprus', 'Mexico');

--make gtd_master
drop table if exists gtd_master;
create external table gtd_master (event_id string, year int, month int, day int, country string, region string, doubt_terrorist int, success int, suicide int, attack_type string, target_type string, nationality string, num_kill int);
truncate table gtd_master;
insert into gtd_master select * from gtd_master1 where EXISTS (select year, country FROM cy_intersection WHERE gtd_master1.year=cy_intersection.year and gtd_master1.country=cy_intersection.country);

--gtd check for duplicated - no duplicates
Select event_id, count(*) as cnt from gtd_master group by event_id having count (*)>1;

--make hfi master
drop table if exists hfi_master;
create external table hfi_master(year int,country string, region string, hf_score float, pf_rol float,pf_ss_disappearances_disap float,pf_ss_disappearances_violent float,pf_ss_disappearances_organized float,pf_ss_disappearances_fatalities float,pf_ss_disappearances_injuries float,pf_ss_disappearances float,pf_ss_women_fgm float,pf_ss_women_inheritance_widows float,pf_ss_women_inheritance_daughters float,pf_ss_women_inheritance float,pf_ss_women float,pf_ss float,pf_movement_domestic float,pf_movement_foreign float,pf_movement float,pf_religion_estop_establish float,pf_religion_estop_operate float,pf_religion_estop float,pf_religion_harassment float,pf_religion_restrictions float,pf_religion float,pf_association float,pf_expression_killed float,pf_expression_jailed float,pf_expression_influence float,pf_expression_control float,pf_expression_cable float,pf_expression_newspapers float, pf_expression_internet float,pf_expression float,pf_identity float,pf_score float, ef_trade_tariffs_revenue float, ef_trade_tariffs float,ef_trade_black float,ef_trade_movement_foreign float,ef_trade_movement_capital float,ef_trade_movement_visit float,ef_trade_movement float,ef_trade float, ef_regulation_labor float, ef_regulation_business float,ef_regulation float,ef_score float);
truncate table hfi_master;
insert into hfi_master select * from hfi_master1 where EXISTS (select year, country FROM cy_intersection WHERE hfi_master1.year=cy_intersection.year and hfi_master1.country=cy_intersection.country);

--hfi check for duplicates - no duplicates
Select year, country, count(*) as cnt from hfi_master group by country, year having count (*)>1;

----------------------------------------experiments
----------------------------------------------------------------------
--------------------Correlation with aggregate results
--create a table gtd with aggreagte number of events, agg numb of succes, suicide and doubt_terr on years, add NULL for nonexistent data an 0 where 0 for country/year
drop table if exists agg_gtd_master;
create external table agg_gtd_master (year int, country string, region string, sum_event int, sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int);
truncate table agg_gtd_master;
insert into agg_gtd_master select year, country, region, cast(count(event_id) as int), cast(sum(success)as int), cast(sum(doubt_terrorist) as int), cast(sum(suicide) as int), cast(sum(num_kill) as int) from gtd_master group by year, country, region;
select * from agg_gtd_master order by region, country, year;

--make the agg_master table. jtable with join with values from hfi everything only for years available in gtd.
drop table if exists hfi_main;
create external table hfi_main (year int, country string, region string, hf_score float, pf_score float, ef_score float, pf_rol float, pf_ss float, pf_movement float, pf_expression float, pf_religion float, pf_identity float);
truncate table hfi_main;
insert into hfi_main select year, country, region, hf_score, pf_score, ef_score, pf_rol, pf_ss, pf_movement, pf_expression, pf_religion, pf_identity from hfi_master;

drop table if exists agg_main;
create external table agg_main (year int, country string, region string, sum_event int, sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, year1 int, country1 string, region1 string,hf_score float, pf_score float, ef_score float, pf_rol float, pf_ss float, pf_movement float, pf_expression float, pf_religion float, pf_identity float);
truncate table agg_main;
insert into agg_main select * from agg_gtd_master INNER join hfi_main on agg_gtd_master.year=hfi_main.year and agg_gtd_master.country=hfi_main.country and agg_gtd_master.region=hfi_main.region;

--look at the correlation between hf_score and sum_event, sum_success, sum_doubt, sum_suicide, sum_num_kill
--hf_score and sum_event -0.2586787072293959
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select hf_score as x, sum_event as y from agg_main) agg_main where x!=-1;

--hf_score and sum_success -0.2700105019135249
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select hf_score as x, sum_success as y from agg_main) agg_main where x!=-1;

--hf_score and sum_doubt -0.2773144154206327
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select hf_score as x, sum_doubt as y from agg_main) agg_main where x!=-1;

--hf_score and sum_suicide -0.235640386722502
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select hf_score as x, sum_suicide as y from agg_main) agg_main where x!=-1;

--hf_score and sum_num_kill -0.282328117120186
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select hf_score as x, sum_num_kill as y from agg_main) agg_main where x!=-1;

--sum_event and pf_expression  -0.1509986061122048
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_event as x, pf_expression as y from agg_main) agg_main where y!=-1;

--sum_suicide and pf_expression 0.1764124232823556
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_suicide as x, pf_expression as y from agg_main) agg_main where y!=-1;

--sum_suicide and pf_score -0.2659559717887977
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_suicide as x, pf_score as y from agg_main) agg_main where y!=-1;

--sum_event and pf_religion -0.1511442201058381
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_event as x, pf_religion as y from agg_main) agg_main where y!=-1;

--sum_event and pf_score -0.2982929259636605
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_event as x, pf_score as y from agg_main) agg_main where y!=-1;

--sum_event and ef_score -0.148928954515931
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_event as x, ef_score as y from agg_main) agg_main where y!=-1;

--pf_score and sum_num_kill -0.3177736253190219
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_num_kill as y from agg_main) agg_main where x!=-1;

--ef_score and sum_num_kill -0.1746505412493314
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_num_kill as y from agg_main) agg_main where x!=-1;

----------------------------------------------------------------------
------------------------exploring the significance of the nationality status
drop table if exists gtd_nat;
create external table gtd_nat (year int, nationality string, country string, region string, mismatch int);
truncate table gtd_nat;
insert into gtd_nat select year, nationality, country, region, case when country = nationality then 0 else 1 end as mismatch from gtd_master;
select * from gtd_nat limit 10;

----by country and year
drop table if exists gtd_nat_agg_country;
create external table gtd_nat_agg_country (year int, country string, region string, sum_mismatch int);
truncate table gtd_nat_agg_country;
insert into gtd_nat_agg_country select year, country, region, cast(sum(mismatch) as int) from gtd_nat group by year, country, region;
select * from gtd_nat_agg_country limit 10;

drop table if exists joint_nat_agg_country;
create external table joint_nat_agg_country (year int, country string, region string, sum_mismatch int, year1 int, country1 string, region1 string,hf_score float, pf_score float, ef_score float, pf_rol float, pf_ss float, pf_movement float, pf_expression float, pf_religion float, pf_identity float);
truncate table joint_nat_agg_country;
insert into joint_nat_agg_country select * from gtd_nat_agg_country INNER join hfi_main on gtd_nat_agg_country.year=hfi_main.year and gtd_nat_agg_country.country=hfi_main.country and gtd_nat_agg_country.region=hfi_main.region;
select * from joint_nat_agg_country limit 10;

select year, country, sum_mismatch, hf_score, pf_score, ef_score from joint_nat_agg_country order by sum_mismatch desc limit 20;

--correlation sum_mismatch and hf_score -0.06524553837184541
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_mismatch as x, hf_score as y from joint_nat_agg_country) joint_nat_agg_country where y!=-1;

----by country
drop table if exists gtd_nat_agg_country_c;
create external table gtd_nat_agg_country_c (country string, sum_mismatch int);
truncate table gtd_nat_agg_country_c;
insert into gtd_nat_agg_country_c select country, cast(sum(mismatch) as int) from gtd_nat group by country;
select * from gtd_nat_agg_country_c order by sum_mismatch desc limit 20;

--get the max change in freedom scores per country
drop table if exists hfi_country_diff;
create external table hfi_country_diff (country string, min_hf float, max_hf float, diff float);
truncate table hfi_country_diff;
insert into hfi_country_diff select country, cast(min(hf_score) as float), cast(max(hf_score) as float), cast ((max(hf_score)- min(hf_score)) as float) from hfi_main where hf_score!=-1 group by country;
select * from hfi_country_diff order by diff desc;

drop table if exists joint_nat_agg_country_c;
create external table joint_nat_agg_country_c (country string, sum_mismatch int, country1 string, min_hf float, max_hf float, diff float);
truncate table joint_nat_agg_country_c;
insert into joint_nat_agg_country_c select * from gtd_nat_agg_country_c INNER join hfi_country_diff on gtd_nat_agg_country_c.country=hfi_country_diff.country;

--correlation sum_mismatch and diff 0.1223763223805548
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_mismatch as x, diff as y from joint_nat_agg_country_c) joint_nat_agg_country_c;

--correlation sum_mismatch and min_hf -0.1329204323638297
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_mismatch as x, min_hf as y from joint_nat_agg_country_c) joint_nat_agg_country_c;

--correlation sum_mismatch and max_hf -0.1156633136143024
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select sum_mismatch as x, max_hf as y from joint_nat_agg_country_c) joint_nat_agg_country_c;

----by region
drop table if exists gtd_nat_agg_region;
create external table gtd_nat_agg_region (region string, sum_mismatch int);
truncate table gtd_nat_agg_region;
insert into gtd_nat_agg_region select region, cast(sum(mismatch) as int) from gtd_nat group by region;
select * from gtd_nat_agg_region;

drop table if exists hfi_avg_hf;
create external table hfi_avg_hf (region string, avg_hf float);
insert into hfi_avg_hf select region, cast(avg(hf_score) as float) from hfi_main where hf_score!=-1 group by region;
select * from hfi_avg_hf;

drop table if exists joint_region_avg;
create external table joint_region_avg (region string, sum_mismatch int, region1 string, avg_hf float);
insert into joint_region_avg select * from gtd_nat_agg_region inner join hfi_avg_hf on gtd_nat_agg_region.region=hfi_avg_hf.region;
select * from joint_region_avg order by sum_mismatch;

---------------------------------------------------------------------
------------------------exloring the importance of pf_ss
drop table if exists hfi_ss;
create external table hfi_ss(year int, country string, region string,pf_ss_disappearances_disap float,pf_ss_disappearances_violent float,pf_ss_disappearances_organized float,pf_ss_disappearances_fatalities float,pf_ss_disappearances_injuries float,pf_ss_disappearances float,pf_ss_women_inheritance float,pf_ss_women float,pf_ss float);
truncate table hfi_ss;
insert into hfi_ss select year,country,region,pf_ss_disappearances_disap,pf_ss_disappearances_violent,pf_ss_disappearances_organized,pf_ss_disappearances_fatalities,pf_ss_disappearances_injuries,pf_ss_disappearances,pf_ss_women_inheritance,pf_ss_women,pf_ss from hfi_master;
select * from hfi_ss limit 10;

drop table if exists gtd_agg;
create external table gtd_agg (event_id string, year int, country string, region string, mismatch int, success int, doubt_terrorist int,suicide int, num_kill int);
truncate table gtd_agg;
insert into gtd_agg select event_id, year, country, region, case when country = nationality then 0 else 1 end as mismatch, success, doubt_terrorist, suicide, num_kill from gtd_master;
select * from gtd_agg limit 10;

--very imp table :) per year and country.
drop table if exists agg_gtd_master;
create external table agg_gtd_master (year int, country string, region string, num_events int, sum_mismatch int,sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, perc_suicide float, perc_doubt float, perc_success float, perc_mismatch float, killed_per_event float);
truncate table agg_gtd_master;
insert into agg_gtd_master select year, country, region, cast(count(event_id) as int), cast(sum(mismatch) as int), cast(sum(success) as int), cast(sum(doubt_terrorist) as int), cast(sum(suicide) as int), cast(sum(num_kill) as int), cast(((cast(sum(suicide)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(doubt_terrorist)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(success)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(mismatch)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(num_kill)as int)/(cast(count(event_id) as int)))) as float) from gtd_agg group by year, country, region;

select * from agg_gtd_master order by region, country, year limit 10;

drop table if exists joint_ss;
create external table joint_ss (year int, country string, region string, num_events int, sum_mismatch int,sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, perc_suicide float, perc_doubt float, perc_success float, perc_mismatch float, killed_per_event float, year1 int, country1 string, region1 string,pf_ss_disappearances_disap float,pf_ss_disappearances_violent float,pf_ss_disappearances_organized float,pf_ss_disappearances_fatalities float,pf_ss_disappearances_injuries float,pf_ss_disappearances float,pf_ss_women_inheritance float,pf_ss_women float,pf_ss float);
truncate table joint_ss;
insert into joint_ss select * from agg_gtd_master inner join hfi_ss on agg_gtd_master.year=hfi_ss.year and agg_gtd_master.country=hfi_ss.country;
select * from joint_ss limit 10;

--correlation num events, pf_ss -0.338581993342012
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss as x, num_events as y from joint_ss) joint_ss where x!=-1;

--correlation num events, pf_women -0.186024806371069
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_women as x, num_events as y from joint_ss) joint_ss where x!=-1;

--correlation num events, pf_ss_disappearances -0.5304097936310816
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_disappearances as x, num_events as y from joint_ss) joint_ss where x!=-1;

--correlation perc_mismatch , pf_ss   0.07469564377654513
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss as x, perc_mismatch  as y from joint_ss) joint_ss where x!=-1;

--correlation perc_mismatch , pf_women  -0.01564469274217693
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_women as x, perc_mismatch  as y from joint_ss) joint_ss where x!=-1;

--correlation perc_mismatch , pf_ss_disappearances 0.1177606874453454
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_disappearances as x, perc_mismatch  as y from joint_ss) joint_ss where x!=-1;

--correlation perc_success , pf_ss   -0.1405118571574495
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss as x, perc_success  as y from joint_ss) joint_ss where x!=-1;

--correlation perc_success , pf_women  -0.09795450553861296
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_women as x, perc_success  as y from joint_ss) joint_ss where x!=-1;

--correlation perc_success , pf_ss_disappearances -0.1372973666310264
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_disappearances as x, perc_success  as y from joint_ss) joint_ss where x!=-1;

--correlation killed_per_event , pf_ss  -0.328408578284803
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss as x, killed_per_event  as y from joint_ss) joint_ss where x!=-1;

--correlation killed_per_event , pf_women  -0.253673936549973
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_women as x, killed_per_event  as y from joint_ss) joint_ss where x!=-1;

--correlation killed_per_event , pf_ss_disappearances -0.3576100051162761
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_disappearances as x, killed_per_event  as y from joint_ss) joint_ss where x!=-1;

-- per country
drop table if exists hfi_ss_avg_c;
create external table hfi_ss_avg_c(country string, region string,pf_ss_disappearances_avg float,pf_ss_women_avg float,pf_ss_avg float);
truncate table hfi_ss_avg_c;
insert into hfi_ss_avg_c select country,region,cast(avg(pf_ss_disappearances) as float),cast(avg(pf_ss_women) as float),cast(avg(pf_ss) as float) from hfi_master where pf_ss_disappearances!=-1 and pf_ss_women!=-1 and pf_ss!=-1 group by country, region;
select * from hfi_ss_avg_c limit 10;

drop table if exists agg_gtd_master_c;
create external table agg_gtd_master_c (country string, region string, num_events int, sum_mismatch int,sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, perc_suicide float, perc_doubt float, perc_success float, perc_mismatch float, killed_per_event float);
truncate table agg_gtd_master_c;
insert into agg_gtd_master_c select country, region, cast(count(event_id) as int), cast(sum(mismatch) as int), cast(sum(success) as int), cast(sum(doubt_terrorist) as int), cast(sum(suicide) as int), cast(sum(num_kill) as int), cast(((cast(sum(suicide)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(doubt_terrorist)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(success)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(mismatch)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(num_kill)as int)/(cast(count(event_id) as int)))) as float) from gtd_agg group by country, region;
select * from agg_gtd_master_c limit 10;

drop table if exists joint_ss_avg_c;
create external table joint_ss_avg_c (country string, region string, num_events int, sum_mismatch int,sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, perc_suicide float, perc_doubt float, perc_success float, perc_mismatch float, killed_per_event float, country1 string, region1 string, pf_ss_disappearances_avg_c float, pf_ss_women_avg_c float, pf_ss_avg_c float);
truncate table joint_ss_avg_c;
insert into joint_ss_avg_c select * from agg_gtd_master_c inner join hfi_ss_avg_c on agg_gtd_master_c.country=hfi_ss_avg_c.country;
select * from joint_ss_avg_c limit 10;

--pf_ss_disappearances_avg_c float, pf_ss_women_avg_c float, pf_ss_avg_c float
--correlation num events, pf_ss_avg_c -0.3590094769033141
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_avg_c as x, num_events as y from joint_ss_avg_c) joint_ss_avg_c;

--correlation num events, pf_ss_women_avg_c -0.1947640171550596
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_women_avg_c as x, num_events as y from joint_ss_avg_c) joint_ss_avg_c;

--correlation num events, pf_ss_disappearances_avg_c -0.6231297780225182
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_disappearances_avg_c as x, num_events as y from joint_ss_avg_c) joint_ss_avg_c;


--correlation perc_success, pf_ss_avg_c -0.1361791478804895
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_avg_c as x, perc_success as y from joint_ss_avg_c) joint_ss_avg_c;

--correlation perc_success, pf_ss_women_avg_c -0.1438211027496321
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_women_avg_c as x, perc_success as y from joint_ss_avg_c) joint_ss_avg_c;

--correlation perc_success, pf_ss_disappearances_avg_c -0.1667009545784773
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_disappearances_avg_c as x, perc_success as y from joint_ss_avg_c) joint_ss_avg_c;


--correlation killed_per_event, pf_ss_avg_c -0.3285735507385212
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_avg_c as x, killed_per_event as y from joint_ss_avg_c) joint_ss_avg_c;

--correlation killed_per_event, pf_ss_women_avg_c -0.3390211805059641
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_women_avg_c as x, killed_per_event as y from joint_ss_avg_c) joint_ss_avg_c;

--correlation killed_per_event, pf_ss_disappearances_avg_c -0.3585208416055937
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_disappearances_avg_c as x, killed_per_event as y from joint_ss_avg_c) joint_ss_avg_c;


--correlation perc_mismatch, pf_ss_avg_c 0.1627239385478483
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_avg_c as x, perc_mismatch as y from joint_ss_avg_c) joint_ss_avg_c;

--correlation perc_mismatch, pf_ss_women_avg_c -0.01987992128365883
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_women_avg_c as x, perc_mismatch as y from joint_ss_avg_c) joint_ss_avg_c;

--correlation perc_mismatch, pf_ss_disappearances_avg_c 0.1658906212754286
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_disappearances_avg_c as x, perc_mismatch as y from joint_ss_avg_c) joint_ss_avg_c;


--correlation perc_suicide, pf_ss_avg_c -0.1942671774523673
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_avg_c as x, perc_suicide as y from joint_ss_avg_c) joint_ss_avg_c;

--correlation perc_suicide, pf_ss_women_avg_c -0.3028405691222803
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_women_avg_c as x, perc_suicide as y from joint_ss_avg_c) joint_ss_avg_c;

--correlation perc_suicide, pf_ss_disappearances_avg_c -0.2681057302306353
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_disappearances_avg_c as x, perc_suicide as y from joint_ss_avg_c) joint_ss_avg_c;


-- per region
drop table if exists hfi_ss_avg_r;
create external table hfi_ss_avg_r(region string,pf_ss_disappearances_avg float,pf_ss_women_avg float,pf_ss_avg float);
truncate table hfi_ss_avg_r;
insert into hfi_ss_avg_r select region,cast(avg(pf_ss_disappearances) as float),cast(avg(pf_ss_women) as float),cast(avg(pf_ss) as float) from hfi_master where pf_ss_disappearances!=-1 and pf_ss_women!=-1 and pf_ss!=-1 group by region;
select *  from hfi_ss_avg_r limit 10;

drop table if exists agg_gtd_master_r;
create external table agg_gtd_master_r (region string, num_events int, sum_mismatch int,sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, perc_suicide float, perc_doubt float, perc_success float, perc_mismatch float, killed_per_event float);
truncate table agg_gtd_master_r;
insert into agg_gtd_master_r select region, cast(count(event_id) as int), cast(sum(mismatch) as int), cast(sum(success) as int), cast(sum(doubt_terrorist) as int), cast(sum(suicide) as int), cast(sum(num_kill) as int), cast(((cast(sum(suicide)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(doubt_terrorist)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(success)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(mismatch)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(num_kill)as int)/(cast(count(event_id) as int)))) as float) from gtd_agg group by region;
select *  from agg_gtd_master_r limit 10;

drop table if exists joint_ss_avg_r;
create external table joint_ss_avg_r (region string, num_events int, sum_mismatch int,sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, perc_suicide float, perc_doubt float, perc_success float, perc_mismatch float, killed_per_event float, region1 string,pf_ss_disappearances_avg float, pf_ss_women_avg float,pf_ss_avg float);
truncate table joint_ss_avg_r;
insert into joint_ss_avg_r select * from agg_gtd_master_r inner join hfi_ss_avg_r on agg_gtd_master_r.region=hfi_ss_avg_r.region;
select *  from joint_ss_avg_r limit 10;


--correlation num events, pf_ss -0.5571216488658709
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_avg as x, num_events as y from joint_ss_avg_r) joint_ss_avg_r;

--correlation num events, pf_women -0.7382238949717782
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_women_avg as x, num_events as y from joint_ss_avg_r) joint_ss_avg_r;

--correlation num events, pf_ss_disappearances -0.8246752116524928
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_disappearances_avg as x, num_events as y from joint_ss_avg_r) joint_ss_avg_r;


--correlation perc_success, pf_ss -0.5606969538728604
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_avg as x, perc_success as y from joint_ss_avg_r) joint_ss_avg_r;

--correlation perc_success, pf_women -0.4524852598024012
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_women_avg as x, perc_success as y from joint_ss_avg_r) joint_ss_avg_r;

--correlation perc_success, pf_ss_disappearances -0.4347888476730642
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_disappearances_avg as x, perc_success as y from joint_ss_avg_r) joint_ss_avg_r;


--correlation perc_doubt, pf_ss -0.1089454989341255
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_avg as x, perc_doubt as y from joint_ss_avg_r) joint_ss_avg_r;

--correlation perc_doubt, pf_women -0.04686783073728932
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_women_avg as x, perc_doubt as y from joint_ss_avg_r) joint_ss_avg_r;

--correlation perc_doubt, pf_ss_disappearances -0.1506692054899281
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_disappearances_avg as x, perc_doubt as y from joint_ss_avg_r) joint_ss_avg_r;


--correlation perc_suicide, pf_ss -0.3430214060603894
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_avg as x, perc_suicide as y from joint_ss_avg_r) joint_ss_avg_r;

--correlation perc_suicide, pf_women -0.6370741612245698
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_women_avg as x, perc_suicide as y from joint_ss_avg_r) joint_ss_avg_r;

--correlation perc_suicide, pf_ss_disappearances -0.5984398108385053
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_disappearances_avg as x, perc_suicide as y from joint_ss_avg_r) joint_ss_avg_r;



--correlation perc_mismatch, pf_ss 0.351560083164714
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_avg as x, perc_mismatch as y from joint_ss_avg_r) joint_ss_avg_r;

--correlation perc_mismatch, pf_women  0.2245000556072212
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_women_avg as x, perc_mismatch as y from joint_ss_avg_r) joint_ss_avg_r;

--correlation perc_mismatch, pf_ss_disappearances 0.3172645044309964
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_disappearances_avg as x, perc_mismatch as y from joint_ss_avg_r) joint_ss_avg_r;


--correlation killed_per_event, pf_ss -0.3313542612638815
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_avg as x, killed_per_event as y from joint_ss_avg_r) joint_ss_avg_r;

--correlation killed_per_event, pf_women -0.4851697294116677
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_women_avg as x, killed_per_event as y from joint_ss_avg_r) joint_ss_avg_r;

--correlation killed_per_event, pf_ss_disappearances -0.4342068590638547
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss_disappearances_avg as x, killed_per_event as y from joint_ss_avg_r) joint_ss_avg_r;


------------------------------------------------------------------------
------------------------exploring the importance of pf_expression
--general
drop table if exists hfi_expression;
create external table hfi_expression (year int, country string, region string, pf_expression_killed float, pf_expression_jailed float, pf_expression_influence float, pf_expression_control float, pf_expression_cable float, pf_expression_newspapers float, pf_expression_internet float, pf_expression float);
truncate table hfi_expression;
insert into hfi_expression select year, country, region, pf_expression_killed, pf_expression_jailed, pf_expression_influence, pf_expression_control, pf_expression_cable, pf_expression_newspapers, pf_expression_internet, pf_expression from hfi_master;
select * from hfi_expression limit 10;

drop table if exists joint_expression;
create external table joint_expression (year int, country string, region string, num_events int, sum_mismatch int,sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, perc_suicide float, perc_doubt float, perc_success float, perc_mismatch float, killed_per_event float, year1 int, country1 string, region1 string, pf_expression_killed float, pf_expression_jailed float, pf_expression_influence float, pf_expression_control float, pf_expression_cable float, pf_expression_newspapers float, pf_expression_internet float, pf_expression float);
truncate table joint_expression;
insert into joint_expression select * from agg_gtd_master inner join hfi_expression on agg_gtd_master.year=hfi_expression.year and agg_gtd_master.country=hfi_expression.country;
select * from joint_expression limit 10;

--correlation num events, pf_expression -0.1509986061122048
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression as x, num_events as y from joint_expression) joint_expression where x!=-1;

--correlation num events, pf_expression_killed -0.2970070306152988
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_killed as x, num_events as y from joint_expression) joint_expression where x!=-1;

--correlation num events, pf_expression_influence -0.08817236794131672
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_influence as x, num_events as y from joint_expression) joint_expression where x!=-1;

--country
drop table if exists hfi_expression_c;
create external table hfi_expression_c (country string, region string, pf_expression_killed_c float, pf_expression_jailed_c float, pf_expression_influence_c float, pf_expression_c float);
truncate table hfi_expression_c;
insert into hfi_expression_c select country, region, cast(avg(pf_expression_killed) as float), cast(avg(pf_expression_jailed) as float), cast(avg(pf_expression_influence) as float),cast(avg(pf_expression) as float) from hfi_master where pf_expression_killed!=-1 and pf_expression_jailed!=-1 and pf_expression_influence!=-1 and pf_expression!=-1 group by country, region;
select * from hfi_expression_c limit 10;

drop table if exists agg_gtd_master_c;
create external table agg_gtd_master_c(country string, region string, num_events int, sum_mismatch int,sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, perc_suicide float, perc_doubt float, perc_success float, perc_mismatch float, killed_per_event float);
truncate table agg_gtd_master_c;
insert into agg_gtd_master_c select country, region, cast(count(event_id) as int), cast(sum(mismatch) as int), cast(sum(success) as int), cast(sum(doubt_terrorist) as int), cast(sum(suicide) as int), cast(sum(num_kill) as int), cast(((cast(sum(suicide)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(doubt_terrorist)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(success)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(mismatch)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(num_kill)as int)/(cast(count(event_id) as int)))) as float) from gtd_agg group by country, region;
select * from agg_gtd_master_c limit 10;

drop table if exists joint_expression_c;
create external table joint_expression_c (country string, region string, num_events int, sum_mismatch int,sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, perc_suicide float, perc_doubt float, perc_success float, perc_mismatch float, killed_per_event float, country1 string, region1 string, pf_expression_killed_c float, pf_expression_jailed_c float, pf_expression_influence_c float, pf_expression_c float);
truncate table joint_expression_c;
insert into joint_expression_c select * from agg_gtd_master_c inner join hfi_expression_c on agg_gtd_master_c.country=hfi_expression_c.country and agg_gtd_master_c.region=hfi_expression_c.region;
select * from agg_gtd_master_c limit 10;

--pf_expression_killed_c , pf_expression_jailed_c , pf_expression_influence_c , pf_expression_c
--correlation num-events, pf_expression_c -0.1982909486506064
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_c as x, num_events as y from joint_expression_c) joint_expression_c;

--correlation num events, pf_expression_killed_c -0.5415623898174073
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_killed_c as x, num_events as y from joint_expression_c) joint_expression_c;

--correlation num events, pf_expression_jailed_c -0.004174725116306813
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_jailed_c as x, num_events as y from joint_expression_c) joint_expression_c;

--correlation num events, pf_expression_influence_c -0.1297286630839961
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_influence_c as x, num_events as y from joint_expression_c) joint_expression_c;


--region
drop table if exists hfi_expression_r;
create external table hfi_expression_r (region string, pf_expression_killed_r float, pf_expression_jailed_r float, pf_expression_influence_r float, pf_expression_r float);
truncate table hfi_expression_r;
insert into hfi_expression_r select region, cast(avg(pf_expression_killed) as float), cast(avg(pf_expression_jailed) as float), cast(avg(pf_expression_influence) as float),cast(avg(pf_expression) as float) from hfi_master where pf_expression_killed!=-1 and pf_expression_jailed!=-1 and pf_expression_influence!=-1 and pf_expression!=-1 group by region;

drop table if exists agg_gtd_master_r;
create external table agg_gtd_master_r (region string, num_events int, sum_mismatch int,sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, perc_suicide float, perc_doubt float, perc_success float, perc_mismatch float, killed_per_event float);
truncate table agg_gtd_master_r;
insert into agg_gtd_master_r select region, cast(count(event_id) as int), cast(sum(mismatch) as int), cast(sum(success) as int), cast(sum(doubt_terrorist) as int), cast(sum(suicide) as int), cast(sum(num_kill) as int), cast(((cast(sum(suicide)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(doubt_terrorist)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(success)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(mismatch)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(num_kill)as int)/(cast(count(event_id) as int)))) as float) from gtd_agg group by region;

drop table if exists joint_expression_r;
create external table joint_expression_r (region string, num_events int, sum_mismatch int,sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, perc_suicide float, perc_doubt float, perc_success float, perc_mismatch float, killed_per_event float, region1 string, pf_expression_killed_r float, pf_expression_jailed_r float, pf_expression_influence_r float, pf_expression_r float);
truncate table joint_expression_r;
insert into joint_expression_r select * from agg_gtd_master_r inner join hfi_expression_r on agg_gtd_master_r.region=hfi_expression_r.region;

--correlation num events, pf_expression_r -0.5780298334381314
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_r as x, num_events as y from joint_expression_r) joint_expression_r;

--correlation num events, pf_expression_killed_r -0.7132147494274967
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_killed_r as x, num_events as y from joint_expression_r) joint_expression_r;

--correlation num events, pf_expression_jailed_r -0.5647158851204993
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_jailed_r as x, num_events as y from joint_expression_r) joint_expression_r;

--correlation num events, pf_expression_influence_r -0.5614366153236151
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_influence_r as x, num_events as y from joint_expression_r) joint_expression_r;


--correlation killed_per_event, pf_expression_r -0.4357344556495424
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_r as x, killed_per_event as y from joint_expression_r) joint_expression_r;

--correlation killed_per_event, pf_expression_killed_r 0.04903461233076398
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_killed_r as x, killed_per_event as y from joint_expression_r) joint_expression_r;

--correlation killed_per_event, pf_expression_jailed_r -0.07657598719321891
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_jailed_r as x, killed_per_event as y from joint_expression_r) joint_expression_r;

--correlation killed_per_event, pf_expression_influence_r -0.4650614064513985
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_influence_r as x, killed_per_event as y from joint_expression_r) joint_expression_r;



--correlation perc_success, pf_expression_r -0.396368950004456
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_r as x, perc_success as y from joint_expression_r) joint_expression_r;

--correlation perc_success, pf_expression_killed_r -0.1760614187823221
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_killed_r as x, perc_success as y from joint_expression_r) joint_expression_r;

--correlation perc_success, pf_expression_jailed_r -0.0600740306326475
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_jailed_r as x, perc_success as y from joint_expression_r) joint_expression_r;

--correlation perc_success, pf_expression_influence_r -0.4478814077885677
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_influence_r as x, perc_success as y from joint_expression_r) joint_expression_r;


--correlation perc_mismatch, pf_expression_r 0.367827202080618
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_r as x, perc_mismatch as y from joint_expression_r) joint_expression_r;

--correlation perc_mismatch, pf_expression_killed_r  0.2509434401581724
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_killed_r as x, perc_mismatch as y from joint_expression_r) joint_expression_r;

--correlation perc_mismatch, pf_expression_jailed_r  0.1954762739106924
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_jailed_r as x, perc_mismatch as y from joint_expression_r) joint_expression_r;

--correlation perc_mismatch, pf_expression_influence_r 0.3776996653007669
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression_influence_r as x, perc_mismatch as y from joint_expression_r) joint_expression_r;

------------------------------------------------------------------------
------------------------exploring the importance of pf_movement
--general
drop table if exists hfi_movement;
create external table hfi_movement (year int, country string, region string, pf_movement_domestic float,pf_movement_foreign float,pf_movement float);
truncate table hfi_movement;
insert into hfi_movement select year, country, region, pf_movement_domestic,pf_movement_foreign,pf_movement from hfi_master;

drop table if exists joint_movement;
create external table joint_movement (year int, country string, region string, num_events int, sum_mismatch int,sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, perc_suicide float, perc_doubt float, perc_success float, perc_mismatch float, killed_per_event float, year1 int, country1 string, region1 string, pf_movement_domestic float,pf_movement_foreign float,pf_movement float);
truncate table joint_movement;
insert into joint_movement select * from agg_gtd_master inner join hfi_movement on agg_gtd_master.year=hfi_movement.year and agg_gtd_master.country=hfi_movement.country;

--correlation num events, pf_movement_domestic -0.1098482777894522
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_domestic as x, num_events as y from joint_movement) joint_movement where x!=-1;

--correlation num events, pf_movement_foreign -0.1630611344969534
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_foreign as x, num_events as y from joint_movement) joint_movement where x!=-1;

--correlation num events, pf_movement -0.2151317368247711
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement as x, num_events as y from joint_movement) joint_movement where x!=-1;


--correlation sum_mismatch, pf_movement_domestic -0.02728803104352451
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_domestic as x, sum_mismatch as y from joint_movement) joint_movement where x!=-1;

--correlation sum_mismatch, pf_movement_foreign -0.04726481515088081
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_foreign as x, sum_mismatch as y from joint_movement) joint_movement where x!=-1;

--correlation sum_mismatch, pf_movement -0.07952815396959362
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement as x, sum_mismatch as y from joint_movement) joint_movement where x!=-1;


--correlation perc_mismatch, pf_movement_domestic 0.02382476420211157
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_domestic as x, perc_mismatch as y from joint_movement) joint_movement where x!=-1;

--correlation perc_mismatch, pf_movement_foreign 0.006707978558520928
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_foreign as x, perc_mismatch as y from joint_movement) joint_movement where x!=-1;

--correlation perc_mismatch, pf_movement 0.03420202497001088
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement as x, perc_mismatch as y from joint_movement) joint_movement where x!=-1;


------country
drop table if exists hfi_movement_c;
create external table hfi_movement_c (country string, region string, pf_movement_domestic_avg_c float, pf_movement_foreign_avg_c float, pf_movement_avg_c float);
truncate table hfi_movement_c;
insert into hfi_movement_c select country, region, cast(avg(pf_movement_domestic) as float),cast(avg(pf_movement_foreign) as float),cast(avg(pf_movement) as float) from hfi_master where pf_movement!=-1 and pf_movement_domestic!=-1 and pf_movement_foreign!=-1 group by country, region;

drop table if exists agg_gtd_master_c;
create external table agg_gtd_master_c (country string, region string, num_events int, sum_mismatch int,sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, perc_suicide float, perc_doubt float, perc_success float, perc_mismatch float, killed_per_event float);
truncate table agg_gtd_master_c;
insert into agg_gtd_master_c select country, region, cast(count(event_id) as int), cast(sum(mismatch) as int), cast(sum(success) as int), cast(sum(doubt_terrorist) as int), cast(sum(suicide) as int), cast(sum(num_kill) as int), cast(((cast(sum(suicide)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(doubt_terrorist)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(success)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(mismatch)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(num_kill)as int)/(cast(count(event_id) as int)))) as float) from gtd_agg group by country, region;

drop table if exists joint_movement_c;
create external table joint_movement_c (country string, region string, num_events int, sum_mismatch int,sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, perc_suicide float, perc_doubt float, perc_success float, perc_mismatch float, killed_per_event float, country1 string, region1 string, pf_movement_domestic_avg_c float, pf_movement_foreign_avg_c float, pf_movement_avg_c float);
truncate table joint_movement_c;
insert into joint_movement_c select * from agg_gtd_master_c inner join hfi_movement_c on agg_gtd_master_c.region=hfi_movement_c.region and agg_gtd_master_c.country=hfi_movement_c.country;

--correlation num_events, pf_movement_domestic_avg_c -0.1545667134549102
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_domestic_avg_c as x, num_events as y from joint_movement_c) joint_movement_c;

--correlation num_events, pf_movement_foreign_avg_c  -0.2397534054599129
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_foreign_avg_c as x, num_events as y from joint_movement_c) joint_movement_c;

--correlation num_events, pf_movement_avg_c -0.2642513248113118
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_avg_c as x, num_events as y from joint_movement_c) joint_movement_c;



------region
drop table if exists hfi_movement_r;
create external table hfi_movement_r (region string, pf_movement_domestic_avg_r float, pf_movement_foreign_avg_r float, pf_movement_avg_r float);
truncate table hfi_movement_r;
insert into hfi_movement_r select region, cast(avg(pf_movement_domestic) as float),cast(avg(pf_movement_foreign) as float),cast(avg(pf_movement) as float) from hfi_master where pf_movement!=-1 and pf_movement_domestic!=-1 and pf_movement_foreign!=-1 group by region;

drop table if exists agg_gtd_master_r;
create external table agg_gtd_master_r (region string, num_events int, sum_mismatch int,sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, perc_suicide float, perc_doubt float, perc_success float, perc_mismatch float, killed_per_event float);
truncate table agg_gtd_master_r;
insert into agg_gtd_master_r select region, cast(count(event_id) as int), cast(sum(mismatch) as int), cast(sum(success) as int), cast(sum(doubt_terrorist) as int), cast(sum(suicide) as int), cast(sum(num_kill) as int), cast(((cast(sum(suicide)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(doubt_terrorist)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(success)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(mismatch)as int)/(cast(count(event_id) as int)))) as float), cast(((cast(sum(num_kill)as int)/(cast(count(event_id) as int)))) as float) from gtd_agg group by region;

drop table if exists joint_movement_r;
create external table joint_movement_r (region string, num_events int, sum_mismatch int,sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, perc_suicide float, perc_doubt float, perc_success float, perc_mismatch float, killed_per_event float, region1 string, pf_movement_domestic_avg_r float, pf_movement_foreign_avg_r float, pf_movement_avg_r float);
truncate table joint_movement_r;
insert into joint_movement_r select * from agg_gtd_master_r inner join hfi_movement_r on agg_gtd_master_r.region=hfi_movement_r.region;


--correlation num_events, pf_movement_domestic_avg_r -0.6054089362005651
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_domestic_avg_r as x, num_events as y from joint_movement_r) joint_movement_r;

--correlation num_events, pf_movement_foreign_avg_r  -0.6876389004674183
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_foreign_avg_r as x, num_events as y from joint_movement_r) joint_movement_r;

--correlation num_events, pf_movement_avg_r -0.7660694493775217
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_avg_r as x, num_events as y from joint_movement_r) joint_movement_r;


--correlation sum_mismatch, pf_movement_domestic_avg_r -0.2760576003773071
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_domestic_avg_r as x, sum_mismatch as y from joint_movement_r) joint_movement_r;

--correlation sum_mismatch, pf_movement_foreign_avg_r  -0.3571514299018476
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_foreign_avg_r as x, sum_mismatch as y from joint_movement_r) joint_movement_r;

--correlation sum_mismatch, pf_movement_avg_r -0.4390617792705432
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_avg_r as x, sum_mismatch as y from joint_movement_r) joint_movement_r;


--correlation perc_mismatch, pf_movement_domestic_avg_r 0.3197075987788187
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_domestic_avg_r as x, perc_mismatch as y from joint_movement_r) joint_movement_r;

--correlation perc_mismatch, pf_movement_foreign_avg_r 0.3094022992642456
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_foreign_avg_r as x, perc_mismatch as y from joint_movement_r) joint_movement_r;

--correlation perc_mismatch, pf_movement_avg_r 0.3136788326685807
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_avg_r as x, perc_mismatch as y from joint_movement_r) joint_movement_r;


--correlation perc_doubt, pf_movement_domestic_avg_r -0.1210031465266982
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_domestic_avg_r as x, perc_doubt as y from joint_movement_r) joint_movement_r;

--correlation perc_doubt, pf_movement_foreign_avg_r  -0.06849938278220262
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_foreign_avg_r as x, perc_doubt as y from joint_movement_r) joint_movement_r;

--correlation perc_doubt, pf_movement_avg_r -0.09744082391862408
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement_avg_r as x, perc_doubt as y from joint_movement_r) joint_movement_r;
