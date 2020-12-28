select target_type from gtd_master group by target_type order by target_type;

-- more broad sense of aggregate gtd_master
drop table if exists agg_gtd_master_types_ts;
create external table agg_gtd_master_types_ts (year int, country string, region string, target_type string, attack_type string, sum_event int, sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int);
truncate table agg_gtd_master_types_ts;
insert into agg_gtd_master_types_ts select year, country, region, target_type, attack_type, cast(count(event_id) as int), cast(sum(success)as int), cast(sum(doubt_terrorist) as int), cast(sum(suicide) as int), cast(sum(num_kill) as int) from gtd_master group by year, country, region, target_type, attack_type;
select * from agg_gtd_master_types_ts order by region, country, year, target_type, attack_type limit 10;

-- specific target_type

--'Business'
drop table if exists agg_gtd_target_business_ts;
create external table agg_gtd_target_business_ts (year int, country string, region string, target_type string, sum_event int, sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int);
truncate table agg_gtd_target_business_ts;
insert into agg_gtd_target_business_ts select year, country, region, target_type, cast(count(event_id) as int), cast(sum(success)as int), cast(sum(doubt_terrorist) as int), cast(sum(suicide) as int), cast(sum(num_kill) as int) from gtd_master where target_type = 'Business' group by year, country, region, target_type;
select * from agg_gtd_target_business_ts order by region, country, year, target_type limit 10;

drop table if exists agg_target_business_ts;
create external table agg_target_business_ts (year int, country string, region string, target_type string, sum_event int, sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, year1 int, country1 string, region1 string, hf_score float, pf_score float, ef_score float, pf_rol float, pf_ss float, pf_movement float, pf_expression float, pf_religion float, pf_identity float);
truncate table agg_target_business_ts;
insert into agg_target_bussiness_ts select * from agg_gtd_target_business_ts INNER join hfi_main on agg_gtd_target_business_ts.year=hfi_main.year and agg_gtd_target_business_ts.country=hfi_main.country and agg_gtd_target_business_ts.region=hfi_main.region;

--hf_score and sum_event  -0.2186961962553501
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select hf_score as x, sum_event as y from agg_target_business_ts) agg_target_business_ts where x!=-1;

--hf_score and sum_success -0.2203865671949161
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select hf_score as x, sum_success as y from agg_target_business_ts) agg_target_business_ts where x!=-1;

--hf_score and sum_doubt  -0.1096614447696293
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select hf_score as x, sum_doubt as y from agg_target_business_ts) agg_target_business_ts where x!=-1;

--hf_score and sum_suicide  -0.2840446701618343
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select hf_score as x, sum_suicide as y from agg_target_business_ts) agg_target_business_ts where x!=-1;

--hf_score and sum_num_kill -0.2579867506589929
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select hf_score as x, sum_num_kill as y from agg_target_business_ts) agg_target_business_ts where x!=-1;


--pf_score and sum_event  -0.2420180262377825
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_event as y from agg_target_business_ts) agg_target_business_ts where x!=-1;

--pf_score and sum_success -0.2429044098062401
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_success as y from agg_target_business_ts) agg_target_business_ts where x!=-1;

--pf_score and sum_doubt -0.1237325299401097
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_doubt as y from agg_target_business_ts) agg_target_business_ts where x!=-1;

--pf_score and sum_suicide -0.3103267247394221
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_suicide as y from agg_target_business_ts) agg_target_business_ts where x!=-1;

--pf_score and sum_num_kill -0.2805227418884986
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_num_kill as y from agg_target_business_ts) agg_target_business_ts where x!=-1;


--ef_score and sum_event  -0.142690835886893
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_event as y from agg_target_business_ts) agg_target_business_ts where x!=-1;

--ef_score and sum_success -0.1454679617455107
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_success as y from agg_target_business_ts) agg_target_business_ts where x!=-1;

--ef_score and sum_doubt -0.06735800304335898
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_doubt as y from agg_target_business_ts) agg_target_business_ts where x!=-1;

--ef_score and sum_suicide -0.191623629289037
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_suicide as y from agg_target_business_ts) agg_target_business_ts where x!=-1;

--ef_score and sum_num_kill -0.1768260631005825
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_num_kill as y from agg_target_business_ts) agg_target_business_ts where x!=-1;



--pf_rol and sum_event  -0.2513677415285278
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_rol as x, sum_event as y from agg_target_business_ts) agg_target_business_ts where x!=-1;

--pf_ss and sum_event  -0.3070059584682477
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss as x, sum_event as y from agg_target_business_ts) agg_target_business_ts where x!=-1;

--pf_movement and sum_event  -0.1448482220826458
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement as x, sum_event as y from agg_target_business_ts) agg_target_business_ts where x!=-1;

--pf_expression and sum_event  -0.09123929187175095
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression as x, sum_event as y from agg_target_business_ts) agg_target_business_ts where x!=-1;

--pf_religion and sum_event  -0.08238963215683003
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_religion as x, sum_event as y from agg_target_business_ts) agg_target_business_ts where x!=-1;

--pf_identity and sum_event  -0.2368375592982362
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_identity as x, sum_event as y from agg_target_business_ts) agg_target_business_ts where x!=-1;



--'Government (General)'
drop table if exists agg_gtd_target_gen_gov_ts;
create external table agg_gtd_target_gen_gov_ts (year int, country string, region string, target_type string, sum_event int, sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int);
truncate table agg_gtd_target_gen_gov_ts;
insert into agg_gtd_target_gen_gov_ts select year, country, region, target_type, cast(count(event_id) as int), cast(sum(success)as int), cast(sum(doubt_terrorist) as int), cast(sum(suicide) as int), cast(sum(num_kill) as int) from gtd_master where target_type = 'Government (General)' group by year, country, region, target_type;
select * from agg_gtd_target_gen_gov_ts order by region, country, year, target_type;

drop table if exists agg_target_gen_gov_ts;
create external table agg_target_gen_gov_ts (year int, country string, region string, target_type string, sum_event int, sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, year1 int, country1 string, region1 string, hf_score float, pf_score float, ef_score float, pf_rol float, pf_ss float, pf_movement float, pf_expression float, pf_religion float, pf_identity float);
truncate table agg_target_gen_gov_ts;
insert into agg_target_gen_gov_ts select * from agg_gtd_target_gen_gov_ts INNER join hfi_main on agg_gtd_target_gen_gov_ts.year=hfi_main.year and agg_gtd_target_gen_gov_ts.country=hfi_main.country and agg_gtd_target_gen_gov_ts.region=hfi_main.region;


--pf_score and sum_event  -0.2138870817950855
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_event as y from agg_target_gen_gov_ts) agg_target_gen_gov_ts where x!=-1;

--pf_score and sum_success -0.2144434602259516
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_success as y from agg_target_gen_gov_ts) agg_target_gen_gov_ts where x!=-1;

--pf_score and sum_doubt -0.0575938243599411
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_doubt as y from agg_target_gen_gov_ts) agg_target_gen_gov_ts where x!=-1;

--pf_score and sum_suicide -0.3503355347297925
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_suicide as y from agg_target_gen_gov_ts) agg_target_gen_gov_ts where x!=-1;

--pf_score and sum_num_kill -0.3498860480994208
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_num_kill as y from agg_target_gen_gov_ts) agg_target_gen_gov_ts where x!=-1;


--ef_score and sum_event  -0.06142940764069421
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_event as y from agg_target_gen_gov_ts) agg_target_gen_gov_ts where x!=-1;

--ef_score and sum_success -0.07137688001218981
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_success as y from agg_target_gen_gov_ts) agg_target_gen_gov_ts where x!=-1;

--ef_score and sum_doubt -0.06735800304335898
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_doubt as y from agg_target_gen_gov_ts) agg_target_gen_gov_ts where x!=-1;

--ef_score and sum_suicide -0.03643165650482617
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_suicide as y from agg_target_gen_gov_ts) agg_target_gen_gov_ts where x!=-1;

--ef_score and sum_num_kill -0.1610283766969726
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_num_kill as y from agg_target_gen_gov_ts) agg_target_gen_gov_ts where x!=-1;



--pf_rol and sum_event  -0.2551244330682672
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_rol as x, sum_event as y from agg_target_gen_gov_ts) agg_target_gen_gov_ts where x!=-1;

--pf_ss and sum_event  -0.2749183868754812
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss as x, sum_event as y from agg_target_gen_gov_ts) agg_target_gen_gov_ts where x!=-1;

--pf_movement and sum_event  -0.1117568926771026
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement as x, sum_event as y from agg_target_gen_gov_ts) agg_target_gen_gov_ts where x!=-1;

--pf_expression and sum_event  -0.02696581854729257
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression as x, sum_event as y from agg_target_gen_gov_ts) agg_target_gen_gov_ts where x!=-1;

--pf_religion and sum_event  0.000971658326665076
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_religion as x, sum_event as y from agg_target_gen_gov_ts) agg_target_gen_gov_ts where x!=-1;

--pf_identity and sum_event  -0.2064685163536775
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_identity as x, sum_event as y from agg_target_gen_gov_ts) agg_target_gen_gov_ts where x!=-1;


--'Government (Diplomatic)'
drop table if exists agg_gtd_target_dip_gov_ts;
create external table agg_gtd_target_dip_gov_ts (year int, country string, region string, target_type string, sum_event int, sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int);
truncate table agg_gtd_target_dip_gov_ts;
insert into agg_gtd_target_dip_gov_ts select year, country, region, target_type, cast(count(event_id) as int), cast(sum(success)as int), cast(sum(doubt_terrorist) as int), cast(sum(suicide) as int), cast(sum(num_kill) as int) from gtd_master where target_type = 'Government (Diplomatic)' group by year, country, region, target_type;
select * from agg_gtd_target_dip_gov_ts order by region, country, year, target_type;

drop table if exists agg_target_dip_gov_ts;
create external table agg_target_dip_gov_ts (year int, country string, region string, target_type string, sum_event int, sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, year1 int, country1 string, region1 string, hf_score float, pf_score float, ef_score float, pf_rol float, pf_ss float, pf_movement float, pf_expression float, pf_religion float, pf_identity float);
truncate table agg_target_dip_gov_ts;
insert into agg_target_dip_gov_ts select * from agg_gtd_target_dip_gov_ts INNER join hfi_main on agg_gtd_target_dip_gov_ts.year=hfi_main.year and agg_gtd_target_dip_gov_ts.country=hfi_main.country and agg_gtd_target_dip_gov_ts.region=hfi_main.region;



--pf_score and sum_event  -0.2422690352072454
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_event as y from agg_target_dip_gov_ts) agg_target_dip_gov_ts where x!=-1;

--pf_score and sum_success -0.2435016131576648
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_success as y from agg_target_dip_gov_ts) agg_target_dip_gov_ts where x!=-1;

--pf_score and sum_doubt -0.1670741085795267
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_doubt as y from agg_target_dip_gov_ts) agg_target_dip_gov_ts where x!=-1;

--pf_score and sum_suicide -0.1015269822620536
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_suicide as y from agg_target_dip_gov_ts) agg_target_dip_gov_ts where x!=-1;

--pf_score and sum_num_kill -0.1644761667334973
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_num_kill as y from agg_target_dip_gov_ts) agg_target_dip_gov_ts where x!=-1;


--ef_score and sum_event -0.2823942356053825
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_event as y from agg_target_dip_gov_ts) agg_target_dip_gov_ts where x!=-1;

--ef_score and sum_success -0.2938671166662638
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_success as y from agg_target_dip_gov_ts) agg_target_dip_gov_ts where x!=-1;

--ef_score and sum_doubt -0.228656960971143
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_doubt as y from agg_target_dip_gov_ts) agg_target_dip_gov_ts where x!=-1;

--ef_score and sum_suicide -0.05781818918644432
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_suicide as y from agg_target_dip_gov_ts) agg_target_dip_gov_ts where x!=-1;

--ef_score and sum_num_kill -0.2016903646076905
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_num_kill as y from agg_target_dip_gov_ts) agg_target_dip_gov_ts where x!=-1;



--pf_rol and sum_event -0.2732046204825764
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_rol as x, sum_event as y from agg_target_dip_gov_ts) agg_target_dip_gov_ts where x!=-1;

--pf_ss and sum_event -0.4153261314793003
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss as x, sum_event as y from agg_target_dip_gov_ts) agg_target_dip_gov_ts where x!=-1;

--pf_movement and sum_event  -0.1111558831687655
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement as x, sum_event as y from agg_target_dip_gov_ts) agg_target_dip_gov_ts where x!=-1;

--pf_expression and sum_event -0.09128085360808291
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression as x, sum_event as y from agg_target_dip_gov_ts) agg_target_dip_gov_ts where x!=-1;

--pf_religion and sum_event  0.1150300960635536
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_religion as x, sum_event as y from agg_target_dip_gov_ts) agg_target_dip_gov_ts where x!=-1;

--pf_identity and sum_event -0.1312764125547893
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_identity as x, sum_event as y from agg_target_dip_gov_ts) agg_target_dip_gov_ts where x!=-1;

--'Military'
drop table if exists agg_gtd_target_military_ts;
create external table agg_gtd_target_military_ts (year int, country string, region string, target_type string, sum_event int, sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int);
truncate table agg_gtd_target_military_ts;
insert into agg_gtd_target_military_ts select year, country, region, target_type, cast(count(event_id) as int), cast(sum(success)as int), cast(sum(doubt_terrorist) as int), cast(sum(suicide) as int), cast(sum(num_kill) as int) from gtd_master where target_type = 'Military' group by year, country, region, target_type;
select * from agg_gtd_target_military_ts order by region, country, year, target_type limit 10;

drop table if exists agg_target_military_ts;
create external table agg_target_military_ts (year int, country string, region string, target_type string,sum_event int, sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, year1 int, country1 string, region1 string, hf_score float, pf_score float, ef_score float, pf_rol float, pf_ss float, pf_movement float, pf_expression float, pf_religion float, pf_identity float);
truncate table agg_target_military_ts;
insert into agg_target_military_ts select * from agg_gtd_target_military_ts INNER join hfi_main on agg_gtd_target_military_ts.year=hfi_main.year and agg_gtd_target_military_ts.country=hfi_main.country and agg_gtd_target_military_ts.region=hfi_main.region;
select * from agg_target_military_ts order by region, country, year, target_type limit 10;


--pf_score and sum_event  -0.3160709957475701
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_event as y from agg_target_military_ts) agg_target_military_ts where x!=-1;

--pf_score and sum_success -0.3154462747879878
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_success as y from agg_target_military_ts) agg_target_military_ts where x!=-1;

--pf_score and sum_doubt -0.3072275518195788
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_doubt as y from agg_target_military_ts) agg_target_military_ts where x!=-1;

--pf_score and sum_suicide -0.304428283224489
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_suicide as y from agg_target_military_ts) agg_target_military_ts where x!=-1;

--pf_score and sum_num_kill -0.4296946595998887
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_num_kill as y from agg_target_military_ts) agg_target_military_ts where x!=-1;


--ef_score and sum_event  -0.1342362921130257
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_event as y from agg_target_military_ts) agg_target_military_ts where x!=-1;

--ef_score and sum_success -0.1375959657038341
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_success as y from agg_target_military_ts) agg_target_military_ts where x!=-1;

--ef_score and sum_doubt -0.1307489641676797
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_doubt as y from agg_target_military_ts) agg_target_military_ts where x!=-1;

--ef_score and sum_suicide -0.1709538185479028
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_suicide as y from agg_target_military_ts) agg_target_military_ts where x!=-1;

--ef_score and sum_num_kill -0.2395821335093709
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_num_kill as y from agg_target_military_ts) agg_target_military_ts where x!=-1;



--pf_rol and sum_event  -0.2754305533244497
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_rol as x, sum_event as y from agg_target_military_ts) agg_target_military_ts where x!=-1;

--pf_ss and sum_event  -0.3559277053527976
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss as x, sum_event as y from agg_target_military_ts) agg_target_military_ts where x!=-1;

--pf_movement and sum_event  -0.1455316410268352
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement as x, sum_event as y from agg_target_military_ts) agg_target_military_ts where x!=-1;

--pf_expression and sum_event  -0.1942105431438529
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression as x, sum_event as y from agg_target_military_ts) agg_target_military_ts where x!=-1;

--pf_religion and sum_event  -0.1680664402754537
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_religion as x, sum_event as y from agg_target_military_ts) agg_target_military_ts where x!=-1;

--pf_identity and sum_event  -0.2098778782783816
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_identity as x, sum_event as y from agg_target_military_ts) agg_target_military_ts where x!=-1;

--'Religious Figures/Institutions'
drop table if exists agg_gtd_target_religion_ts;
create external table agg_gtd_target_religion_ts (year int, country string, region string, target_type string, sum_event int, sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int);
truncate table agg_gtd_target_religion_ts;
insert into agg_gtd_target_religion_ts select year, country, region, target_type, cast(count(event_id) as int), cast(sum(success)as int), cast(sum(doubt_terrorist) as int), cast(sum(suicide) as int), cast(sum(num_kill) as int) from gtd_master where target_type = 'Religious Figures/Institutions' group by year, country, region, target_type;
select * from agg_gtd_target_religion_ts order by region, country, year, target_type limit 10;

drop table if exists agg_target_religion_ts;
create external table agg_target_religion_ts (year int, country string, region string, target_type string, sum_event int, sum_success int, sum_doubt int, sum_suicide int, sum_num_kill int, year1 int, country1 string, region1 string, hf_score float, pf_score float, ef_score float, pf_rol float, pf_ss float, pf_movement float, pf_expression float, pf_religion float, pf_identity float);
truncate table agg_target_religion_ts;
insert into agg_target_religion_ts select * from agg_gtd_target_religion_ts INNER join hfi_main on agg_gtd_target_religion_ts.year=hfi_main.year and agg_gtd_target_religion_ts.country=hfi_main.country and agg_gtd_target_religion_ts.region=hfi_main.region;
select * from agg_target_religion_ts order by region, country, year, target_type limit 10;

--pf_score and sum_event  -0.2788424523864767
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_event as y from agg_target_religion_ts) agg_target_religion_ts where x!=-1;

--pf_score and sum_success -0.2902783498247515
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_success as y from agg_target_religion_ts) agg_target_religion_ts where x!=-1;

--pf_score and sum_doubt -0.06551362797457648
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_doubt as y from agg_target_religion_ts) agg_target_religion_ts where x!=-1;

--pf_score and sum_suicide -0.2880477944304409
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_suicide as y from agg_target_religion_ts) agg_target_religion_ts where x!=-1;

--pf_score and sum_num_kill -0.3388840453690288
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_score as x, sum_num_kill as y from agg_target_religion_ts) agg_target_religion_ts where x!=-1;


--ef_score and sum_event  -0.1784039420809513
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_event as y from agg_target_religion_ts) agg_target_religion_ts where x!=-1;

--ef_score and sum_success -0.1920650449639306
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_success as y from agg_target_religion_ts) agg_target_religion_ts where x!=-1;

--ef_score and sum_doubt 0.01108324851516206
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_doubt as y from agg_target_religion_ts) agg_target_religion_ts where x!=-1;

--ef_score and sum_suicide -0.1809038195545936
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_suicide as y from agg_target_religion_ts) agg_target_religion_ts where x!=-1;

--ef_score and sum_num_kill -0.2884227565523556
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select ef_score as x, sum_num_kill as y from agg_target_religion_ts) agg_target_religion_ts where x!=-1;



--pf_rol and sum_event  -0.2404263192686513
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_rol as x, sum_event as y from agg_target_religion_ts) agg_target_religion_ts where x!=-1;

--pf_ss and sum_event  -0.3553327888046254
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_ss as x, sum_event as y from agg_target_religion_ts) agg_target_religion_ts where x!=-1;

--pf_movement and sum_event  -0.2270262187304161
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_movement as x, sum_event as y from agg_target_religion_ts) agg_target_religion_ts where x!=-1;

--pf_expression and sum_event  -0.0911571120009138
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_expression as x, sum_event as y from agg_target_religion_ts) agg_target_religion_ts where x!=-1;

--pf_religion and sum_event  -0.08524186992629658
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_religion as x, sum_event as y from agg_target_religion_ts) agg_target_religion_ts where x!=-1;

--pf_identity and sum_event  -0.334857111040976
select ((sum(x * y) - (sum(x) * sum(y)) / count(*) ) ) / (sqrt(sum(x * x) - (sum(x) * sum (x)) / count(*) ) * sqrt(sum(y * y) - (sum(y) * sum(y)) / count(*) ) )
as correlation from (select pf_identity as x, sum_event as y from agg_target_religion_ts) agg_target_religion_ts where x!=-1;
