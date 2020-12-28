#!/usr/bin/env python

import csv
import sys

columns = ['year','ISO_code','countries','region','hf_score','hf_rank','hf_quartile','pf_rol_procedural','pf_rol_civil','pf_rol_criminal','pf_rol','pf_ss_homicide','pf_ss_disappearances_disap','pf_ss_disappearances_violent','pf_ss_disappearances_organized','pf_ss_disappearances_fatalities','pf_ss_disappearances_injuries','pf_ss_disappearances','pf_ss_women_fgm','pf_ss_women_inheritance_widows','pf_ss_women_inheritance_daughters','pf_ss_women_inheritance','pf_ss_women','pf_ss','pf_movement_domestic',
'pf_movement_foreign','pf_movement_women','pf_movement','pf_religion_estop_establish','pf_religion_estop_operate','pf_religion_estop','pf_religion_harassment','pf_religion_restrictions','pf_religion','pf_association_association','pf_association_assembly','pf_association_political_establish','pf_association_political_operate','pf_association_political','pf_association_prof_establish','pf_association_prof_operate','pf_association_prof','pf_association_sport_establish','pf_association_sport_operate','pf_association_sport','pf_association','pf_expression_killed','pf_expression_jailed','pf_expression_influence','pf_expression_control',
'pf_expression_cable','pf_expression_newspapers','pf_expression_internet','pf_expression','pf_identity_legal','pf_identity_sex_male','pf_identity_sex_female','pf_identity_sex','pf_identity_divorce','pf_identity','pf_score','pf_rank','ef_government_consumption','ef_government_transfers','ef_government_enterprises','ef_government_tax_income','ef_government_tax_payroll','ef_government_tax','ef_government_soa','ef_government','ef_legal_judicial','ef_legal_courts','ef_legal_protection','ef_legal_military','ef_legal_integrity',
'ef_legal_enforcement','ef_legal_restrictions','ef_legal_police','ef_legal_crime','ef_legal_gender','ef_legal','ef_money_growth','ef_money_sd','ef_money_inflation','ef_money_currency','ef_money','ef_trade_tariffs_revenue','ef_trade_tariffs_mean','ef_trade_tariffs_sd','ef_trade_tariffs','ef_trade_regulatory_nontariff','ef_trade_regulatory_compliance','ef_trade_regulatory','ef_trade_black','ef_trade_movement_foreign','ef_trade_movement_capital','ef_trade_movement_visit','ef_trade_movement','ef_trade','ef_regulation_credit_ownership',
'ef_regulation_credit_private','ef_regulation_credit_interest','ef_regulation_credit','ef_regulation_labor_minwage','ef_regulation_labor_firing','ef_regulation_labor_bargain','ef_regulation_labor_hours','ef_regulation_labor_dismissal','ef_regulation_labor_conscription','ef_regulation_labor','ef_regulation_business_adm','ef_regulation_business_bureaucracy','ef_regulation_business_start','ef_regulation_business_bribes','ef_regulation_business_licensing','ef_regulation_business_compliance','ef_regulation_business','ef_regulation','ef_score','ef_rank']

#input file is hfi_data.csv

reader = csv.DictReader(sys.stdin, delimiter=';', fieldnames = columns)

average_year_dict=[]
average_year_count=[]

param=['hf_score','pf_score', 'ef_score','pf_expression_newspapers', 'pf_expression_internet', 'ef_legal']

for z in param:
	z_list={}
	z_count={}
	average_year_dict.append(z_list)
	average_year_count.append(z_count)

for row in reader:
	x=row['year']

	for y in param:
		i=param.index(y)

		if row[y]!='-':
			if x in average_year_dict[i].keys():
				average_year_dict[i][x]+=float(row[y])
				average_year_count[i][x]+=1
			else:
				average_year_dict[i][x]=float(row[y])
				average_year_count[i][x]=1

for y in param:
	i=param.index(y)
	for x in average_year_dict[i]:
		avg=average_year_dict[i][x]/average_year_count[i][x]
		s='Average '+param[i]+' '+str(x)
		print '%s\t%s\n' % (s, avg)
