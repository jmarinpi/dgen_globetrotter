﻿-- residential
DROP TABLE IF EXISTS wind_ds.outputs_res CASCADE;
CREATE TABLE wind_ds.outputs_res (
	gid 					integer,
	year					integer,
	-- eventually these could just be joined in -- but leave for simplicity
	county_id				integer,
	state_abbr				character varying(2),
	census_division_abbr 			text,
	census_region 				text,
	row_number 				bigint,
	max_height 				integer,
	elec_rate_cents_per_kwh 		numeric,
	carbon_price_cents_per_kwh		numeric,
	cap_cost_multiplier 			numeric,
	fixed_om_dollars_per_kw_per_yr 	numeric,
	variable_om_dollars_per_kwh 		numeric,
	installed_costs_dollars_per_kw 	numeric,
	ann_cons_kwh 				numeric,
	prob 					numeric,
	weight 					numeric,
	customers_in_bin 			numeric,
	initial_customers_in_bin 		numeric,
	load_kwh_in_bin 			numeric,
	initial_load_kwh_in_bin 		numeric,
	load_kwh_per_customer_in_bin 		numeric,
	nem_system_limit_kw			float,
	i 					integer,
	j 					integer,
	cf_bin 					integer,
	aep_scale_factor 			numeric,
	derate_factor 				numeric,
	naep 					numeric,
	nameplate_capacity_kw 			numeric,
	power_curve_id 				integer,
	turbine_height_m 			integer,
	scoe 					double precision, 
	-----------
	value_of_pbi_fit			numeric, 
	max_market_share			numeric, 
	market_share_last_year			numeric,
	discount_rate				numeric,
	pbi_fit_length				numeric, 
	ic					numeric,
	down_payment				numeric,
	payback_period				NUMERIC, 
	installed_capacity_last_year		numeric,
	loan_rate				numeric,
	value_of_ptc				numeric, 
	market_value				numeric,
	market_share				numeric,
	value_of_tax_credit_or_deduction	numeric,
	number_of_adopters_last_year		numeric,
	payback_key				INTEGER, 
	market_value_last_year			numeric,
	loan_term_yrs				INTEGER, 
	ptc_length				INTEGER,
	aep					numeric,
	installed_capacity			numeric,
	tax_rate				numeric,
	customer_expec_elec_rates		numeric, 
	length_of_irr_analysis_yrs		INTEGER,
	cap					numeric,
	ownership_model				text,
	lcoe					numeric,
	number_of_adopters			numeric,
	value_of_increment			numeric, 
	value_of_rebate				numeric,
	diffusion_market_share			numeric,
	new_market_share			numeric,
	new_adopters				numeric,
	new_capacity				numeric,
	new_market_value			numeric 
);


-- commercial
DROP TABLE IF EXISTS wind_ds.outputs_com CASCADE;
CREATE TABLE wind_ds.outputs_com (
	gid 					integer,
	year					integer,
	-- eventually these could just be joined in -- but leave for simplicity
	county_id				integer,
	state_abbr				character varying(2),
	census_division_abbr 			text,
	census_region 				text,
	row_number 				bigint,
	max_height 				integer,
	elec_rate_cents_per_kwh 		numeric,
	carbon_price_cents_per_kwh		numeric,
	cap_cost_multiplier 			numeric,
	fixed_om_dollars_per_kw_per_yr 	numeric,
	variable_om_dollars_per_kwh 		numeric,
	installed_costs_dollars_per_kw 	numeric,
	ann_cons_kwh 				numeric,
	prob 					numeric,
	weight 					numeric,
	customers_in_bin 			numeric,
	initial_customers_in_bin 		numeric,
	load_kwh_in_bin 			numeric,
	initial_load_kwh_in_bin 		numeric,
	load_kwh_per_customer_in_bin 		numeric,
	nem_system_limit_kw			float,
	i 					integer,
	j 					integer,
	cf_bin 					integer,
	aep_scale_factor 			numeric,
	derate_factor 				numeric,
	naep 					numeric,
	nameplate_capacity_kw 			numeric,
	power_curve_id 				integer,
	turbine_height_m 			integer,
	scoe 					double precision, 
	-----------
	value_of_pbi_fit			numeric, 
	max_market_share			numeric, 
	market_share_last_year			numeric,
	discount_rate				numeric,
	pbi_fit_length				numeric, 
	ic					numeric,
	down_payment				numeric,
	payback_period				NUMERIC, 
	installed_capacity_last_year		numeric,
	loan_rate				numeric,
	value_of_ptc				numeric, 
	market_value				numeric,
	market_share				numeric,
	value_of_tax_credit_or_deduction	numeric,
	number_of_adopters_last_year		numeric,
	payback_key				INTEGER, 
	market_value_last_year			numeric,
	loan_term_yrs				INTEGER, 
	ptc_length				INTEGER,
	aep					numeric,
	installed_capacity			numeric,
	tax_rate				numeric,
	customer_expec_elec_rates		numeric, 
	length_of_irr_analysis_yrs		INTEGER,
	cap					numeric,
	ownership_model				text,
	lcoe					numeric,
	number_of_adopters			numeric,
	value_of_increment			numeric, 
	value_of_rebate				numeric,
	diffusion_market_share			numeric,
	new_market_share			numeric,
	new_adopters				numeric,
	new_capacity				numeric,
	new_market_value			numeric 
);


-- industrial
DROP TABLE IF EXISTS wind_ds.outputs_ind CASCADE;
CREATE TABLE wind_ds.outputs_ind (
	gid 					integer,
	year					integer,
	-- eventually these could just be joined in -- but leave for simplicity
	county_id				integer,
	state_abbr				character varying(2),
	census_division_abbr 			text,
	census_region 				text,
	row_number 				bigint,
	max_height 				integer,
	elec_rate_cents_per_kwh 		numeric,
	carbon_price_cents_per_kwh		numeric,
	cap_cost_multiplier 			numeric,
	fixed_om_dollars_per_kw_per_yr 	numeric,
	variable_om_dollars_per_kwh 		numeric,
	installed_costs_dollars_per_kw 	numeric,
	ann_cons_kwh 				numeric,
	prob 					numeric,
	weight 					numeric,
	customers_in_bin 			numeric,
	initial_customers_in_bin 		numeric,
	load_kwh_in_bin 			numeric,
	initial_load_kwh_in_bin 		numeric,
	load_kwh_per_customer_in_bin 		numeric,
	nem_system_limit_kw			float,
	i 					integer,
	j 					integer,
	cf_bin 					integer,
	aep_scale_factor 			numeric,
	derate_factor 				numeric,
	naep 					numeric,
	nameplate_capacity_kw 			numeric,
	power_curve_id 				integer,
	turbine_height_m 			integer,
	scoe 					double precision, 
	-----------
	value_of_pbi_fit			numeric, 
	max_market_share			numeric, 
	market_share_last_year			numeric,
	discount_rate				numeric,
	pbi_fit_length				numeric, 
	ic					numeric,
	down_payment				numeric,
	payback_period				NUMERIC, 
	installed_capacity_last_year		numeric,
	loan_rate				numeric,
	value_of_ptc				numeric, 
	market_value				numeric,
	market_share				numeric,
	value_of_tax_credit_or_deduction	numeric,
	number_of_adopters_last_year		numeric,
	payback_key				INTEGER, 
	market_value_last_year			numeric,
	loan_term_yrs				INTEGER, 
	ptc_length				INTEGER,
	aep					numeric,
	installed_capacity			numeric,
	tax_rate				numeric,
	customer_expec_elec_rates		numeric, 
	length_of_irr_analysis_yrs		INTEGER,
	cap					numeric,
	ownership_model				text,
	lcoe					numeric,
	number_of_adopters			numeric,
	value_of_increment			numeric, 
	value_of_rebate				numeric,
	diffusion_market_share			numeric,
	new_market_share			numeric,
	new_adopters				numeric,
	new_capacity				numeric,
	new_market_value			numeric  
);



-- create a view that combines all of these
DROP VIEW IF EXISTS wind_ds.outputs_all;
CREATE OR REPLACE VIEW wind_ds.outputs_all AS
SELECT 'residential'::text as sector, *
FROM wind_ds.outputs_res 
UNION ALL
SELECT 'commercial'::text as sector, * 
FROM wind_ds.outputs_com
UNION ALL
SELECT 'industrial'::text as sector, * 
FROM wind_ds.outputs_ind;


