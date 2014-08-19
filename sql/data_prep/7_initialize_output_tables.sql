﻿-- residential
DROP TABLE IF EXISTS diffusion_wind.outputs_res CASCADE;
CREATE TABLE diffusion_wind.outputs_res (
	micro_id				bigint,
	county_id				integer,
	bin_id					integer,
	year					integer,
	customer_expec_elec_rates		numeric, 
	ownership_model				text,
	loan_term_yrs				INTEGER, 
	loan_rate				numeric,
	down_payment				numeric,
	discount_rate				numeric,
	tax_rate				numeric,
	length_of_irr_analysis_yrs		INTEGER,
	market_share_last_year			numeric,
	number_of_adopters_last_year		numeric,
	installed_capacity_last_year		numeric,
	market_value_last_year			numeric,
	value_of_increment			numeric, 
	value_of_pbi_fit			numeric, 
	value_of_ptc				numeric, 
	pbi_fit_length				numeric, 	
	ptc_length				INTEGER,
	value_of_rebate				numeric,
	value_of_tax_credit_or_deduction	numeric,
	ic					numeric,
	payback_period				NUMERIC, 
	lcoe					numeric,
	payback_key				INTEGER, 
	max_market_share			numeric, 
	diffusion_market_share			numeric,
	new_market_share			numeric,
	new_adopters				numeric,
	new_capacity				numeric,
	new_market_value			numeric,
	market_share				numeric,	
	number_of_adopters			numeric,	
	installed_capacity			numeric,	
	market_value				numeric
);

CREATE INDEX outputs_res_join_fields_btree ON diffusion_wind.outputs_res USING btree(county_id,bin_id,year);


-- commercial
DROP TABLE IF EXISTS diffusion_wind.outputs_com CASCADE;
CREATE TABLE diffusion_wind.outputs_com (
	micro_id				bigint,
	county_id				integer,
	bin_id					integer,
	year					integer,
	customer_expec_elec_rates		numeric, 
	ownership_model				text,
	loan_term_yrs				INTEGER, 
	loan_rate				numeric,
	down_payment				numeric,
	discount_rate				numeric,
	tax_rate				numeric,
	length_of_irr_analysis_yrs		INTEGER,
	market_share_last_year			numeric,
	number_of_adopters_last_year		numeric,
	installed_capacity_last_year		numeric,
	market_value_last_year			numeric,
	value_of_increment			numeric, 
	value_of_pbi_fit			numeric, 
	value_of_ptc				numeric, 
	pbi_fit_length				numeric, 	
	ptc_length				INTEGER,
	value_of_rebate				numeric,
	value_of_tax_credit_or_deduction	numeric,
	ic					numeric,
	payback_period				NUMERIC, 
	lcoe					numeric,
	payback_key				INTEGER, 
	max_market_share			numeric, 
	diffusion_market_share			numeric,
	new_market_share			numeric,
	new_adopters				numeric,
	new_capacity				numeric,
	new_market_value			numeric,
	market_share				numeric,	
	number_of_adopters			numeric,	
	installed_capacity			numeric,	
	market_value				numeric
);

CREATE INDEX outputs_com_join_fields_btree ON diffusion_wind.outputs_com USING btree(county_id,bin_id,year);

-- industrial
DROP TABLE IF EXISTS diffusion_wind.outputs_ind CASCADE;
CREATE TABLE diffusion_wind.outputs_ind (
	micro_id				bigint,
	county_id				integer,
	bin_id					integer,
	year					integer,
	customer_expec_elec_rates		numeric, 
	ownership_model				text,
	loan_term_yrs				INTEGER, 
	loan_rate				numeric,
	down_payment				numeric,
	discount_rate				numeric,
	tax_rate				numeric,
	length_of_irr_analysis_yrs		INTEGER,
	market_share_last_year			numeric,
	number_of_adopters_last_year		numeric,
	installed_capacity_last_year		numeric,
	market_value_last_year			numeric,
	value_of_increment			numeric, 
	value_of_pbi_fit			numeric, 
	value_of_ptc				numeric, 
	pbi_fit_length				numeric, 	
	ptc_length				INTEGER,
	value_of_rebate				numeric,
	value_of_tax_credit_or_deduction	numeric,
	ic					numeric,
	payback_period				NUMERIC, 
	lcoe					numeric,
	payback_key				INTEGER, 
	max_market_share			numeric, 
	diffusion_market_share			numeric,
	new_market_share			numeric,
	new_adopters				numeric,
	new_capacity				numeric,
	new_market_value			numeric,
	market_share				numeric,	
	number_of_adopters			numeric,	
	installed_capacity			numeric,	
	market_value				numeric
);

CREATE INDEX outputs_ind_join_fields_btree ON diffusion_wind.outputs_ind USING btree(county_id,bin_id,year);
