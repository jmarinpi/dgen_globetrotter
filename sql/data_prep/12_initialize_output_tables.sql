-- residential
DROP TABLE IF EXISTS wind_ds.outputs_res CASCADE;
CREATE TABLE wind_ds.outputs_res (
	gid 					integer,
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
	cap					numeric,
	ic					numeric,
	aep					numeric,
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

CREATE INDEX outputs_res_gid_year_btree ON wind_ds.outputs_res USING btree(gid,year);


-- commercial
DROP TABLE IF EXISTS wind_ds.outputs_com CASCADE;
CREATE TABLE wind_ds.outputs_com (
	gid 					integer,
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
	cap					numeric,
	ic					numeric,
	aep					numeric,
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

CREATE INDEX outputs_com_gid_year_btree ON wind_ds.outputs_com USING btree(gid,year);

-- industrial
DROP TABLE IF EXISTS wind_ds.outputs_ind CASCADE;
CREATE TABLE wind_ds.outputs_ind (
	gid 					integer,
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
	cap					numeric,
	ic					numeric,
	aep					numeric,
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

CREATE INDEX outputs_ind_gid_year_btree ON wind_ds.outputs_ind USING btree(gid,year);


-- create a view that combines all of these
-- DROP VIEW IF EXISTS wind_ds.outputs_all;
-- CREATE OR REPLACE VIEW wind_ds.outputs_all AS
-- SELECT 'residential'::text as sector, 
-- 
-- a.gid, a.year, a.customer_expec_elec_rates, a.ownership_model, a.loan_term_yrs, 
-- a.loan_rate, a.down_payment, a.discount_rate, a.tax_rate, a.length_of_irr_analysis_yrs, 
-- a.market_share_last_year, a.number_of_adopters_last_year, a.installed_capacity_last_year, 
-- a.market_value_last_year, a.value_of_increment, a.value_of_pbi_fit, 
-- a.value_of_ptc, a.pbi_fit_length, a.ptc_length, a.value_of_rebate, a.value_of_tax_credit_or_deduction, 
-- a.cap, a.ic, a.aep, a.payback_period, a.lcoe, a.payback_key, a.max_market_share, 
-- a.diffusion_market_share, a.new_market_share, a.new_adopters, a.new_capacity, 
-- a.new_market_value, a.market_share, a.number_of_adopters, a.installed_capacity, 
-- a.market_value,
-- 
-- b.county_id, b.state_abbr, b.census_division_abbr, b.utility_type, 
-- b.census_region, b.row_number, b.max_height, b.elec_rate_cents_per_kwh, 
-- b.carbon_price_cents_per_kwh, b.cap_cost_multiplier, b.fixed_om_dollars_per_kw_per_yr, 
-- b.variable_om_dollars_per_kwh, b.installed_costs_dollars_per_kw, 
-- b.ann_cons_kwh, b.prob, b.weight, b.customers_in_bin, b.initial_customers_in_bin, 
-- b.load_kwh_in_bin, b.initial_load_kwh_in_bin, b.load_kwh_per_customer_in_bin, 
-- b.nem_system_limit_kw, b.excess_generation_factor, b.i, b.j, b.cf_bin, 
-- b.aep_scale_factor, b.derate_factor, b.naep, b.nameplate_capacity_kw, 
-- b.power_curve_id, b.turbine_height_m, b.scoe
-- 
-- FROM wind_ds.outputs_res a
-- LEFT JOIN wind_ds.pt_res_best_option_each_year b
-- ON a.gid = b.gid
-- and a.year = b.year
-- 
-- UNION ALL
-- 
-- SELECT 'commercial'::text as sector, 
-- 
-- a.gid, a.year, a.customer_expec_elec_rates, a.ownership_model, a.loan_term_yrs, 
-- a.loan_rate, a.down_payment, a.discount_rate, a.tax_rate, a.length_of_irr_analysis_yrs, 
-- a.market_share_last_year, a.number_of_adopters_last_year, a.installed_capacity_last_year, 
-- a.market_value_last_year, a.value_of_increment, a.value_of_pbi_fit, 
-- a.value_of_ptc, a.pbi_fit_length, a.ptc_length, a.value_of_rebate, a.value_of_tax_credit_or_deduction, 
-- a.cap, a.ic, a.aep, a.payback_period, a.lcoe, a.payback_key, a.max_market_share, 
-- a.diffusion_market_share, a.new_market_share, a.new_adopters, a.new_capacity, 
-- a.new_market_value, a.market_share, a.number_of_adopters, a.installed_capacity, 
-- a.market_value,
-- 
-- b.county_id, b.state_abbr, b.census_division_abbr, b.utility_type, 
-- b.census_region, b.row_number, b.max_height, b.elec_rate_cents_per_kwh, 
-- b.carbon_price_cents_per_kwh, b.cap_cost_multiplier, b.fixed_om_dollars_per_kw_per_yr, 
-- b.variable_om_dollars_per_kwh, b.installed_costs_dollars_per_kw, 
-- b.ann_cons_kwh, b.prob, b.weight, b.customers_in_bin, b.initial_customers_in_bin, 
-- b.load_kwh_in_bin, b.initial_load_kwh_in_bin, b.load_kwh_per_customer_in_bin, 
-- b.nem_system_limit_kw, b.excess_generation_factor, b.i, b.j, b.cf_bin, 
-- b.aep_scale_factor, b.derate_factor, b.naep, b.nameplate_capacity_kw, 
-- b.power_curve_id, b.turbine_height_m, b.scoe
-- 
-- FROM wind_ds.outputs_com a
-- LEFT JOIN wind_ds.pt_com_best_option_each_year b
-- ON a.gid = b.gid
-- and a.year = b.year
-- 
-- UNION ALL
-- SELECT 'industrial'::text as sector, 
-- 
-- a.gid, a.year, a.customer_expec_elec_rates, a.ownership_model, a.loan_term_yrs, 
-- a.loan_rate, a.down_payment, a.discount_rate, a.tax_rate, a.length_of_irr_analysis_yrs, 
-- a.market_share_last_year, a.number_of_adopters_last_year, a.installed_capacity_last_year, 
-- a.market_value_last_year, a.value_of_increment, a.value_of_pbi_fit, 
-- a.value_of_ptc, a.pbi_fit_length, a.ptc_length, a.value_of_rebate, a.value_of_tax_credit_or_deduction, 
-- a.cap, a.ic, a.aep, a.payback_period, a.lcoe, a.payback_key, a.max_market_share, 
-- a.diffusion_market_share, a.new_market_share, a.new_adopters, a.new_capacity, 
-- a.new_market_value, a.market_share, a.number_of_adopters, a.installed_capacity, 
-- a.market_value,
-- 
-- b.county_id, b.state_abbr, b.census_division_abbr, b.utility_type, 
-- b.census_region, b.row_number, b.max_height, b.elec_rate_cents_per_kwh, 
-- b.carbon_price_cents_per_kwh, b.cap_cost_multiplier, b.fixed_om_dollars_per_kw_per_yr, 
-- b.variable_om_dollars_per_kwh, b.installed_costs_dollars_per_kw, 
-- b.ann_cons_kwh, b.prob, b.weight, b.customers_in_bin, b.initial_customers_in_bin, 
-- b.load_kwh_in_bin, b.initial_load_kwh_in_bin, b.load_kwh_per_customer_in_bin, 
-- b.nem_system_limit_kw, b.excess_generation_factor, b.i, b.j, b.cf_bin, 
-- b.aep_scale_factor, b.derate_factor, b.naep, b.nameplate_capacity_kw, 
-- b.power_curve_id, b.turbine_height_m, b.scoe
-- 
-- 
-- FROM wind_ds.outputs_ind a
-- LEFT JOIN wind_ds.pt_ind_best_option_each_year b
-- ON a.gid = b.gid
-- and a.year = b.year;
-- 
-- 
