set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_template.agent_outputs;
CREATE TABLE diffusion_template.agent_outputs
(
	-- key agent identifers
	selected_option BOOLEAN,
	tech varchar(5),
	pgid bigint,
	county_id integer,
	bin_id integer,
	year integer,
	sector_abbr varchar(3),
	-- geographic info
	state_abbr varchar(2),
	census_division_abbr varchar(3),
	pca_reg TEXT,
	reeds_reg INTEGER,
	-- load and customer characteristics
	customers_in_bin NUMERIC,
	load_kwh_per_customer_in_bin NUMERIC,
	load_kwh_in_bin NUMERIC,
	max_demand_kw INTEGER,
	hdf_load_index INTEGER,
	owner_occupancy_status NUMERIC,
	pct_of_bldgs_developable NUMERIC,
	developable_customers_in_bin NUMERIC,
	developable_load_kwh_in_bin NUMERIC,
	-- solar system characteristics
	solar_re_9809_gid INTEGER,
	tilt INTEGER,
	azimuth VARCHAR(2),
	developable_roof_sqft  NUMERIC,
	inverter_lifetime_yrs NUMERIC,
	ann_system_degradation NUMERIC,
	-- wind system characteristics
	i INTEGER,
	j INTEGER,
	cf_bin INTEGER,
	power_curve_1 NUMERIC,
	power_curve_2 NUMERIC,
	power_curve_interp_factor NUMERIC,
	wind_derate_factor NUMERIC,
	turbine_height_m NUMERIC,
	turbine_size_kw NUMERIC,
	-- general system characteristics
	aep NUMERIC,
	cf numeric,
	system_size_kw NUMERIC,
	system_size_factors text,
	n_units NUMERIC,
	total_gen_twh numeric,
	-- rate inputs
	rate_id_alias INTEGER,
	rate_source VARCHAR(5),
	nem_system_size_limit_kw DOUBLE PRECISION,
	ur_nm_yearend_sell_rate NUMERIC,
	ur_flat_sell_rate NUMERIC,
	flat_rate_excess_gen_kwh NUMERIC,
	ur_enable_net_metering BOOLEAN,
	full_net_metering BOOLEAN,
	-- rate outputs
	excess_generation_percent NUMERIC,
	first_year_bill_with_system NUMERIC,
	first_year_bill_without_system NUMERIC,
	net_fit_credit_dollars NUMERIC,
	monthly_bill_savings NUMERIC,
	percent_monthly_bill_savings NUMERIC,
	cost_of_elec_dols_per_kwh NUMERIC,
	-- costs
	cap_cost_multiplier NUMERIC,
	inverter_cost_dollars_per_kw NUMERIC,
	installed_costs_dollars_per_kw NUMERIC,
	fixed_om_dollars_per_kw_per_yr NUMERIC,
	variable_om_dollars_per_kwh NUMERIC,
	carbon_price_cents_per_kwh NUMERIC,
	-- reeds stuff
	curtailment_rate NUMERIC,
	ReEDS_elec_price_mult NUMERIC,
	-- financial inputs
	business_model VARCHAR(10),
	leasing_allowed BOOLEAN,
	loan_term_yrs INTEGER,
	loan_rate NUMERIC,
	down_payment NUMERIC,
	discount_rate NUMERIC,
	tax_rate NUMERIC,
	length_of_irr_analysis_yrs INTEGER,
	-- incentives values
	value_of_increment NUMERIC,
	value_of_pbi_fit NUMERIC,
	value_of_ptc NUMERIC,
	pbi_fit_length INTEGER,
	ptc_length INTEGER,
	value_of_rebate  NUMERIC,
	value_of_tax_credit_or_deduction NUMERIC,
	value_of_itc NUMERIC,
	total_value_of_incentives NUMERIC,
	-- financial outputs
	lcoe NUMERIC,
	npv4 NUMERIC,
	npv_agent NUMERIC,
	metric TEXT,
	metric_value NUMERIC,
	max_market_share NUMERIC,
	-- initial year diffusion
	initial_number_of_adopters NUMERIC,
	initial_capacity_mw NUMERIC,
	initial_market_share NUMERIC,
	initial_market_value NUMERIC,
	-- previous year diffusion
	number_of_adopters_last_year NUMERIC,
	installed_capacity_last_year NUMERIC,
	market_share_last_year NUMERIC,
	market_value_last_year NUMERIC,
	-- current year diffusion
	number_of_adopters NUMERIC,
	installed_capacity NUMERIC,
	market_value NUMERIC,
	new_market_share NUMERIC
);

------------------------------------------------------------------------------------------------------------
-- tables to hold results from each previous model year 
DROP TABLE IF EXISTS diffusion_template.output_market_last_year;
CREATE TABLE diffusion_template.output_market_last_year
(
	county_id INTEGER,
	bin_id INTEGER,
	tech varchar(5),
	sector_abbr varchar(3),
	market_share_last_year NUMERIC,
	max_market_share_last_year NUMERIC,
	number_of_adopters_last_year NUMERIC,
	installed_capacity_last_year NUMERIC,
	market_value_last_year NUMERIC,
	initial_number_of_adopters NUMERIC,
	initial_capacity_mw NUMERIC,
	initial_market_share NUMERIC,
	initial_market_value NUMERIC

);

