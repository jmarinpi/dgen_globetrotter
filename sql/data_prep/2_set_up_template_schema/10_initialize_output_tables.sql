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
	sector text,
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
	naep numeric,
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
	-- incremental diffusion from current year
	new_adopters NUMERIc,
	new_capacity NUMERIc,
	new_market_share NUMERIc,
	new_market_value NUMERIc,
	-- cumulative diffusion through current year
	number_of_adopters NUMERIC,
	installed_capacity NUMERIC,
	market_share numeric,
	market_value NUMERIC
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

set role 'diffusion-writers';
-- tables to hold results from each previous model year 
DROP TABLE IF EXISTS diffusion_template.output_market_summary_du;
CREATE TABLE diffusion_template.output_market_summary_du
(
	year integer,
	cumulative_market_share_pct numeric,
	cumulative_market_share_mw numeric,
	new_incremental_market_share_pct numeric,
	new_incremental_capacity_mw numeric
);

-- tables to hold results from each previous model year 
DROP TABLE IF EXISTS diffusion_template.output_market_summary_ghp;
CREATE TABLE diffusion_template.output_market_summary_ghp
(
	year integer,
	state_abbr varchar(2),
	sector_abbr varchar(3),
	cumulative_market_share_pct numeric,
	cumulative_market_share_tons numeric,
	new_incremental_market_share_pct numeric,
	new_incremental_capacity_tons numeric
);



set role 'diffusion-writers';
-- tables to hold results from each previous model year 
DROP TABLE IF EXISTS diffusion_template.agent_outputs_du;
CREATE TABLE diffusion_template.agent_outputs_du
(
	agent_id bigint NOT NULL,
	year integer,
	pgid bigint,
	county_id integer,
	state_abbr character varying(2),
	state_fips character varying(2),
	county_fips character varying(3),
	tract_fips character varying(6),
	tract_id_alias integer,
	old_county_id integer,
	census_division_abbr text,
	census_region text,
	reportable_domain integer,
	pca_reg text,
	reeds_reg integer,
	acres_per_bldg numeric,
	hdf_load_index integer,
	hazus_bldg_type text,
	buildings_in_bin numeric,
	space_heat_kwh_in_bin numeric,
	space_cool_kwh_in_bin numeric,
	water_heat_kwh_in_bin numeric,
	total_heat_kwh_in_bin numeric,
	space_heat_kwh_per_building_in_bin numeric,
	space_cool_kwh_per_building_in_bin numeric,
	water_heat_kwh_per_building_in_bin numeric,
	total_heat_kwh_per_building_in_bin numeric,
	space_heat_system_age NUMERIC,
	space_cool_system_age NUMERIC,
	average_system_age NUMERIC,
	space_heat_system_expected_lifetime NUMERIC,
	space_cool_system_expected_lifetime NUMERIC,
	average_system_expected_lifetime NUMERIC,
	baseline_system_type NUMERIC,
	eia_bldg_id integer,
	eia_bldg_weight double precision,
	climate_zone integer,
	pba NUMERIC,
	pbaplus NUMERIC,
	typehuq NUMERIC,
	owner_occupied boolean,
	year_built integer,
	single_family_res boolean,
	num_tenants integer,
	num_floors integer,
	space_heat_equip text,
	space_heat_fuel text,
	water_heat_equip text,
	water_heat_fuel text,
	space_cool_equip text,
	space_cool_fuel text,
	totsqft numeric,
	totsqft_heat numeric,
	totsqft_cool numeric,
	crb_model text,
	gtc_btu_per_hftf numeric,
	sector_abbr varchar(3),
	sector text,
	tech varchar(5),
	new_construction boolean,
	space_heat_dlrs_per_kwh numeric,
	water_heat_dlrs_per_kwh numeric,
	space_cool_dlrs_per_kwh numeric,
	sys_connection_cost_dollars numeric,
	fixed_om_costs_dollars_sf_yr numeric,
	new_sys_installation_costs_dollars_sf numeric,
	retrofit_new_sys_installation_cost_multiplier numeric,
	needs_replacement_heat_system boolean,
	needs_replacement_cool_system boolean,
	needs_replacement_average_system boolean,
	total_heat_mwh_per_building_in_bin numeric,
	weighted_cost_of_energy_dlrs_per_mwh numeric,
	system_installation_costs_dlrs numeric,
	upfront_costs_dlrs numeric,
	levelized_upfront_costs_dlrs_per_yr numeric,
	fixed_om_costs_dollars_per_yr numeric,
	annual_costs_dlrs_per_mwh double precision,
	lcoe_dlrs_mwh numeric,
	new_adopters numeric
);


set role 'diffusion-writers';
DROP TABLE IF EXISTS diffusion_template.resource_outputs_du;
CREATE TABLE diffusion_template.resource_outputs_du
(
	year integer,
	tract_id_alias INTEGER,
	resource_uid INTEGER,
	resource_type TEXT,
	depth_m NUMERIC,
	system_type TEXT,
	n_wellsets_in_tract INTEGER,
	lifetime_resource_per_wellset_mwh NUMERIC,
	total_consumable_energy_per_wellset_mwh NUMERIC,
	plant_nameplate_capacity_per_wellset_mw NUMERIC,
	plant_effective_capacity_per_wellset_mw NUMERIC,
	peaking_boilers_nameplate_capacity_per_wellset_mw NUMERIC,
	peaking_boilers_effective_capacity_per_wellset_mw NUMERIC,
	total_effective_capacity_per_wellset_mw NUMERIC,
	total_nameplate_capacity_per_wellset_mw NUMERIC,
	upfront_costs_per_wellset_dlrs NUMERIC,
	avg_annual_costs_per_wellset_dlrs NUMERIC,
	
	plant_installation_costs_per_wellset_dlrs NUMERIC,
	exploration_total_costs_per_wellset_dlrs NUMERIC,
	drilling_cost_per_wellset_dlrs NUMERIC,
	reservoir_stimulation_costs_per_wellset_dlrs NUMERIC,
	distribution_network_construction_costs_per_wellset_dlrs NUMERIC,
	distribution_m_per_wellset NUMERIC,
	peaking_boilers_construction_cost_per_wellset_dlrs NUMERIC,
	reservoir_pumping_gallons_per_year NUMERIC,
	operating_costs_reservoir_pumping_costs_per_wellset_per_year_dlrs NUMERIC,
	distribution_pumping_gallons_per_year NUMERIC,
	operating_costs_distribution_pumping_costs_per_wellset_per_year_dlrs NUMERIC,
	om_labor_costs_per_wellset_per_year_dlrs NUMERIC,
	om_plant_costs_per_wellset_per_year_dlrs NUMERIC,
	om_well_costs_per_wellset_per_year_dlrs NUMERIC,
	peaking_boilers_mwh_per_year_per_wellset NUMERIC,
	avg_peaking_boilers_fuel_costs_per_wellset_dlrs NUMERIC,

	plant_capacity_factor NUMERIC,
	peaking_boiler_capacity_factor NUMERIC,
	total_blended_capacity_factor NUMERIC,
	inflation_rate NUMERIC,
	interest_rate_nominal NUMERIC,
	interest_rate_during_construction_nominal NUMERIC,
	rate_of_return_on_equity NUMERIC,
	debt_fraction NUMERIC,
	tax_rate NUMERIC,
	construction_period_yrs INTEGER,
	plant_lifetime_yrs INTEGER,
	depreciation_period INTEGER,
	lcoe_dlrs_mwh NUMERIC,
	subscribed_wellsets NUMERIC
);