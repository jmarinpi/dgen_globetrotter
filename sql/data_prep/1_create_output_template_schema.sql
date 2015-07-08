set role 'server-superusers';
SELECT add_schema('diffusion_template', 'diffusion');

set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_template.input_main_scenario_options;
CREATE TABLE diffusion_template.input_main_scenario_options
(
	scenario_name text NOT NULL,
	region text NOT NULL,
	end_year integer NOT NULL,
	markets text NOT NULL,
	load_growth_scenario text NOT NULL,
	res_rate_structure text NOT NULL,
	res_rate_escalation text NOT NULL,
	res_max_market_curve text NOT NULL,
	com_rate_structure text NOT NULL,
	com_rate_escalation text NOT NULL,
	com_max_market_curve text NOT NULL,
	ind_rate_structure text NOT NULL,
	ind_rate_escalation text NOT NULL,
	ind_max_market_curve text NOT NULL,
	carbon_price text NOT NULL,
	random_generator_seed integer NOT NULL, -- doesn't need a constraint -- just needs to be integer
	-- add check/fkey constraints to ensure only valid values are entered in each column
	-- carbon price
	CONSTRAINT input_main_scenario_options_carbon_price_fkey FOREIGN KEY (carbon_price)
		REFERENCES diffusion_config.sceninp_carbon_price (val) MATCH SIMPLE
		ON DELETE RESTRICT,
	-- max market curves
	CONSTRAINT input_main_scenario_options_com_max_market_curve_fkey FOREIGN KEY (com_max_market_curve)
		REFERENCES diffusion_config.sceninp_max_market_curve (val) MATCH SIMPLE
		ON DELETE RESTRICT,
	CONSTRAINT input_main_scenario_options_res_max_market_curve_fkey FOREIGN KEY (res_max_market_curve)
		REFERENCES diffusion_config.sceninp_max_market_curve (val) MATCH SIMPLE
		ON DELETE RESTRICT,
	CONSTRAINT input_main_scenario_options_ind_max_market_curve_fkey FOREIGN KEY (ind_max_market_curve)
		REFERENCES diffusion_config.sceninp_max_market_curve (val) MATCH SIMPLE
		ON DELETE RESTRICT,
	-- rate escalations
	CONSTRAINT input_main_scenario_options_com_rate_escalation_fkey FOREIGN KEY (com_rate_escalation)
		REFERENCES diffusion_config.sceninp_rate_escalation (val) MATCH SIMPLE
		ON DELETE RESTRICT,
	CONSTRAINT input_main_scenario_options_ind_rate_escalation_fkey FOREIGN KEY (ind_rate_escalation)
		REFERENCES diffusion_config.sceninp_rate_escalation (val) MATCH SIMPLE
		ON DELETE RESTRICT,
	CONSTRAINT input_main_scenario_options_res_rate_escalation_fkey FOREIGN KEY (res_rate_escalation)
		REFERENCES diffusion_config.sceninp_rate_escalation (val) MATCH SIMPLE
		ON DELETE RESTRICT,
	-- rate structures
	CONSTRAINT input_main_scenario_options_com_rate_structure_fkey FOREIGN KEY (com_rate_structure)
		REFERENCES diffusion_config.sceninp_rate_structure (val) MATCH SIMPLE
		ON DELETE RESTRICT,
	CONSTRAINT input_main_scenario_options_ind_rate_structure_fkey FOREIGN KEY (ind_rate_structure)
		REFERENCES diffusion_config.sceninp_rate_structure (val) MATCH SIMPLE
		ON DELETE RESTRICT,
	CONSTRAINT input_main_scenario_options_res_rate_structure_fkey FOREIGN KEY (res_rate_structure)
		REFERENCES diffusion_config.sceninp_rate_structure (val) MATCH SIMPLE
		ON DELETE RESTRICT,
	-- end year
	CONSTRAINT input_main_scenario_options_end_year_fkey FOREIGN KEY (end_year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON DELETE RESTRICT,
	-- load growth
	CONSTRAINT input_main_scenario_options_load_growth_scenario_fkey FOREIGN KEY (load_growth_scenario)
		REFERENCES diffusion_config.sceninp_load_growth_scenario (val) MATCH SIMPLE
		ON DELETE RESTRICT,
	-- markets (i.e., sectors)
	CONSTRAINT input_main_scenario_options_markets_fkey FOREIGN KEY (markets)
		REFERENCES diffusion_config.sceninp_markets (val) MATCH SIMPLE
		ON DELETE RESTRICT,
	-- region
	CONSTRAINT input_main_scenario_options_region_fkey FOREIGN KEY (region)
		REFERENCES diffusion_config.sceninp_region (val) MATCH SIMPLE
		ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_main_inflation;
CREATE TABLE diffusion_template.input_main_inflation
(
	ann_inflation numeric NOT NULL
);


DROP TABLE IF EXISTS diffusion_template.input_wind_incentive_options;
CREATE TABLE diffusion_template.input_wind_incentive_options
(
	overwrite_exist_inc boolean NOT NULL,
	incentive_start_year integer NOT NULL,
	utility_type_iou boolean NOT NULL,
	utility_type_muni boolean NOT NULL,
	utility_type_coop boolean NOT NULL,
	utility_type_allother boolean NOT NULL,
	CONSTRAINT input_wind_incentive_options_incentive_start_year_fkey FOREIGN KEY (incentive_start_year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_solar_incentive_options;
CREATE TABLE diffusion_template.input_solar_incentive_options
(
	overwrite_exist_inc boolean NOT NULL,
	incentive_start_year integer NOT NULL,
	utility_type_iou boolean NOT NULL,
	utility_type_muni boolean NOT NULL,
	utility_type_coop boolean NOT NULL,
	utility_type_allother boolean NOT NULL,
	CONSTRAINT input_solar_incentive_options_incentive_start_year_fkey FOREIGN KEY (incentive_start_year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_wind_siting_options;
CREATE TABLE diffusion_template.input_wind_siting_options
(	
	parcel_size_enabled boolean NOT NULL,
	hi_dev_enabled boolean NOT NULL,
	canopy_clearance_enabled boolean NOT NULL
);


DROP TABLE IF EXISTS diffusion_template.input_solar_cost_and_degradation_options;
CREATE TABLE diffusion_template.input_solar_cost_and_degradation_options
(
	cost_assumptions text NOT NULL,
	ann_system_degradation numeric NOT NULL,
	CONSTRAINT input_solar_cost_and_degradation_options_cost_assumptions FOREIGN KEY (cost_assumptions)
		REFERENCES diffusion_config.sceninp_cost_assumptions_solar (val) MATCH SIMPLE
		ON DELETE RESTRICT
);




