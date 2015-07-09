set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_template.input_main_nem_utility_types;
CREATE TABLE diffusion_template.input_main_nem_utility_types 
(
	utility_type_iou boolean NOT NULL,
	utility_type_muni boolean NOT NULL,
	utility_type_coop boolean NOT NULL,
	utility_type_allother boolean NOT NULL
);


DROP TABLE IF EXISTS diffusion_template.input_main_nem_selected_scenario;
CREATE TABLE diffusion_template.input_main_nem_selected_scenario 
(
	val text not null,
	CONSTRAINT input_main_nem_selected_scenario_fkey FOREIGN KEY (val)
		REFERENCES diffusion_config.sceninp_nem_scenario (val) MATCH SIMPLE
		ON DELETE RESTRICT
);


DROP VIEW IF EXISTS diffusion_template.input_main_nem_avoided_costs;
CREATE VIEW diffusion_template.input_main_nem_avoided_costs AS
SELECT 	a.year, 
	b.state_abbr,
	unnest(array['res','com','ind']) as sector_abbr,
	unnest(array['All Other', 'Coop', 'IOU', 'Muni']) as utility_type,
	0::double precision as system_size_limit_kw,
	0::numeric as year_end_excess_sell_rate_dlrs_per_kwh,
	a.avoided_costs_dollars_per_kwh as hourly_excess_sell_rate_dlrs_per_kwh
FROM diffusion_template.input_main_market_projections a
CROSS JOIN diffusion_shared.state_fips_lkup b
WHERE b.state_abbr <> 'PR';


DROP TABLE IF EXISTS diffusion_template.input_main_nem_user_defined_scenario;
CREATE TABLE diffusion_template.input_main_nem_user_defined_scenario
(
	state_abbr character varying(2),
	
	system_size_limit_kw_res double precision,
	state_cap_limit_mw_res numeric,
	year_end_excess_sell_rate_dlrs_per_kwh_res numeric,
	hourly_excess_sell_rate_dlrs_per_kwh_res numeric,

	system_size_limit_kw_com double precision,
	state_cap_limit_mw_com numeric,
	year_end_excess_sell_rate_dlrs_per_kwh_com numeric,
	hourly_excess_sell_rate_dlrs_per_kwh_com numeric,

	system_size_limit_kw_ind double precision,
	state_cap_limit_mw_ind numeric,
	year_end_excess_sell_rate_dlrs_per_kwh_ind numeric,
	hourly_excess_sell_rate_dlrs_per_kwh_ind numeric,

	first_year integer,
	last_year integer,

	CONSTRAINT input_main_nem_user_defined_scenario_first_year_fkey FOREIGN KEY (first_year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON DELETE RESTRICT,

	CONSTRAINT input_main_nem_user_defined_scenario_last_year_fkey FOREIGN KEY (last_year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON DELETE RESTRICT

);
