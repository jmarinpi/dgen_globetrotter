set role 'diffusion-writers';

-- incentives (solar and wind)

DROP TABLE IF EXISTS diffusion_template.input_wind_incentive_utility_types;
CREATE TABLE diffusion_template.input_wind_incentive_utility_types
(
	utility_type_iou boolean NOT NULL,
	utility_type_muni boolean NOT NULL,
	utility_type_coop boolean NOT NULL,
	utility_type_allother boolean NOT NULL
);


DROP TABLE IF EXISTS diffusion_template.input_wind_incentive_options;
CREATE TABLE diffusion_template.input_wind_incentive_options
(
	overwrite_exist_inc boolean NOT NULL,
	incentive_start_year integer NOT NULL,
	CONSTRAINT input_wind_incentive_options_incentive_start_year_fkey FOREIGN KEY (incentive_start_year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_solar_incentive_utility_types;
CREATE TABLE diffusion_template.input_solar_incentive_utility_types
(
	utility_type_iou boolean NOT NULL,
	utility_type_muni boolean NOT NULL,
	utility_type_coop boolean NOT NULL,
	utility_type_allother boolean NOT NULL
);


DROP TABLE IF EXISTS diffusion_template.input_solar_incentive_options;
CREATE TABLE diffusion_template.input_solar_incentive_options
(
	overwrite_exist_inc boolean NOT NULL,
	incentive_start_year integer NOT NULL,
	CONSTRAINT input_solar_incentive_options_incentive_start_year_fkey FOREIGN KEY (incentive_start_year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON DELETE RESTRICT
);