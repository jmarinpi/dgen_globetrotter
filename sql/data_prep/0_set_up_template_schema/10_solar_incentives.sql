set role 'diffusion-writers';

DROP TABLE IF EXISTs diffusion_template.input_solar_incentive_options;
CREATE TABLE diffusion_template.input_solar_incentive_options
(
	overwrite_exist_inc boolean not null,
	incentive_start_year integer not null,
	CONSTRAINT input_solar_incentive_options_year_fkey FOREIGN KEY (incentive_start_year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_solar_incentive_utility_types CASCADE;
CREATE TABLE diffusion_template.input_solar_incentive_utility_types 
(
	utility_type_iou boolean NOT NULL,
	utility_type_muni boolean NOT NULL,
	utility_type_coop boolean NOT NULL,
	utility_type_allother boolean NOT NULL
);

