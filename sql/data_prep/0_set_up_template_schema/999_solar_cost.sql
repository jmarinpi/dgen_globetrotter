set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_template.input_solar_cost_and_degradation_options;
CREATE TABLE diffusion_template.input_solar_cost_and_degradation_options
(
	cost_assumptions text NOT NULL,
	ann_system_degradation numeric NOT NULL,
	CONSTRAINT input_solar_cost_and_degradation_options_cost_assumptions FOREIGN KEY (cost_assumptions)
		REFERENCES diffusion_config.sceninp_cost_assumptions_solar (val) MATCH SIMPLE
		ON DELETE RESTRICT
);

