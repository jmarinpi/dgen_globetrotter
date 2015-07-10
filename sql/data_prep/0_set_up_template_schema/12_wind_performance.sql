set role 'diffusion-writers';


DROP TABLE IF EXISTS diffusion_template.input_wind_performance_improvements;
CREATE TABLE diffusion_template.input_wind_performance_improvements
(
	turbine_size_kw numeric not null,
	year integer not null,
	power_curve_id integer not null,
	CONSTRAINT input_wind_performance_improvements_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT,
	CONSTRAINT input_wind_performance_improvements_power_curve_id_fkey FOREIGN KEY (power_curve_id)
		REFERENCES diffusion_config.sceninp_power_curve_ids (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_wind_performance_gen_derate_factors;
CREATE TABLE diffusion_template.input_wind_performance_gen_derate_factors
(
	turbine_size_kw numeric not null,
	year integer not null,
	derate_factor numeric not null,
	CONSTRAINT input_wind_performance_gen_derate_factors_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);