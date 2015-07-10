set role 'diffusion-writers';


DROP TABLE IF EXIStS diffusion_template.input_solar_performance_improvements;
CREATE TABLE diffusion_template.input_solar_performance_improvements
(
	year integer NOT NULL,
	efficiency_improvement_factor numeric NOT NULL,
	density_w_per_sqft numeric NOT NULL,
	inverter_lifetime_yrs integer NOT NULL,
	CONSTRAINT input_solar_performance_improvements_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_solar_performance_annual_system_degradation;
CREATE TABLE diffusion_template.input_solar_performance_annual_system_degradation
(
	ann_system_degradation numeric not null
);
