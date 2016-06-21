set role 'diffusion-writers';

DROP TABLE IF EXISTs diffusion_template.input_baseline_performance_hvac_res_type_1 CASCADE;
CREATE TABLE diffusion_template.input_baseline_performance_hvac_res_type_1 
(
	year integer not null,
	efficiency_improvement_factor_heating numeric not null,
	efficiency_improvement_factor_cooling numeric not null,
	sys_lifetime_yrs numeric not null,
	perc_annual_sys_degradation numeric not null,
	CONSTRAINT input_baseline_performance_hvac_res_type_1_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_performance_hvac_res_type_2 CASCADE;
CREATE TABLE diffusion_template.input_baseline_performance_hvac_res_type_2
(
	year integer not null,
	efficiency_improvement_factor_heating numeric not null,
	efficiency_improvement_factor_cooling numeric not null,
	sys_lifetime_yrs numeric not null,
	perc_annual_sys_degradation numeric not null,
	CONSTRAINT input_baseline_performance_hvac_res_type_2_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_performance_hvac_res_type_3 CASCADE;
CREATE TABLE diffusion_template.input_baseline_performance_hvac_res_type_3
(
	year integer not null,
	efficiency_improvement_factor_heating numeric not null,
	efficiency_improvement_factor_cooling numeric not null,
	sys_lifetime_yrs numeric not null,
	perc_annual_sys_degradation numeric not null,
	CONSTRAINT input_baseline_performance_hvac_res_type_3_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_performance_hvac_res_type_4 CASCADE;
CREATE TABLE diffusion_template.input_baseline_performance_hvac_res_type_4
(
	year integer not null,
	efficiency_improvement_factor_heating numeric not null,
	efficiency_improvement_factor_cooling numeric not null,
	sys_lifetime_yrs numeric not null,
	perc_annual_sys_degradation numeric not null,
	CONSTRAINT input_baseline_performance_hvac_res_type_4_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

----
-- Comm


DROP TABLE IF EXISTs diffusion_template.input_baseline_performance_hvac_com_type_1 CASCADE;
CREATE TABLE diffusion_template.input_baseline_performance_hvac_com_type_1 
(
	year integer not null,
	efficiency_improvement_factor_heating numeric not null,
	efficiency_improvement_factor_cooling numeric not null,
	sys_lifetime_yrs numeric not null,
	perc_annual_sys_degradation numeric not null,
	CONSTRAINT input_baseline_performance_hvac_com_type_1_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_performance_hvac_com_type_2 CASCADE;
CREATE TABLE diffusion_template.input_baseline_performance_hvac_com_type_2
(
	year integer not null,
	efficiency_improvement_factor_heating numeric not null,
	efficiency_improvement_factor_cooling numeric not null,
	sys_lifetime_yrs numeric not null,
	perc_annual_sys_degradation numeric not null,
	CONSTRAINT input_baseline_performance_hvac_com_type_2_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_performance_hvac_com_type_3 CASCADE;
CREATE TABLE diffusion_template.input_baseline_performance_hvac_com_type_3
(
	year integer not null,
	efficiency_improvement_factor_heating numeric not null,
	efficiency_improvement_factor_cooling numeric not null,
	sys_lifetime_yrs numeric not null,
	perc_annual_sys_degradation numeric not null,
	CONSTRAINT input_baseline_performance_hvac_com_type_3_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_performance_hvac_com_type_4 CASCADE;
CREATE TABLE diffusion_template.input_baseline_performance_hvac_com_type_4
(
	year integer not null,
	efficiency_improvement_factor_heating numeric not null,
	efficiency_improvement_factor_cooling numeric not null,
	sys_lifetime_yrs numeric not null,
	perc_annual_sys_degradation numeric not null,
	CONSTRAINT input_baseline_performance_hvac_com_type_4_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

--- 
-- COnventional Systems

DROP TABLE IF EXISTs diffusion_template.input_baseline_performance_conventional_process_heat CASCADE;
CREATE TABLE diffusion_template.input_baseline_performance_conventional_process_heat
(
	year integer not null,
	efficiency_improvement_factor  numeric not null,
	sys_lifetime_yrs numeric not null,
	annual_sys_degradation numeric not null,
	CONSTRAINT input_baseline_performance_conventional_process_heat_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);
