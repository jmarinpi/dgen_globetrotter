DROP TABLE IF EXISTs diffusion_template.input_ghp_performance_improvements_vertical CASCADE;
CREATE TABLE diffusion_template.input_ghp_performance_improvements_vertical
(
	year integer NOT NULL,
	efficiency_improvement_factor_cooling numeric NOT NULL,
	efficiency_improvement_factor_cop numeric NOT NULL,
	efficiency_improvement_factor_water_heating numeric NOT NULL,
	heat_pump_lifetime_yrs numeric NOT NULL,
	CONSTRAINT input_ghp_performance_improvements_vertical_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_ghp_performance_degradation_vertical CASCADE;
CREATE TABLE diffusion_template.input_ghp_performance_degradation_vertical
(
	year integer NOT NULL,
	res_perc_annual_system_degradation_climate_zone_1 numeric NOT NULL,
	res_perc_annual_system_degradation_climate_zone_2 numeric NOT NULL,
	res_perc_annual_system_degradation_climate_zone_3 numeric NOT NULL,
	res_perc_annual_system_degradation_climate_zone_4 numeric NOT NULL,
	res_perc_annual_system_degradation_climate_zone_5 numeric NOT NULL,
	res_perc_annual_system_degradation_climate_zone_6 numeric NOT NULL,
	res_perc_annual_system_degradation_climate_zone_7 numeric NOT NULL,
	com_perc_annual_system_degradation_climate_zone_1 numeric NOT NULL,
	com_perc_annual_system_degradation_climate_zone_2 numeric NOT NULL,
	com_perc_annual_system_degradation_climate_zone_3 numeric NOT NULL,
	com_perc_annual_system_degradation_climate_zone_4 numeric NOT NULL,
	com_perc_annual_system_degradation_climate_zone_5 numeric NOT NULL,
	com_perc_annual_system_degradation_climate_zone_6 numeric NOT NULL,
	com_perc_annual_system_degradation_climate_zone_7 numeric NOT NULL,
	CONSTRAINT input_ghp_performance_degradation_vertical_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


-- Horizontal
DROP TABLE IF EXISTs diffusion_template.input_ghp_performance_improvements_horizontal CASCADE;

CREATE TABLE diffusion_template.input_ghp_performance_improvements_horizontal
(
	year integer NOT NULL,
	efficiency_improvement_factor_cooling numeric NOT NULL,
	efficiency_improvement_factor_cop numeric NOT NULL,
	efficiency_improvement_factor_water_heating numeric NOT NULL,
	heat_pump_lifetime_yrs numeric NOT NULL,
	CONSTRAINT input_ghp_performance_horizontal_improvements_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_ghp_performance_degradation_horizontal CASCADE;
CREATE TABLE diffusion_template.input_ghp_performance_degradation_horizontal
(
	year integer NOT NULL,
	res_perc_annual_system_degradation_climate_zone_1 numeric NOT NULL,
	res_perc_annual_system_degradation_climate_zone_2 numeric NOT NULL,
	res_perc_annual_system_degradation_climate_zone_3 numeric NOT NULL,
	res_perc_annual_system_degradation_climate_zone_4 numeric NOT NULL,
	res_perc_annual_system_degradation_climate_zone_5 numeric NOT NULL,
	res_perc_annual_system_degradation_climate_zone_6 numeric NOT NULL,
	res_perc_annual_system_degradation_climate_zone_7 numeric NOT NULL,
	com_perc_annual_system_degradation_climate_zone_1 numeric NOT NULL,
	com_perc_annual_system_degradation_climate_zone_2 numeric NOT NULL,
	com_perc_annual_system_degradation_climate_zone_3 numeric NOT NULL,
	com_perc_annual_system_degradation_climate_zone_4 numeric NOT NULL,
	com_perc_annual_system_degradation_climate_zone_5 numeric NOT NULL,
	com_perc_annual_system_degradation_climate_zone_6 numeric NOT NULL,
	com_perc_annual_system_degradation_climate_zone_7 numeric NOT NULL,
	CONSTRAINT input_ghp_performance_degradation_horizontal_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);