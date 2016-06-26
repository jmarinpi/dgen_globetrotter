-- Vertical
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



--- Create views
DROP VIEW IF EXISTS diffusion_template.input_ghp_performance_improvements;
CREATE VIEW diffusion_template.input_ghp_performance_improvements AS 
(
	SELECT year, 'closed horizontal'::text as sys_config, 
	efficiency_improvement_factor_cooling, efficiency_improvement_factor_cop,
	efficiency_improvement_factor_water_heating, heat_pump_lifetime_yrs
	FROM diffusion_template.input_ghp_performance_improvements_horizontal 
	UNION ALL
	SELECT year, 'closed vertical'::text as sys_config, 
	efficiency_improvement_factor_cooling, efficiency_improvement_factor_cop,
	efficiency_improvement_factor_water_heating, heat_pump_lifetime_yrs
	FROM diffusion_template.input_ghp_performance_improvements_vertical
);

DROP VIEW IF EXISTS diffusion_template.input_ghp_performance_degradation;
CREATE VIEW diffusion_template.input_ghp_performance_degradation AS 
(
	-- horizontal res
	SELECT year, 'res'::char varying(3) as sector, 'closed horizontal'::text as sys_config, 
	'climate zone 1'::char varying(14) as climate_zone, res_perc_annual_system_degradation_climate_zone_1
	FROM diffusion_template.input_ghp_performance_degradation_horizontal
	UNION ALL
	SELECT year, 'res'::char varying(3) as sector, 'closed horizontal'::text as sys_config, 
	'climate zone 2'::char varying(14) as climate_zone, res_perc_annual_system_degradation_climate_zone_2
	FROM diffusion_template.input_ghp_performance_degradation_horizontal
	UNION ALL
	SELECT year, 'res'::char varying(3) as sector, 'closed horizontal'::text as sys_config, 
	'climate zone 3'::char varying(14) as climate_zone, res_perc_annual_system_degradation_climate_zone_3
	FROM diffusion_template.input_ghp_performance_degradation_horizontal
	UNION ALL
	SELECT year, 'res'::char varying(3) as sector, 'closed horizontal'::text as sys_config, 
	'climate zone 4'::char varying(14) as climate_zone, res_perc_annual_system_degradation_climate_zone_4
	FROM diffusion_template.input_ghp_performance_degradation_horizontal
	UNION ALL
	SELECT year, 'res'::char varying(3) as sector, 'closed horizontal'::text as sys_config, 
	'climate zone 5'::char varying(14) as climate_zone, res_perc_annual_system_degradation_climate_zone_5
	FROM diffusion_template.input_ghp_performance_degradation_horizontal
	UNION ALL
	SELECT year, 'res'::char varying(3) as sector, 'closed horizontal'::text as sys_config, 
	'climate zone 6'::char varying(14) as climate_zone, res_perc_annual_system_degradation_climate_zone_6
	FROM diffusion_template.input_ghp_performance_degradation_horizontal
	UNION ALL
	SELECT year, 'res'::char varying(3) as sector, 'closed horizontal'::text as sys_config, 
	'climate zone 7'::char varying(14) as climate_zone, res_perc_annual_system_degradation_climate_zone_7
	FROM diffusion_template.input_ghp_performance_degradation_horizontal
	
	-- horizontal comm
	UNION ALL
	SELECT year, 'com'::char varying(3) as sector, 'closed horizontal'::text as sys_config, 
	'climate zone 1'::char varying(14) as climate_zone, com_perc_annual_system_degradation_climate_zone_1
	FROM diffusion_template.input_ghp_performance_degradation_horizontal
	UNION ALL
	SELECT year, 'com'::char varying(3) as sector, 'closed horizontal'::text as sys_config, 
	'climate zone 2'::char varying(14) as climate_zone, com_perc_annual_system_degradation_climate_zone_2
	FROM diffusion_template.input_ghp_performance_degradation_horizontal
	UNION ALL
	SELECT year, 'com'::char varying(3) as sector, 'closed horizontal'::text as sys_config, 
	'climate zone 3'::char varying(14) as climate_zone, com_perc_annual_system_degradation_climate_zone_3
	FROM diffusion_template.input_ghp_performance_degradation_horizontal
	UNION ALL
	SELECT year, 'com'::char varying(3) as sector, 'closed horizontal'::text as sys_config, 
	'climate zone 4'::char varying(14) as climate_zone, com_perc_annual_system_degradation_climate_zone_4
	FROM diffusion_template.input_ghp_performance_degradation_horizontal
	UNION ALL
	SELECT year, 'com'::char varying(3) as sector, 'closed horizontal'::text as sys_config, 
	'climate zone 5'::char varying(14) as climate_zone, com_perc_annual_system_degradation_climate_zone_5
	FROM diffusion_template.input_ghp_performance_degradation_horizontal
	UNION ALL
	SELECT year, 'com'::char varying(3) as sector, 'closed horizontal'::text as sys_config, 
	'climate zone 6'::char varying(14) as climate_zone, com_perc_annual_system_degradation_climate_zone_6
	FROM diffusion_template.input_ghp_performance_degradation_horizontal
	UNION ALL
	SELECT year, 'com'::char varying(3) as sector, 'closed horizontal'::text as sys_config, 
	'climate zone 7'::char varying(14) as climate_zone, com_perc_annual_system_degradation_climate_zone_7
	FROM diffusion_template.input_ghp_performance_degradation_horizontal

	-- vertical res
	UNION ALL
	SELECT year, 'res'::char varying(3) as sector, 'closed vertical'::text as sys_config, 
	'climate zone 1'::char varying(14) as climate_zone, res_perc_annual_system_degradation_climate_zone_1
	FROM diffusion_template.input_ghp_performance_degradation_vertical
	UNION ALL
	SELECT year, 'res'::char varying(3) as sector, 'closed vertical'::text as sys_config, 
	'climate zone 2'::char varying(14) as climate_zone, res_perc_annual_system_degradation_climate_zone_2
	FROM diffusion_template.input_ghp_performance_degradation_vertical
	UNION ALL
	SELECT year, 'res'::char varying(3) as sector, 'closed vertical'::text as sys_config, 
	'climate zone 3'::char varying(14) as climate_zone, res_perc_annual_system_degradation_climate_zone_3
	FROM diffusion_template.input_ghp_performance_degradation_vertical
	UNION ALL
	SELECT year, 'res'::char varying(3) as sector, 'closed vertical'::text as sys_config, 
	'climate zone 4'::char varying(14) as climate_zone, res_perc_annual_system_degradation_climate_zone_4
	FROM diffusion_template.input_ghp_performance_degradation_vertical
	UNION ALL
	SELECT year, 'res'::char varying(3) as sector, 'closed vertical'::text as sys_config, 
	'climate zone 5'::char varying(14) as climate_zone, res_perc_annual_system_degradation_climate_zone_5
	FROM diffusion_template.input_ghp_performance_degradation_vertical
	UNION ALL
	SELECT year, 'res'::char varying(3) as sector, 'closed vertical'::text as sys_config, 
	'climate zone 6'::char varying(14) as climate_zone, res_perc_annual_system_degradation_climate_zone_6
	FROM diffusion_template.input_ghp_performance_degradation_vertical
	UNION ALL
	SELECT year, 'res'::char varying(3) as sector, 'closed vertical'::text as sys_config, 
	'climate zone 7'::char varying(14) as climate_zone, res_perc_annual_system_degradation_climate_zone_7
	FROM diffusion_template.input_ghp_performance_degradation_vertical

	-- vertical comm
	UNION ALL
	SELECT year, 'com'::char varying(3) as sector, 'closed vertical'::text as sys_config, 
	'climate zone 1'::char varying(14) as climate_zone, com_perc_annual_system_degradation_climate_zone_1
	FROM diffusion_template.input_ghp_performance_degradation_vertical
	UNION ALL
	SELECT year, 'com'::char varying(3) as sector, 'closed vertical'::text as sys_config, 
	'climate zone 2'::char varying(14) as climate_zone, com_perc_annual_system_degradation_climate_zone_2
	FROM diffusion_template.input_ghp_performance_degradation_vertical
	UNION ALL
	SELECT year, 'com'::char varying(3) as sector, 'closed vertical'::text as sys_config, 
	'climate zone 3'::char varying(14) as climate_zone, com_perc_annual_system_degradation_climate_zone_3
	FROM diffusion_template.input_ghp_performance_degradation_vertical
	UNION ALL
	SELECT year, 'com'::char varying(3) as sector, 'closed vertical'::text as sys_config, 
	'climate zone 4'::char varying(14) as climate_zone, com_perc_annual_system_degradation_climate_zone_4
	FROM diffusion_template.input_ghp_performance_degradation_vertical
	UNION ALL
	SELECT year, 'com'::char varying(3) as sector, 'closed vertical'::text as sys_config, 
	'climate zone 5'::char varying(14) as climate_zone, com_perc_annual_system_degradation_climate_zone_5
	FROM diffusion_template.input_ghp_performance_degradation_vertical
	UNION ALL
	SELECT year, 'com'::char varying(3) as sector, 'closed vertical'::text as sys_config, 
	'climate zone 6'::char varying(14) as climate_zone, com_perc_annual_system_degradation_climate_zone_6
	FROM diffusion_template.input_ghp_performance_degradation_vertical
	UNION ALL
	SELECT year, 'com'::char varying(3) as sector, 'closed vertical'::text as sys_config, 
	'climate zone 7'::char varying(14) as climate_zone, com_perc_annual_system_degradation_climate_zone_7
	FROM diffusion_template.input_ghp_performance_degradation_vertical
);

