set role 'diffusion-writers';

DROP TABLE IF EXISTs diffusion_template.input_baseline_performance_hvac_res_type_1 CASCADE;
CREATE TABLE diffusion_template.input_baseline_performance_hvac_res_type_1 
(
	year integer not null,
	efficiency_improvement_factor numeric not null,
	system_lifetime_yrs numeric not null,
	annual_degradation_pct numeric not null,
	CONSTRAINT input_baseline_performance_hvac_res_type_1_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_performance_hvac_res_type_2 CASCADE;
CREATE TABLE diffusion_template.input_baseline_performance_hvac_res_type_2
(
	year integer not null,
	efficiency_improvement_factor numeric not null,
	system_lifetime_yrs numeric not null,
	annual_degradation_pct numeric not null,

	CONSTRAINT input_baseline_performance_hvac_res_type_2_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_performance_hvac_res_type_3 CASCADE;
CREATE TABLE diffusion_template.input_baseline_performance_hvac_res_type_3
(
	year integer not null,
	efficiency_improvement_factor numeric not null,
	system_lifetime_yrs numeric not null,
	annual_degradation_pct numeric not null,
	CONSTRAINT input_baseline_performance_hvac_res_type_3_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_performance_hvac_res_type_4 CASCADE;
CREATE TABLE diffusion_template.input_baseline_performance_hvac_res_type_4
(
	year integer not null,
	efficiency_improvement_factor numeric not null,
	system_lifetime_yrs numeric not null,
	annual_degradation_pct numeric not null,
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
	efficiency_improvement_factor numeric not null,
	system_lifetime_yrs numeric not null,
	annual_degradation_pct numeric not null,
	CONSTRAINT input_baseline_performance_hvac_com_type_1_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_performance_hvac_com_type_2 CASCADE;
CREATE TABLE diffusion_template.input_baseline_performance_hvac_com_type_2
(
	year integer not null,
	efficiency_improvement_factor numeric not null,
	system_lifetime_yrs numeric not null,
	annual_degradation_pct numeric not null,
	CONSTRAINT input_baseline_performance_hvac_com_type_2_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_performance_hvac_com_type_3 CASCADE;
CREATE TABLE diffusion_template.input_baseline_performance_hvac_com_type_3
(
	year integer not null,
	efficiency_improvement_factor numeric not null,
	system_lifetime_yrs numeric not null,
	annual_degradation_pct numeric not null,
	CONSTRAINT input_baseline_performance_hvac_com_type_3_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_performance_hvac_com_type_4 CASCADE;
CREATE TABLE diffusion_template.input_baseline_performance_hvac_com_type_4
(
	year integer not null,
	efficiency_improvement_factor numeric not null,
	system_lifetime_yrs numeric not null,
	annual_degradation_pct numeric not null,
	CONSTRAINT input_baseline_performance_hvac_com_type_4_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);



----------
-- Create views

DROP VIEW IF EXISTS diffusion_template.input_baseline_performance_hvac;
CREATE VIEW diffusion_template.input_baseline_performance_hvac AS 
(
	--res
	SELECT year, 
		'res'::char varying(3) as sector_abbr, 
		1::INTEGER as baseline_system_type,
		efficiency_improvement_factor,
		system_lifetime_yrs,
		annual_degradation_pct
	FROM diffusion_template.input_baseline_performance_hvac_res_type_1
	UNION ALL
	SELECT year, 
		'res'::char varying(3) as sector_abbr, 
		2::INTEGER as baseline_system_type,
		efficiency_improvement_factor,
		system_lifetime_yrs,
		annual_degradation_pct
	FROM diffusion_template.input_baseline_performance_hvac_res_type_2
	UNION ALL
	SELECT year, 
		'res'::char varying(3) as sector_abbr, 
		3::INTEGER as baseline_system_type,
		efficiency_improvement_factor,
		system_lifetime_yrs,
		annual_degradation_pct
	FROM diffusion_template.input_baseline_performance_hvac_res_type_3
	UNION ALL
	SELECT year, 
		'res'::char varying(3) as sector_abbr, 
		4::INTEGER as baseline_system_type,
		efficiency_improvement_factor,
		system_lifetime_yrs,
		annual_degradation_pct
	FROM diffusion_template.input_baseline_performance_hvac_res_type_4
	UNION ALL
	--com
	SELECT year, 
		'com'::char varying(3) as sector_abbr, 
		1::INTEGER as baseline_system_type,
		efficiency_improvement_factor,
		system_lifetime_yrs,
		annual_degradation_pct
	FROM diffusion_template.input_baseline_performance_hvac_com_type_1
	UNION ALL
	SELECT year, 
		'com'::char varying(3) as sector_abbr, 
		2::INTEGER as baseline_system_type,
		efficiency_improvement_factor,
		system_lifetime_yrs,
		annual_degradation_pct
	FROM diffusion_template.input_baseline_performance_hvac_com_type_2
	UNION ALL
	SELECT year, 
		'com'::char varying(3) as sector_abbr, 
		3::INTEGER as baseline_system_type,
		efficiency_improvement_factor,
		system_lifetime_yrs,
		annual_degradation_pct
	FROM diffusion_template.input_baseline_performance_hvac_com_type_3
	UNION ALL
	SELECT year, 
		'com'::char varying(3) as sector_abbr, 
		4::INTEGER as baseline_system_type,
		efficiency_improvement_factor,
		system_lifetime_yrs,
		annual_degradation_pct
	FROM diffusion_template.input_baseline_performance_hvac_com_type_4
);


