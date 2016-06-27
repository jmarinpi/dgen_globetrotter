set role 'diffusion-writers';

--Baseline Residential
DROP TABLE IF EXISTs diffusion_template.input_baseline_costs_hvac_res_type_1 CASCADE;
CREATE TABLE diffusion_template.input_baseline_costs_hvac_res_type_1 
(
	year integer not null,
	new_hvac_or_process_sys_costs_dollars_per_sf numeric not null,
	reftrofit_hvac_or_process_sys_costs_dollars_per_sf numeric not null,
	fixed_om_dollars_sf_year integer not null,
	CONSTRAINT input_baseline_costs_hvac_res_type_1_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_costs_hvac_res_type_2 CASCADE;
CREATE TABLE diffusion_template.input_baseline_costs_hvac_res_type_2
(
	year integer not null,
	new_hvac_or_process_sys_costs_dollars_per_sf numeric not null,
	reftrofit_hvac_or_process_sys_costs_dollars_per_sf numeric not null,
	fixed_om_dollars_sf_year integer not null,
	CONSTRAINT input_baseline_costs_hvac_res_type_2_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_costs_hvac_res_type_3 CASCADE;
CREATE TABLE diffusion_template.input_baseline_costs_hvac_res_type_3
(
	year integer not null,
	new_hvac_or_process_sys_costs_dollars_per_sf numeric not null,
	reftrofit_hvac_or_process_sys_costs_dollars_per_sf numeric not null,
	fixed_om_dollars_sf_year integer not null,
	CONSTRAINT input_baseline_costs_hvac_res_type_3_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_costs_hvac_res_type_4 CASCADE;
CREATE TABLE diffusion_template.input_baseline_costs_hvac_res_type_4
(
	year integer not null,
	new_hvac_or_process_sys_costs_dollars_per_sf numeric not null,
	reftrofit_hvac_or_process_sys_costs_dollars_per_sf numeric not null,
	fixed_om_dollars_sf_year integer not null,
	CONSTRAINT input_baseline_costs_hvac_res_type_4_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

--Baseline Commercial
DROP TABLE IF EXISTs diffusion_template.input_baseline_costs_hvac_com_type_1 CASCADE;
CREATE TABLE diffusion_template.input_baseline_costs_hvac_com_type_1 
(
	year integer not null,
	new_hvac_or_process_sys_costs_dollars_per_sf numeric not null,
	reftrofit_hvac_or_process_sys_costs_dollars_per_sf numeric not null,
	fixed_om_dollars_sf_year integer not null,
	CONSTRAINT input_baseline_costs_hvac_com_type_1_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_costs_hvac_com_type_2 CASCADE;
CREATE TABLE diffusion_template.input_baseline_costs_hvac_com_type_2
(
	year integer not null,
	new_hvac_or_process_sys_costs_dollars_per_sf numeric not null,
	reftrofit_hvac_or_process_sys_costs_dollars_per_sf numeric not null,
	fixed_om_dollars_sf_year integer not null,
	CONSTRAINT input_baseline_costs_hvac_com_type_2_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_costs_hvac_com_type_3 CASCADE;
CREATE TABLE diffusion_template.input_baseline_costs_hvac_com_type_3
(
	year integer not null,
	new_hvac_or_process_sys_costs_dollars_per_sf numeric not null,
	reftrofit_hvac_or_process_sys_costs_dollars_per_sf numeric not null,
	fixed_om_dollars_sf_year integer not null,
	CONSTRAINT input_baseline_costs_hvac_com_type_3_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_costs_hvac_com_type_4 CASCADE;
CREATE TABLE diffusion_template.input_baseline_costs_hvac_com_type_4
(
	year integer not null,
	new_hvac_or_process_sys_costs_dollars_per_sf numeric not null,
	reftrofit_hvac_or_process_sys_costs_dollars_per_sf numeric not null,
	fixed_om_dollars_sf_year integer not null,
	CONSTRAINT input_baseline_costs_hvac_com_type_4_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


-- Create Views
DROP VIEW IF EXISTS diffusion_template.input_baseline_costs_hvac;
CREATE VIEW diffusion_template.input_baseline_costs_hvac AS 
(
	--res
	SELECT year, 'res'::char varying(3) as sector_abbr, 'baseline 1' as baseline_type,
	new_hvac_or_process_sys_costs_dollars_per_sf, reftrofit_hvac_or_process_sys_costs_dollars_per_sf, 
	fixed_om_dollars_sf_year
	FROM diffusion_template.input_baseline_costs_hvac_res_type_1
	UNION ALL
	SELECT year, 'res'::char varying(3) as sector_abbr, 'baseline 2' as baseline_type,
	new_hvac_or_process_sys_costs_dollars_per_sf, reftrofit_hvac_or_process_sys_costs_dollars_per_sf, 
	fixed_om_dollars_sf_year
	FROM diffusion_template.input_baseline_costs_hvac_res_type_2
	UNION ALL
	SELECT year, 'res'::char varying(3) as sector_abbr, 'baseline 3' as baseline_type,
	new_hvac_or_process_sys_costs_dollars_per_sf, reftrofit_hvac_or_process_sys_costs_dollars_per_sf, 
	fixed_om_dollars_sf_year
	FROM diffusion_template.input_baseline_costs_hvac_res_type_3
	UNION ALL
	SELECT year, 'res'::char varying(3) as sector_abbr, 'baseline 4' as baseline_type,
	new_hvac_or_process_sys_costs_dollars_per_sf, reftrofit_hvac_or_process_sys_costs_dollars_per_sf, 
	fixed_om_dollars_sf_year
	FROM diffusion_template.input_baseline_costs_hvac_res_type_4
	UNION ALL
	--com
	SELECT year, 'com'::char varying(3) as sector_abbr, 'baseline 1' as baseline_type,
	new_hvac_or_process_sys_costs_dollars_per_sf, reftrofit_hvac_or_process_sys_costs_dollars_per_sf, 
	fixed_om_dollars_sf_year
	FROM diffusion_template.input_baseline_costs_hvac_com_type_1
	UNION ALL
	SELECT year, 'com'::char varying(3) as sector_abbr, 'baseline 2' as baseline_type,
	new_hvac_or_process_sys_costs_dollars_per_sf, reftrofit_hvac_or_process_sys_costs_dollars_per_sf, 
	fixed_om_dollars_sf_year
	FROM diffusion_template.input_baseline_costs_hvac_com_type_2
	UNION ALL
	SELECT year, 'com'::char varying(3) as sector_abbr, 'baseline 3' as baseline_type,
	new_hvac_or_process_sys_costs_dollars_per_sf, reftrofit_hvac_or_process_sys_costs_dollars_per_sf, 
	fixed_om_dollars_sf_year
	FROM diffusion_template.input_baseline_costs_hvac_com_type_3
	UNION ALL
	SELECT year, 'com'::char varying(3) as sector_abbr, 'baseline 4' as baseline_type,
	new_hvac_or_process_sys_costs_dollars_per_sf, reftrofit_hvac_or_process_sys_costs_dollars_per_sf, 
	fixed_om_dollars_sf_year
	FROM diffusion_template.input_baseline_costs_hvac_com_type_4
);