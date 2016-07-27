set role 'diffusion-writers';

--Baseline Residential
DROP TABLE IF EXISTs diffusion_template.input_baseline_costs_hvac_res_type_1 CASCADE;
CREATE TABLE diffusion_template.input_baseline_costs_hvac_res_type_1 
(
	year integer not null,
	hvac_equipment_cost_dollars_per_cooling_ton numeric not null,
	new_rest_of_system_costs_dollars_per_cooling_ton numeric not null,
	retrofit_rest_of_system_multiplier numeric NOT NULL,
	fixed_om_dollars_per_sf_per_year numeric NOT NULL,
	CONSTRAINT input_baseline_costs_hvac_res_type_1_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_costs_hvac_res_type_2 CASCADE;
CREATE TABLE diffusion_template.input_baseline_costs_hvac_res_type_2
(
	year integer not null,
	hvac_equipment_cost_dollars_per_cooling_ton numeric not null,
	new_rest_of_system_costs_dollars_per_cooling_ton numeric not null,
	retrofit_rest_of_system_multiplier numeric NOT NULL,
	fixed_om_dollars_per_sf_per_year numeric NOT NULL,
	CONSTRAINT input_baseline_costs_hvac_res_type_2_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_costs_hvac_res_type_3 CASCADE;
CREATE TABLE diffusion_template.input_baseline_costs_hvac_res_type_3
(
	year integer not null,
	hvac_equipment_cost_dollars_per_cooling_ton numeric not null,
	new_rest_of_system_costs_dollars_per_cooling_ton numeric not null,
	retrofit_rest_of_system_multiplier numeric NOT NULL,
	fixed_om_dollars_per_sf_per_year numeric NOT NULL,
	CONSTRAINT input_baseline_costs_hvac_res_type_3_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_costs_hvac_res_type_4 CASCADE;
CREATE TABLE diffusion_template.input_baseline_costs_hvac_res_type_4
(
	year integer not null,
	hvac_equipment_cost_dollars_per_cooling_ton numeric not null,
	new_rest_of_system_costs_dollars_per_cooling_ton numeric not null,
	retrofit_rest_of_system_multiplier numeric NOT NULL,
	fixed_om_dollars_per_sf_per_year numeric NOT NULL,
	CONSTRAINT input_baseline_costs_hvac_res_type_4_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

--Baseline Commercial
DROP TABLE IF EXISTs diffusion_template.input_baseline_costs_hvac_com_type_1 CASCADE;
CREATE TABLE diffusion_template.input_baseline_costs_hvac_com_type_1 
(
	year integer not null,
	hvac_equipment_cost_dollars_per_cooling_ton numeric not null,
	new_rest_of_system_costs_dollars_per_cooling_ton numeric not null,
	retrofit_rest_of_system_multiplier numeric NOT NULL,
	fixed_om_dollars_per_sf_per_year numeric NOT NULL,
	CONSTRAINT input_baseline_costs_hvac_com_type_1_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_costs_hvac_com_type_2 CASCADE;
CREATE TABLE diffusion_template.input_baseline_costs_hvac_com_type_2
(
	year integer not null,
	hvac_equipment_cost_dollars_per_cooling_ton numeric not null,
	new_rest_of_system_costs_dollars_per_cooling_ton numeric not null,
	retrofit_rest_of_system_multiplier numeric NOT NULL,
	fixed_om_dollars_per_sf_per_year numeric NOT NULL,
	CONSTRAINT input_baseline_costs_hvac_com_type_2_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_costs_hvac_com_type_3 CASCADE;
CREATE TABLE diffusion_template.input_baseline_costs_hvac_com_type_3
(
	year integer not null,
	hvac_equipment_cost_dollars_per_cooling_ton numeric not null,
	new_rest_of_system_costs_dollars_per_cooling_ton numeric not null,
	retrofit_rest_of_system_multiplier numeric NOT NULL,
	fixed_om_dollars_per_sf_per_year numeric NOT NULL,
	CONSTRAINT input_baseline_costs_hvac_com_type_3_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_baseline_costs_hvac_com_type_4 CASCADE;
CREATE TABLE diffusion_template.input_baseline_costs_hvac_com_type_4
(
	year integer not null,
	hvac_equipment_cost_dollars_per_cooling_ton numeric not null,
	new_rest_of_system_costs_dollars_per_cooling_ton numeric not null,
	retrofit_rest_of_system_multiplier numeric NOT NULL,
	fixed_om_dollars_per_sf_per_year numeric NOT NULL,
	CONSTRAINT input_baseline_costs_hvac_com_type_4_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


-- Create Views
DROP VIEW IF EXISTS diffusion_template.input_baseline_costs_hvac;
CREATE VIEW diffusion_template.input_baseline_costs_hvac AS 
(
	--res
	SELECT  year, 
			'res'::varchar(3) as sector_abbr, 
			1::INTEGER as baseline_system_type,
			hvac_equipment_cost_dollars_per_cooling_ton,
			new_rest_of_system_costs_dollars_per_cooling_ton,
			retrofit_rest_of_system_multiplier,
			fixed_om_dollars_per_sf_per_year
	FROM diffusion_template.input_baseline_costs_hvac_res_type_1
	UNION ALL
	SELECT year, 
			'res'::varchar(3) as sector_abbr, 
			2::INTEGER as baseline_system_type,
			hvac_equipment_cost_dollars_per_cooling_ton,
			new_rest_of_system_costs_dollars_per_cooling_ton,
			retrofit_rest_of_system_multiplier,
			fixed_om_dollars_per_sf_per_year
	FROM diffusion_template.input_baseline_costs_hvac_res_type_2
	UNION ALL
	SELECT year, 
			'res'::varchar(3) as sector_abbr, 
			3::INTEGER as baseline_system_type,
			hvac_equipment_cost_dollars_per_cooling_ton,
			new_rest_of_system_costs_dollars_per_cooling_ton,
			retrofit_rest_of_system_multiplier,
			fixed_om_dollars_per_sf_per_year
	FROM diffusion_template.input_baseline_costs_hvac_res_type_3
	UNION ALL
	SELECT year, 
			'res'::varchar(3) as sector_abbr, 
			4::INTEGER as baseline_system_type,
			hvac_equipment_cost_dollars_per_cooling_ton,
			new_rest_of_system_costs_dollars_per_cooling_ton,
			retrofit_rest_of_system_multiplier,
			fixed_om_dollars_per_sf_per_year
	FROM diffusion_template.input_baseline_costs_hvac_res_type_4
	UNION ALL
	--com
	SELECT year, 
			'com'::varchar(3) as sector_abbr, 
			1::INTEGER as baseline_system_type,
			hvac_equipment_cost_dollars_per_cooling_ton,
			new_rest_of_system_costs_dollars_per_cooling_ton,
			retrofit_rest_of_system_multiplier,
			fixed_om_dollars_per_sf_per_year
	FROM diffusion_template.input_baseline_costs_hvac_com_type_1
	UNION ALL
	SELECT year, 
			'com'::varchar(3) as sector_abbr, 
			2::INTEGER as baseline_system_type,
			hvac_equipment_cost_dollars_per_cooling_ton,
			new_rest_of_system_costs_dollars_per_cooling_ton,
			retrofit_rest_of_system_multiplier,
			fixed_om_dollars_per_sf_per_year
	FROM diffusion_template.input_baseline_costs_hvac_com_type_2
	UNION ALL
	SELECT year, 
			'com'::varchar(3) as sector_abbr, 
			3::INTEGER as baseline_system_type,
			hvac_equipment_cost_dollars_per_cooling_ton,
			new_rest_of_system_costs_dollars_per_cooling_ton,
			retrofit_rest_of_system_multiplier,
			fixed_om_dollars_per_sf_per_year
	FROM diffusion_template.input_baseline_costs_hvac_com_type_3
	UNION ALL
	SELECT year, 
			'com'::varchar(3) as sector_abbr, 
			4::INTEGER as baseline_system_type,
			hvac_equipment_cost_dollars_per_cooling_ton,
			new_rest_of_system_costs_dollars_per_cooling_ton,
			retrofit_rest_of_system_multiplier,
			fixed_om_dollars_per_sf_per_year
	FROM diffusion_template.input_baseline_costs_hvac_com_type_4
);