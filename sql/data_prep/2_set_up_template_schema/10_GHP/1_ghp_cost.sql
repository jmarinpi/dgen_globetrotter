set role 'diffusion-writers';

-- res
DROP TABLE IF EXISTs diffusion_template.input_ghp_cost_horizontal_res CASCADE;
CREATE TABLE diffusion_template.input_ghp_cost_horizontal_res
(
	year integer NOT NULL,
	new_heat_exchanger_cost_dollars_per_ton numeric NOT NULL,
	new_heat_pump_cost_dollars_per_ton numeric NOT NULL,
	new_rest_of_system_costs_dollars_per_sf numeric NOT NULL,
	fixed_om_dollars_per_sf_per_year numeric NOT NULL,
	retrofit_rest_of_system_multiplier numeric NOT NULL,
	CONSTRAINT input_ghp_cost_horizontal_res_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTs diffusion_template.input_ghp_cost_vertical_res CASCADE;
CREATE TABLE diffusion_template.input_ghp_cost_vertical_res
(
	year integer NOT NULL,
	new_heat_exchanger_cost_dollars_per_ton numeric NOT NULL,
	new_heat_pump_cost_dollars_per_ton numeric NOT NULL,
	new_rest_of_system_costs_dollars_per_sf numeric NOT NULL,
	fixed_om_dollars_per_sf_per_year numeric NOT NULL,
	retrofit_rest_of_system_multiplier numeric NOT NULL,
	CONSTRAINT input_ghp_cost_vertical_res_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


-- Com
DROP TABLE IF EXISTs diffusion_template.input_ghp_cost_horizontal_com CASCADE;
CREATE TABLE diffusion_template.input_ghp_cost_horizontal_com
(
	year integer NOT NULL,
	new_heat_exchanger_cost_dollars_per_ton numeric NOT NULL,
	new_heat_pump_cost_dollars_per_ton numeric NOT NULL,
	new_rest_of_system_costs_dollars_per_sf numeric NOT NULL,
	fixed_om_dollars_per_sf_per_year numeric NOT NULL,
	retrofit_rest_of_system_multiplier numeric NOT NULL,
	CONSTRAINT input_ghp_cost_horizontal_com_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTs diffusion_template.input_ghp_cost_vertical_com CASCADE;
CREATE TABLE diffusion_template.input_ghp_cost_vertical_com
(
	year integer NOT NULL,
	new_heat_exchanger_cost_dollars_per_ton numeric NOT NULL,
	new_heat_pump_cost_dollars_per_ton numeric NOT NULL,
	new_rest_of_system_costs_dollars_per_sf numeric NOT NULL,
	fixed_om_dollars_per_sf_per_year numeric NOT NULL,
	retrofit_rest_of_system_multiplier numeric NOT NULL,
	CONSTRAINT input_ghp_cost_vertical_com_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

-- Ind
DROP TABLE IF EXISTs diffusion_template.input_ghp_cost_horizontal_ind CASCADE;
CREATE TABLE diffusion_template.input_ghp_cost_horizontal_ind
(
	year integer NOT NULL,
	new_heat_exchanger_cost_dollars_per_ton numeric NOT NULL,
	new_heat_pump_cost_dollars_per_ton numeric NOT NULL,
	new_rest_of_system_costs_dollars_per_sf numeric NOT NULL,
	fixed_om_dollars_per_sf_per_year numeric NOT NULL,
	retrofit_rest_of_system_multiplier numeric NOT NULL,
	CONSTRAINT input_ghp_cost_horizontal_ind_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTs diffusion_template.input_ghp_cost_vertical_ind CASCADE;
CREATE TABLE diffusion_template.input_ghp_cost_vertical_ind
(
	year integer NOT NULL,
	new_heat_exchanger_cost_dollars_per_ton numeric NOT NULL,
	new_heat_pump_cost_dollars_per_ton numeric NOT NULL,
	new_rest_of_system_costs_dollars_per_sf numeric NOT NULL,
	fixed_om_dollars_per_sf_per_year numeric NOT NULL,
	retrofit_rest_of_system_multiplier numeric NOT NULL,
	CONSTRAINT input_ghp_cost_vertical_ind_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


---- Create Views

DROP VIEW IF EXISTS diffusion_template.input_ghp_cost;
CREATE VIEW diffusion_template.input_ghp_cost as 
(
	--res
	SELECT year, 'res'::char varying(3) as sector, 'closed horizontal'::text as sys_config,
	new_heat_exchanger_cost_dollars_per_ton, new_heat_pump_cost_dollars_per_ton, new_rest_of_system_costs_dollars_per_sf 
	fixed_om_dollars_per_sf_per_year, retrofit_rest_of_system_multiplier
	FROM diffusion_template.input_ghp_cost_horizontal_res
	UNION ALL
	SELECT year, 'res'::char varying(3) as sector, 'closed vertical'::text as sys_config,
	new_heat_exchanger_cost_dollars_per_ton, new_heat_pump_cost_dollars_per_ton, new_rest_of_system_costs_dollars_per_sf 
	fixed_om_dollars_per_sf_per_year, retrofit_rest_of_system_multiplier
	FROM diffusion_template.input_ghp_cost_vertical_res
	UNION ALL
	--com
	SELECT year, 'com'::char varying(3) as sector, 'closed horizontal'::text as sys_config,
	new_heat_exchanger_cost_dollars_per_ton, new_heat_pump_cost_dollars_per_ton, new_rest_of_system_costs_dollars_per_sf 
	fixed_om_dollars_per_sf_per_year, retrofit_rest_of_system_multiplier
	FROM diffusion_template.input_ghp_cost_horizontal_com
	UNION ALL
	SELECT year, 'com'::char varying(3) as sector, 'closed vertical'::text as sys_config,
	new_heat_exchanger_cost_dollars_per_ton, new_heat_pump_cost_dollars_per_ton, new_rest_of_system_costs_dollars_per_sf 
	fixed_om_dollars_per_sf_per_year, retrofit_rest_of_system_multiplier
	FROM diffusion_template.input_ghp_cost_vertical_com
	UNION ALL
	--ind
	SELECT year, 'ind'::char varying(3) as sector, 'closed horizontal'::text as sys_config,
	new_heat_exchanger_cost_dollars_per_ton, new_heat_pump_cost_dollars_per_ton, new_rest_of_system_costs_dollars_per_sf 
	fixed_om_dollars_per_sf_per_year, retrofit_rest_of_system_multiplier
	FROM diffusion_template.input_ghp_cost_horizontal_ind
	UNION ALL
	SELECT year, 'ind'::char varying(3) as sector, 'closed vertical'::text as sys_config,
	new_heat_exchanger_cost_dollars_per_ton, new_heat_pump_cost_dollars_per_ton, new_rest_of_system_costs_dollars_per_sf 
	fixed_om_dollars_per_sf_per_year, retrofit_rest_of_system_multiplier
	FROM diffusion_template.input_ghp_cost_vertical_ind
);
