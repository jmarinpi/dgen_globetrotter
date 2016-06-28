set role 'diffusion-writers';

-- plant costs --
-- subsurface
DROP TABLE IF EXISTs diffusion_template.input_du_cost_plant_subsurface CASCADE;
CREATE TABLE diffusion_template.input_du_cost_plant_subsurface
(
	year integer NOT NULL,
	future_drilling_cost_improvements_perc_current_costs numeric NOT NULL,
	reservoir_stimulation_costs_dollars_per_well_set numeric NOT NULL,
	exploration_and_discovery_costs_perc_cap_costs numeric NOT NULL,
	CONSTRAINT input_du_cost_plant_subsurface_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);
--surface
DROP TABLE IF EXISTs diffusion_template.input_du_cost_plant_surface CASCADE;
CREATE TABLE diffusion_template.input_du_cost_plant_surface
(
	year integer not null,
	plant_installation_costs_dollars_per_kw numeric not null,
	fixed_om_costs_perc_cap_costs numeric not null,
	distribution_network_construction_costs_dollars_per_m numeric not null,
	operating_costs_reservoir_pumping_costs_dollars_per_gal numeric not null,
	operating_costs_pumping_costs_dollars_per_gal_mile numeric not null,
	natural_gas_peaking_boilers_dollars_per_kw numeric not null,
	construction_period_yrs numeric not null,
	CONSTRAINT input_du_cost_plant_surface_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


-- End User costs --
-- res
DROP TABLE IF EXISTs diffusion_template.input_du_cost_user_res CASCADE;
CREATE TABLE diffusion_template.input_du_cost_user_res
(
	year integer not null,
	sys_connection_cost_dollars numeric not null,
	fixed_om_costs_dollars_sf_yr numeric not null,
	new_sys_installation_costs_dollars_sf numeric not null,
	retrofit_new_sys_installation_cost_multiplier numeric not null,
	CONSTRAINT input_du_cost_user_res_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

-- com 
DROP TABLE IF EXISTs diffusion_template.input_du_cost_user_com CASCADE;
CREATE TABLE diffusion_template.input_du_cost_user_com
(
	year integer not null,
	sys_connection_cost_dollars numeric not null,
	fixed_om_costs_dollars_sf_yr numeric not null,
	new_sys_installation_costs_dollars_sf numeric not null,
	retrofit_new_sys_installation_cost_multiplier numeric not null,
	CONSTRAINT input_du_cost_user_com_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

-- ind
DROP TABLE IF EXISTs diffusion_template.input_du_cost_user_ind CASCADE;
CREATE TABLE diffusion_template.input_du_cost_user_ind
(
	year integer not null,
	sys_connection_cost_dollars numeric not null,
	fixed_om_costs_dollars_sf_yr numeric not null,
	new_sys_installation_costs_dollars_sf numeric not null,
	retrofit_new_sys_installation_cost_multiplier numeric not null,
	CONSTRAINT input_du_cost_user_ind_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);




-- create view for end user costs 
DROP VIEW IF EXISTs diffusion_template.input_du_cost_user;
CREATE VIEW diffusion_template.input_du_cost_user AS
(
	--res
	SELECT *, 'res'::character varying(3) as sector_abbr
	FROM diffusion_template.input_du_cost_user_res

	UNION ALL
	--com
	SELECT *, 'com'::character varying(3) as sector_abbr
	FROM diffusion_template.input_du_cost_user_com

	UNION ALL
	--ind
	SELECT *, 'ind'::character varying(3) as sector_abbr
	FROM diffusion_template.input_du_cost_user_ind
);


