﻿set role 'diffusion-writers';


DROP TABLE IF EXISTs diffusion_template.input_solar_cost_projections_res CASCADE;
CREATE TABLE diffusion_template.input_solar_cost_projections_res
(
	year integer NOT NULL,
	capital_cost_dollars_per_kw numeric NOT NULL,
	inverter_cost_dollars_per_kw numeric NOT NULL,
	fixed_om_dollars_per_kw_per_yr numeric NOT NULL,
	variable_om_dollars_per_kwh numeric NOT NULL,
	CONSTRAINT input_solar_cost_projections_res_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_solar_cost_projections_com CASCADE;
CREATE TABLE diffusion_template.input_solar_cost_projections_com
(
	year integer NOT NULL,
	capital_cost_dollars_per_kw numeric NOT NULL,
	inverter_cost_dollars_per_kw numeric NOT NULL,
	fixed_om_dollars_per_kw_per_yr numeric NOT NULL,
	variable_om_dollars_per_kwh numeric NOT NULL,
	CONSTRAINT input_solar_cost_projections_com_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP TABLE IF EXISTs diffusion_template.input_solar_cost_projections_ind CASCADE;
CREATE TABLE diffusion_template.input_solar_cost_projections_ind
(
	year integer NOT NULL,
	capital_cost_dollars_per_kw numeric NOT NULL,
	inverter_cost_dollars_per_kw numeric NOT NULL,
	fixed_om_dollars_per_kw_per_yr numeric NOT NULL,
	variable_om_dollars_per_kwh numeric NOT NULL,
	CONSTRAINT input_solar_cost_projections_ind_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

DROP VIEW IF EXISTS diffusion_template.input_solar_cost_projections;
CREATE VIEW diffusion_template.input_solar_cost_projections AS

SELECT *, 'res'::character varying(3) as sector_abbr
FROM diffusion_template.input_solar_cost_projections_res

UNION ALL

SELECT *, 'com'::character varying(3) as sector_abbr
FROM diffusion_template.input_solar_cost_projections_com

UNION ALL

SELECT *, 'ind'::character varying(3) as sector_abbr
FROM diffusion_template.input_solar_cost_projections_ind;


DROP TABLE IF EXISTS diffusion_template.input_solar_cost_learning_rates;
CREATE TABLE diffusion_template.input_solar_cost_learning_rates
(
	year integer NOT NULL,
	learning_rate numeric NOT NULL,
	us_frac_of_global_mkt numeric NOT NULL,
	CONSTRAINT input_solar_cost_learning_rates_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_solar_cost_assumptions;
CREATE TABLE diffusion_template.input_solar_cost_assumptions
(
	cost_assumptions text not null,
	CONSTRAINT input_solar_cost_assumptions_cost_assumptions FOREIGN KEY (cost_assumptions)
		REFERENCES diffusion_config.sceninp_cost_assumptions_solar (val) MATCH SIMPLE
		ON DELETE RESTRICT
);



DROP VIEW IF EXISTS diffusion_template.input_solar_cost_projections_to_model;
CREAte VIEW diffusion_template.input_solar_cost_projections_to_model As
WITH a as 
(
	SELECT year, capital_cost_dollars_per_kw, inverter_cost_dollars_per_kw, 
	       fixed_om_dollars_per_kw_per_yr, variable_om_dollars_per_kwh, 
	       sector_abbr as sector, 'User Defined'::text as source
	FROM diffusion_template.input_solar_cost_projections

	UNION ALL

	SELECT year, capital_cost_dollars_per_kw, inverter_cost_dollars_per_kw, 
		fixed_om_dollars_per_kw_per_yr, variable_om_dollars_per_kwh, 
		sector, 'Solar Program Targets'::text as source
	FROM diffusion_solar.solar_program_target_cost_projections

	UNION ALL

	select year, capital_cost_dollars_per_kw, inverter_cost_dollars_per_kw, 
		fixed_om_dollars_per_kw_per_yr, variable_om_dollars_per_kwh, 
		sector, 'AEO 2014'::text as source
	from diffusion_solar.solar_costs_aeo2014
)


SELECT a.*
FROM a
INNER JOIN diffusion_template.input_solar_cost_assumptions b
ON a.source = b.cost_assumptions;