set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_template.input_wind_cost_projections_0002p5_kw CASCADE;
CREATE TABLE diffusion_template.input_wind_cost_projections_0002p5_kw
(
	year integer NOT NULL,
	capital_cost_dollars_per_kw numeric NOT NULL,
	fixed_om_dollars_per_kw_per_yr numeric NOT NULL,
	variable_om_dollars_per_kwh numeric NOT NULL,
	default_tower_height_m numeric NOT NULL,
	cost_for_higher_towers_dollars_per_kw_per_m numeric NOT NULL,
	CONSTRAINT input_wind_cost_projections_0002p5_kw_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_wind_cost_projections_0005_kw CASCADE;
CREATE TABLE diffusion_template.input_wind_cost_projections_0005_kw
(
	year integer NOT NULL,
	capital_cost_dollars_per_kw numeric NOT NULL,
	fixed_om_dollars_per_kw_per_yr numeric NOT NULL,
	variable_om_dollars_per_kwh numeric NOT NULL,
	default_tower_height_m numeric NOT NULL,
	cost_for_higher_towers_dollars_per_kw_per_m numeric NOT NULL,
	CONSTRAINT input_wind_cost_projections_0005_kw_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_wind_cost_projections_0010_kw CASCADE;
CREATE TABLE diffusion_template.input_wind_cost_projections_0010_kw
(
	year integer NOT NULL,
	capital_cost_dollars_per_kw numeric NOT NULL,
	fixed_om_dollars_per_kw_per_yr numeric NOT NULL,
	variable_om_dollars_per_kwh numeric NOT NULL,
	default_tower_height_m numeric NOT NULL,
	cost_for_higher_towers_dollars_per_kw_per_m numeric NOT NULL,
	CONSTRAINT input_wind_cost_projections_0010_kw_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_wind_cost_projections_0020_kw CASCADE;
CREATE TABLE diffusion_template.input_wind_cost_projections_0020_kw
(
	year integer NOT NULL,
	capital_cost_dollars_per_kw numeric NOT NULL,
	fixed_om_dollars_per_kw_per_yr numeric NOT NULL,
	variable_om_dollars_per_kwh numeric NOT NULL,
	default_tower_height_m numeric NOT NULL,
	cost_for_higher_towers_dollars_per_kw_per_m numeric NOT NULL,
	CONSTRAINT input_wind_cost_projections_0020_kw_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_wind_cost_projections_0050_kw CASCADE;
CREATE TABLE diffusion_template.input_wind_cost_projections_0050_kw
(
	year integer NOT NULL,
	capital_cost_dollars_per_kw numeric NOT NULL,
	fixed_om_dollars_per_kw_per_yr numeric NOT NULL,
	variable_om_dollars_per_kwh numeric NOT NULL,
	default_tower_height_m numeric NOT NULL,
	cost_for_higher_towers_dollars_per_kw_per_m numeric NOT NULL,
	CONSTRAINT input_wind_cost_projections_0050_kw_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_wind_cost_projections_0100_kw CASCADE;
CREATE TABLE diffusion_template.input_wind_cost_projections_0100_kw
(
	year integer NOT NULL,
	capital_cost_dollars_per_kw numeric NOT NULL,
	fixed_om_dollars_per_kw_per_yr numeric NOT NULL,
	variable_om_dollars_per_kwh numeric NOT NULL,
	default_tower_height_m numeric NOT NULL,
	cost_for_higher_towers_dollars_per_kw_per_m numeric NOT NULL,
	CONSTRAINT input_wind_cost_projections_0100_kw_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_wind_cost_projections_0250_kw CASCADE;
CREATE TABLE diffusion_template.input_wind_cost_projections_0250_kw
(
	year integer NOT NULL,
	capital_cost_dollars_per_kw numeric NOT NULL,
	fixed_om_dollars_per_kw_per_yr numeric NOT NULL,
	variable_om_dollars_per_kwh numeric NOT NULL,
	default_tower_height_m numeric NOT NULL,
	cost_for_higher_towers_dollars_per_kw_per_m numeric NOT NULL,
	CONSTRAINT input_wind_cost_projections_0250_kw_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_wind_cost_projections_0500_kw CASCADE;
CREATE TABLE diffusion_template.input_wind_cost_projections_0500_kw
(
	year integer NOT NULL,
	capital_cost_dollars_per_kw numeric NOT NULL,
	fixed_om_dollars_per_kw_per_yr numeric NOT NULL,
	variable_om_dollars_per_kwh numeric NOT NULL,
	default_tower_height_m numeric NOT NULL,
	cost_for_higher_towers_dollars_per_kw_per_m numeric NOT NULL,
	CONSTRAINT input_wind_cost_projections_0500_kw_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_wind_cost_projections_0750_kw CASCADE;
CREATE TABLE diffusion_template.input_wind_cost_projections_0750_kw
(
	year integer NOT NULL,
	capital_cost_dollars_per_kw numeric NOT NULL,
	fixed_om_dollars_per_kw_per_yr numeric NOT NULL,
	variable_om_dollars_per_kwh numeric NOT NULL,
	default_tower_height_m numeric NOT NULL,
	cost_for_higher_towers_dollars_per_kw_per_m numeric NOT NULL,
	CONSTRAINT input_wind_cost_projections_0750_kw_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_wind_cost_projections_1000_kw CASCADE;
CREATE TABLE diffusion_template.input_wind_cost_projections_1000_kw
(
	year integer NOT NULL,
	capital_cost_dollars_per_kw numeric NOT NULL,
	fixed_om_dollars_per_kw_per_yr numeric NOT NULL,
	variable_om_dollars_per_kwh numeric NOT NULL,
	default_tower_height_m numeric NOT NULL,
	cost_for_higher_towers_dollars_per_kw_per_m numeric NOT NULL,
	CONSTRAINT input_wind_cost_projections_1000_kw_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_wind_cost_projections_1500_kw CASCADE;
CREATE TABLE diffusion_template.input_wind_cost_projections_1500_kw
(
	year integer NOT NULL,
	capital_cost_dollars_per_kw numeric NOT NULL,
	fixed_om_dollars_per_kw_per_yr numeric NOT NULL,
	variable_om_dollars_per_kwh numeric NOT NULL,
	default_tower_height_m numeric NOT NULL,
	cost_for_higher_towers_dollars_per_kw_per_m numeric NOT NULL,
	CONSTRAINT input_wind_cost_projections_1500_kw_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP VIEW IF EXISTS diffusion_template.input_wind_cost_projections;
CREATE VIEW diffusion_template.input_wind_cost_projections AS
SELECT *, 2.5::NUMERIC as turbine_size_kw
FROM diffusion_template.input_wind_cost_projections_0002p5_kw
UNION ALL
SELECT *, 5::NUMERIC as turbine_size_kw
FROM diffusion_template.input_wind_cost_projections_0005_kw
UNION ALL
SELECT *, 10::NUMERIC as turbine_size_kw
FROM diffusion_template.input_wind_cost_projections_0010_kw
UNION ALL
SELECT *, 20::NUMERIC as turbine_size_kw
FROM diffusion_template.input_wind_cost_projections_0020_kw
UNION ALL
SELECT *, 50::NUMERIC as turbine_size_kw
FROM diffusion_template.input_wind_cost_projections_0050_kw
UNION ALL
SELECT *, 100::NUMERIC as turbine_size_kw
FROM diffusion_template.input_wind_cost_projections_0100_kw
UNION ALL
SELECT *, 250::NUMERIC as turbine_size_kw
FROM diffusion_template.input_wind_cost_projections_0250_kw
UNION ALL
SELECT *, 500::NUMERIC as turbine_size_kw
FROM diffusion_template.input_wind_cost_projections_0500_kw
UNION ALL
SELECT *, 750::NUMERIC as turbine_size_kw
FROM diffusion_template.input_wind_cost_projections_0750_kw
UNION ALL
SELECT *, 1000::NUMERIC as turbine_size_kw
FROM diffusion_template.input_wind_cost_projections_1000_kw
UNION ALL
SELECT *, 1500::NUMERIC as turbine_size_kw
FROM diffusion_template.input_wind_cost_projections_1500_kw;


--   turbine_size_kw numeric