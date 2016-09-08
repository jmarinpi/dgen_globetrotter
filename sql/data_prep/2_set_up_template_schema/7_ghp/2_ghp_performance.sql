﻿set role 'diffusion-writers';

-- improvements
DROP TABLE IF EXISTs diffusion_template.input_ghp_performance_improvements CASCADE;
CREATE TABLE diffusion_template.input_ghp_performance_improvements
(
	year integer NOT NULL,
	ghp_heat_pump_lifetime_yrs numeric NOT NULL,
	CONSTRAINT input_ghp_performance_improvements_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


-- system degradation -- vertical
DROP TABLE IF EXISTs diffusion_template.input_ghp_performance_degradation_vertical CASCADE;
CREATE TABLE diffusion_template.input_ghp_performance_degradation_vertical
(
	iecc_temperature_zone integer not null,
	year integer NOT NULL,
	annual_degradation_pct numeric not null,
	CONSTRAINT input_ghp_performance_degradation_vertical_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

-- system degradation -- horizontal
DROP TABLE IF EXISTs diffusion_template.input_ghp_performance_degradation_horizontal CASCADE;
CREATE TABLE diffusion_template.input_ghp_performance_degradation_horizontal
(
	iecc_temperature_zone integer not null,
	year integer NOT NULL,
	annual_degradation_pct numeric not null,
	CONSTRAINT input_ghp_performance_degradation_horizontal_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

-- views
DROP VIEW IF EXISTS diffusion_template.input_ghp_system_degradation;
CREATE VIEW diffusion_template.input_ghp_system_degradation AS
select iecc_temperature_zone,
	year,
	annual_degradation_pct, 
	'vertical' as sys_config
from diffusion_template.input_ghp_performance_degradation_vertical
UNION ALL
select iecc_temperature_zone,
	year,
	annual_degradation_pct,
	'horizontal' as sys_config
from diffusion_template.input_ghp_performance_degradation_horizontal;


