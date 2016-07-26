set role 'diffusion-writers';

-- improvements
DROP TABLE IF EXISTs diffusion_template.input_ghp_performance_improvements_raw CASCADE;
CREATE TABLE diffusion_template.input_ghp_performance_improvements_raw
(
	year integer NOT NULL,
	heat_pump_lifetime_yrs numeric NOT NULL,
	efficiency_improvement_factor_horizontal numeric NOT NULL,
	efficiency_improvement_factor_vertical numeric NOT NULL,
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
	annual_degradatation_pct numeric not null,
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
	annual_degradatation_pct numeric not null,
	CONSTRAINT input_ghp_performance_degradation_horizontal_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);

-- views
DROP VIEW IF EXISTS diffusion_template.input_ghp_performance_improvements;
CREATE VIEW diffusion_template.input_ghp_performance_improvements AS
select year, 
	heat_pump_lifetime_yrs, 
	efficiency_improvement_factor_horizontal as efficiency_improvement_factor,
	'horizontal'::TEXT as sys_config
FROM  diffusion_template.input_ghp_performance_improvements_raw
UNION ALL
select year, 
	heat_pump_lifetime_yrs, 
	efficiency_improvement_factor_vertical as efficiency_improvement_factor,
	'vertical'::TEXT as sys_config
FROM  diffusion_template.input_ghp_performance_improvements_raw;



DROP VIEW IF EXISTS diffusion_template.input_ghp_performance_degradation;
CREATE VIEW diffusion_template.input_ghp_performance_degradation AS
select *, 'vertical' as sys_config
from diffusion_template.input_ghp_performance_degradation_vertical
UNION ALL
select *, 'horizontal' as sys_config
from diffusion_template.input_ghp_performance_degradation_horizontal;


