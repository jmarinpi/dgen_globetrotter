﻿set role 'diffusion-writers';


DROP TABLE IF EXISTS diffusion_template.input_wind_performance_allowable_turbine_sizes_raw CASCADE;
CREATE TABLE diffusion_template.input_wind_performance_allowable_turbine_sizes_raw
(
	turbine_size_kw numeric NOT NULL,
	turbine_height_m integer NOT NULL,
	allowed boolean NOT NULL
);


DROP VIEW IF EXISTS diffusion_template.input_wind_performance_allowable_turbine_sizes CASCADE;
CREATE VIEW diffusion_template.input_wind_performance_allowable_turbine_sizes AS
SELECT turbine_size_kw, turbine_height_m
from diffusion_template.input_wind_performance_allowable_turbine_sizes_raw
where allowed = True ;


DROP TABLE IF EXISTS diffusion_template.input_wind_performance_turbine_size_classes;
CREATE TABLE diffusion_template.input_wind_performance_turbine_size_classes
(
	turbine_size_kw numeric NOT NULL,
	size_class text NOT NULL
);


DROP TABLE IF EXISTS diffusion_template.input_wind_performance_improvements CASCADE;
CREATE TABLE diffusion_template.input_wind_performance_improvements
(
	turbine_size_kw numeric not null,
	year integer not null,
	perf_improvement_factor numeric not null,
	CONSTRAINT input_wind_performance_improvements_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT,
	CONSTRAINT perf_improvement_factor_check CHECK (perf_improvement_factor >= 0 and perf_improvement_factor <= .25)	
);



DROP TABLE IF EXISTS diffusion_template.input_wind_performance_gen_derate_factors;
CREATE TABLE diffusion_template.input_wind_performance_gen_derate_factors
(
	turbine_size_kw numeric not null,
	year integer not null,
	derate_factor numeric not null,
	CONSTRAINT input_wind_performance_gen_derate_factors_year_fkey FOREIGN KEY (year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);



DROP TABLE IF EXIStS diffusion_template.input_wind_performance_system_sizing_factors_raw CASCADE;
CREATE TABLE diffusion_template.input_wind_performance_system_sizing_factors_raw
(
	sector text not null,
	sys_size_target_nem numeric not null,
	sys_oversize_limit_nem numeric not null,
	sys_size_target_no_nem numeric not null,
	sys_oversize_limit_no_nem numeric not null,
	CONSTRAINT sys_oversize_limit_nem_check CHECK (sys_oversize_limit_nem >= 1::numeric),
	CONSTRAINT sys_oversize_limit_no_nem_check CHECK (sys_oversize_limit_no_nem >= 1::numeric),
	CONSTRAINT sys_size_target_nem_check CHECK (sys_size_target_nem >= 0.01 AND sys_size_target_nem <= 1::numeric),
	CONSTRAINT sys_size_target_no_nem_check CHECK (sys_size_target_no_nem >= 0.01 AND sys_size_target_no_nem <= 1::numeric)
);

DROP VIEW IF EXIStS diffusion_template.input_wind_performance_system_sizing_factors;
CREATE VIeW diffusion_template.input_wind_performance_system_sizing_factors AS
select 	b.sector_abbr, 
	a.sys_size_target_nem, a.sys_oversize_limit_nem,
	sys_size_target_no_nem, a.sys_oversize_limit_no_nem
from diffusion_template.input_wind_performance_system_sizing_factors_raw a
left join diffusion_config.sceninp_sector b
ON a.sector = b.sector;