SET ROLE 'diffusion-writers';
DROP tABlE IF EXISTS diffusion_solar.nem_scenario;
CREATE TABLE diffusion_solar.nem_scenario
(
	year integer,
	state_abbr character varying(2),
	sector_abbr character varying(3),
	utility_type character varying(9),
	system_size_limit_kw double precision,
	--state_installed_capacitiy_limit_mw double precision, -- not yet enabled
	year_end_excess_sell_rate_dlrs_per_kwh numeric,
	hourly_excess_sell_rate_dlrs_per_kwh numeric
);


SET ROLE 'diffusion-writers';
DROP tABlE IF EXISTS diffusion_wind.nem_scenario;
CREATE TABLE diffusion_wind.nem_scenario
(
	year integer,
	state_abbr character varying(2),
	sector_abbr character varying(3),
	utility_type character varying(9),
	system_size_limit_kw double precision,
	--state_installed_capacitiy_limit_mw double precision, -- not yet enabled
	year_end_excess_sell_rate_dlrs_per_kwh numeric,
	hourly_excess_sell_rate_dlrs_per_kwh numeric
);

