SET ROLE 'diffusion-writers';
-- full everywhere
DROP tABlE IF EXISTS diffusion_shared.nem_scenario_full_everywhere;
CREATE TABLE diffusion_shared.nem_scenario_full_everywhere AS
-- get states from state_fips_lkup table
SELECT generate_series(2014,2050,2) as year,
	state_abbr, 
	unnest(array['res','com','ind']) as sector_abbr,
       unnest(array['All Other', 'Coop', 'IOU', 'Muni']) as utility_type,
       'Inf'::double precision as system_size_limit_kw,
       0::numeric as year_end_excess_sell_rate_dlrs_per_kwh,
       0::numeric hourly_excess_sell_rate_dlrs_per_kwh

FROM diffusion_shared.state_fips_lkup
where state_abbr <> 'PR';

-- none everywhere
DROP tABlE IF EXISTS diffusion_shared.nem_scenario_none_everywhere;
CREATE TABLE diffusion_shared.nem_scenario_none_everywhere AS
-- get states from state_fips_lkup table
SELECT generate_series(2014,2050,2) as year,
	state_abbr, 
	unnest(array['res','com','ind']) as sector_abbr,
       unnest(array['All Other', 'Coop', 'IOU', 'Muni']) as utility_type,
       0::double precision as system_size_limit_kw,
       0::numeric as year_end_excess_sell_rate_dlrs_per_kwh,
       0::numeric hourly_excess_sell_rate_dlrs_per_kwh
FROM diffusion_shared.state_fips_lkup
where state_abbr <> 'PR';

-- BAU
-- old version
-- DROP tABlE IF EXISTS diffusion_shared.nem_scenario_bau_old;
-- CREATE TABLE diffusion_shared.nem_scenario_bau_old AS
-- -- get states from state_fips_lkup table
-- SELECT generate_series(2014,2050,2) as year,
-- 	state_abbr, 
--        sector as sector_abbr,
--        utility_type,
--        nem_system_limit_kw as system_size_limit_kw,
--        0::numeric as year_end_excess_sell_rate_dlrs_per_kwh,
--        0::numeric hourly_excess_sell_rate_dlrs_per_kwh
-- FROM diffusion_shared.net_metering_availability_2013;

--- new version
DROP TABLE IF EXISTS diffusion_shared.net_metering_availability_2015;
CREATE TABLE diffusion_shared.net_metering_availability_2015
(
	state_abbr character varying(2),
	exp_year integer,
	sector_abbr character varying(3),
	nem_system_limit_kw DOUBLE PRECISION
);

\COPY diffusion_shared.net_metering_availability_2015 FROM '/Volumes/Staff/mgleason/DG_Solar/Data/Source_Data/nem_update_20151014/nem_bau_update_20151014_reformat.csv' with csv header;

DROP tABlE IF EXISTS diffusion_shared.nem_scenario_bau CASCADE;
CREATE TABLE diffusion_shared.nem_scenario_bau AS
with a as
(
	SELECT generate_series(2014, exp_year, 2) as year,
		state_abbr, 
	       sector_abbr,
	       nem_system_limit_kw as system_size_limit_kw,
	       0::numeric as year_end_excess_sell_rate_dlrs_per_kwh,
	       0::numeric hourly_excess_sell_rate_dlrs_per_kwh
	FROM diffusion_shared.net_metering_availability_2015
)
select year, state_abbr, sector_abbr,
	       unnest(array['All Other', 'Coop', 'IOU', 'Muni']) as utility_type,
	       system_size_limit_kw, year_end_excess_sell_rate_dlrs_per_kwh,
	       hourly_excess_sell_rate_dlrs_per_kwh
from a;

select count(*)
FROM diffusion_shared.nem_scenario_bau;
-- 6960

-- make sure count matches for full everywhere and none everywhere
select count(*)
FROM diffusion_shared.nem_scenario_full_everywhere;
-- 11628
select count(*)
FROM diffusion_shared.nem_scenario_none_everywhere;
-- 11628
