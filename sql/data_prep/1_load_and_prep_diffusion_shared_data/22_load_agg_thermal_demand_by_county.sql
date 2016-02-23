set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_shared.county_thermal_demand;
CREATE TABLE diffusion_shared.county_thermal_demand
(
	state_fips varchar(2),
	county_fips varchar(3),
	fips varchar(5),
	state text,
	county text,
	space_heating_thermal_load_tbtu numeric,
	water_heating_thermal_load_tbtu numeric,
	total_heating_thermal_load_tbtu numeric,
	sector_abbr varchar(3)
);

-- load residential data
\COPY diffusion_shared.county_thermal_demand FROM '/Volumes/Staff/mgleason/dGeo/Data/Source_Data/County_Thermal_Demand_kmccabe/simplified/residential_from_housing_units_2016_02_16.csv' with csv header;

-- fix fips codes (left pad)
update diffusion_shared.county_thermal_demand
set state_fips = lpad(state_fips, 2, '0');

update diffusion_shared.county_thermal_demand
set county_fips = lpad(county_fips, 3, '0');

update diffusion_shared.county_thermal_demand
set fips = lpad(fips, 5, '0');

-- check count and compare to county_geom table
select count(*)
FROM diffusion_shared.county_thermal_demand;
-- 3142 counties

-- how many in county geom table?
select count(*)
FROM diffusion_shared.county_geom;
-- 3141 -- good sign!

-- which county_geom rows are missing from thermal demand table?
select *
from diffusion_shared.county_geom a
left join diffusion_shared.county_thermal_demand b
ON lpad(a.state_fips::TEXT, 2, '0') = b.state_fips
and a.county_fips = b.county_fips
where b.county_fips is null;
-- Skagway-Hoonah-Angoon,Alaska
-- Bedford City,Virginia
-- Wrangell-Petersburg,Alaska
-- Prince of Wales-Outer Ketchikan,Alaska


-- which thermal demand rows are missing from county_geom table
-- which county_geom rows are missing from thermal demand table?
select *
from diffusion_shared.county_thermal_demand b
left join diffusion_shared.county_geom a
ON lpad(a.state_fips::TEXT, 2, '0') = b.state_fips
and a.county_fips = b.county_fips
where a.county_fips is null;
-- Alaska,Hoonah-Angoon Census Area
-- Alaska,Petersburg Borough
-- Alaska,Prince of Wales-Hyder Census Area
-- Alaska,Skagway Municipality
-- Alaska,Wrangell City and Borough

-- should be able to fix as follows
-- Hoonah-Angoon Census Area + Skagway Municipality = Skagway-Hoonah-Angoon,Alaska
-- Wrangell City and Borough + Alaska,Petersburg Borough = Wrangell-Petersburg,Alaska
-- Prince of Wales-Hyder Census Area = Prince of Wales-Outer Ketchikan,Alaska

ALTER TABLE diffusion_shared.county_thermal_demand
ADD COLUMN county_id integer;

UPDATE diffusion_shared.county_thermal_demand a
set county_id = b.county_id
from diffusion_shared.county_geom b
where a.state_fips = lpad(b.state_fips::TEXT, 2, '0')
and a.county_fips = b.county_fips;

-- should be 5 nulls
select *
from diffusion_shared.county_thermal_demand
where county_id is null;

--
select *
FROM diffusion_shared.county_thermal_demand
where state_fips = '51'

--
select *
FROM diffusion_shared.county_geom
where state_fips = 51
and county_fips = '019'

select *
from diffusion_shared.load_and_customers_by_county_us
where county_id = 2694
