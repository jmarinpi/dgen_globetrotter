﻿set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_shared.county_thermal_demand;
CREATE TABLE diffusion_shared.county_thermal_demand
(
	state_fips varchar(2),
	county_fips varchar(3),
	fips varchar(5),
	state text,
	county text,
	housing_units integer,
	county_weight numeric,
	space_heating_thermal_load_tbtu numeric,
	water_heating_thermal_load_tbtu numeric,
	space_cooling_thermal_load_tbtu numeric
);

------------------------------------------------------------------------------------------------
-- RESIDENTIAL

-- load residential data
\COPY diffusion_shared.county_thermal_demand FROM '/Volumes/Staff/mgleason/dGeo/Data/Source_Data/Thermal_Demand_kmccabe/simplified/residential_from_housing_units_2016_02_29.csv' with csv header;

-- add total_heating_thermal_load_tbtu numeric,
ALTER TABLE diffusion_shared.county_thermal_demand
ADD COLUMN total_heating_thermal_load_tbtu numeric;

UPDATE diffusion_shared.county_thermal_demand
set total_heating_thermal_load_tbtu = space_heating_thermal_load_tbtu + water_heating_thermal_load_tbtu;
-- 3143 rows


-- add sector_abbr column
ALTER TABLE diffusion_shared.county_thermal_demand
ADD COLUMN sector_abbr varchar(3);

UPDATE diffusion_shared.county_thermal_demand
set sector_abbr = 'res';
-- 3143 rows


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
-- 3143 counties

-- how many in county geom table?
select count(*)
FROM diffusion_shared.county_geom;
-- 3141 -- close, good sign, but still a few counties must be off

-- -- which county_geom rows are missing from thermal demand table?
-- select *
-- from diffusion_shared.county_geom a
-- left join diffusion_shared.county_thermal_demand b
-- ON lpad(a.state_fips::TEXT, 2, '0') = b.state_fips
-- and a.county_fips = b.county_fips
-- where b.county_fips is null;
-- -- 9,Skagway-Hoonah-Angoon,Alaska,2,232
-- -- 4,Wrangell-Petersburg,Alaska,2,280
-- -- 3,Prince of Wales-Outer Ketchikan,Alaska,2,201
-- 
-- -- which thermal demand rows are missing from county_geom table
-- -- which county_geom rows are missing from thermal demand table?
-- select *
-- from diffusion_shared.county_thermal_demand b
-- left join diffusion_shared.county_geom a
-- ON lpad(a.state_fips::TEXT, 2, '0') = b.state_fips
-- and a.county_fips = b.county_fips
-- where a.county_fips is null;
-- -- Alaska,Hoonah-Angoon Census Area
-- -- Alaska,Petersburg Borough
-- -- Alaska,Prince of Wales-Hyder Census Area
-- -- Alaska,Skagway Municipality
-- -- Alaska,Wrangell City and Borough
-- 
-- -- should be able to fix as follows
-- -- Hoonah-Angoon Census Area + Skagway Municipality = Skagway-Hoonah-Angoon,Alaska 2,232
-- -- Wrangell City and Borough + Alaska,Petersburg Borough = Wrangell-Petersburg,Alaska 2,280
-- -- Prince of Wales-Hyder Census Area = Prince of Wales-Outer Ketchikan,Alaska 2,201
-- 
-- ALTER TABLE diffusion_shared.county_thermal_demand
-- ADD COLUMN county_id integer;
-- 
-- UPDATE diffusion_shared.county_thermal_demand a
-- set county_id = b.county_id
-- from diffusion_shared.county_geom b
-- where a.state_fips = lpad(b.state_fips::TEXT, 2, '0')
-- and a.county_fips = b.county_fips;
-- -- 3138 rows
-- 
-- -- should be 5 nulls
-- select *
-- from diffusion_shared.county_thermal_demand
-- where county_id is null;
-- 
-- -- Hoonah-Angoon Census Area + Skagway Municipality = Skagway-Hoonah-Angoon,Alaska 2,232 (county_id = 9)
-- INSERT INTO diffusion_shared.county_thermal_demand 
-- select '02' as state_fips, '232' as county_fips, '02323' as fips,
-- 	'Alaska' as state, 'Skagway-Hoonah-Angoon' as county,
-- 	sum(housing_units) as housing_units, sum(county_weight) as county_weight,
-- 	sum(space_heating_thermal_load_tbtu) as space_heating_thermal_load_tbtu,
-- 	sum(water_heating_thermal_load_tbtu) as water_heating_thermal_load_tbtu,
-- 	sum(space_cooling_thermal_load_tbtu) as space_cooling_thermal_load_tbtu,
-- 	sum(total_heating_thermal_load_tbtu) as total_heating_thermal_load_tbtu,
-- 	'res' as sector_abbr, 9 as county_id
-- from diffusion_shared.county_thermal_demand
-- where county in ('Hoonah-Angoon Census Area', 'Skagway Municipality');
-- 
-- -- Wrangell City and Borough + Alaska,Petersburg Borough = Wrangell-Petersburg,Alaska 2,280 (county_id = 4)
-- INSERT INTO diffusion_shared.county_thermal_demand
-- select '02' as state_fips, '280' as county_fips, '02280' as fips,
-- 	'Alaska' as state, 'Wrangell-Petersburg' as county,
-- 	sum(housing_units) as housing_units, sum(county_weight) as county_weight,
-- 	sum(space_heating_thermal_load_tbtu) as space_heating_thermal_load_tbtu,
-- 	sum(water_heating_thermal_load_tbtu) as water_heating_thermal_load_tbtu,
-- 	sum(space_cooling_thermal_load_tbtu) as space_cooling_thermal_load_tbtu,
-- 	sum(total_heating_thermal_load_tbtu) as total_heating_thermal_load_tbtu,
-- 	'res' as sector_abbr, 4 as county_id
-- from diffusion_shared.county_thermal_demand
-- where county in ('Wrangell City and Borough', 'Petersburg Census Area');
-- 
-- 
-- -- Prince of Wales-Hyder Census Area = Prince of Wales-Outer Ketchikan,Alaska 2,201 (county_id = 3)
-- INSERT INTO diffusion_shared.county_thermal_demand
-- select '02' as state_fips, '201' as county_fips, '02201' as fips,
-- 	'Alaska' as state, 'Prince of Wales-Outer Ketchikan' as county,
-- 	sum(housing_units) as housing_units, sum(county_weight) as county_weight,
-- 	sum(space_heating_thermal_load_tbtu) as space_heating_thermal_load_tbtu,
-- 	sum(water_heating_thermal_load_tbtu) as water_heating_thermal_load_tbtu,
-- 	sum(space_cooling_thermal_load_tbtu) as space_cooling_thermal_load_tbtu,
-- 	sum(total_heating_thermal_load_tbtu) as total_heating_thermal_load_tbtu,
-- 	'res' as sector_abbr, 3 as county_id
-- from diffusion_shared.county_thermal_demand
-- where county  = 'Prince of Wales-Hyder Census Area';
-- 
-- -- delete the old nulls
-- DELETE from diffusion_shared.county_thermal_demand
-- where county_id is null;
-- -- 5 rows
-- 
-- -- cehck row count
-- select count(*)
-- FROM diffusion_shared.county_thermal_demand;
-- -- 3141
-- 
-- -- confirm all counties are covered now by checking alignment between datasets
-- select *
-- from diffusion_shared.county_thermal_demand b
-- left join diffusion_shared.county_geom a
-- ON lpad(a.state_fips::TEXT, 2, '0') = b.state_fips
-- and a.county_fips = b.county_fips
-- where a.county_fips is null;
-- -- all set
-- 
-- -- try join on county_id
-- select *
-- from diffusion_shared.county_thermal_demand b
-- left join diffusion_shared.county_geom a
-- ON a.county_id = b.county_id
-- where b.county_id is null;
-- 
-- -- reverse direction
-- select *
-- from diffusion_shared.county_geom a
-- left join diffusion_shared.county_thermal_demand b
-- ON a.county_id = b.county_id
-- where b.county_id is null;
-- -- all set
-- 
-- -- make sure no nulls in the numeric values
-- select count(*)
-- FROM diffusion_shared.county_thermal_demand
-- where space_heating_thermal_load_tbtu IS NULL OR
-- 	water_heating_thermal_load_tbtu IS NULL OR
-- 	space_cooling_thermal_load_tbtu IS NULL OR
-- 	total_heating_thermal_load_tbtu IS NULL ;
-- -- 0, all set!

-- check for cases where water heating is more than space heating
select *
FROM diffusion_shared.county_thermal_demand
where space_heating_thermal_load_tbtu < water_heating_thermal_load_tbtu;
-- 82 rows
-- all in FL and AZ -- makes sense

-- how about cooling exceeding space heating?
select *
FROM diffusion_shared.county_thermal_demand
where space_heating_thermal_load_tbtu < space_cooling_thermal_load_tbtu;
-- all in FL and AZ -- makes sense
-- same 82 rows 

-- how many total rows in AZ and FL?
select count(*)
FROM diffusion_shared.county_thermal_demand
where state in ('Arizona', 'Florida');
-- 82 rows
-- this makes sense -- the aggregate state totals must have shown 
-- cooling > space heating and water heating > space heating
-- and this was applied universally to all counties in the states
------------------------------------------------------------------------------------------------

