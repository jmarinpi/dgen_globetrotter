---------------------------------------------------------------------------------------------------
-- for normalized max demand commercial tables, change "super_market" to "supermarket"
UPDATE diffusion_load_profiles.energy_plus_max_normalized_demand_space_cooling_com
set crb_model = 'supermarket'
where  crb_model = 'super_market';
-- 935

UPDATE diffusion_load_profiles.energy_plus_max_normalized_demand_space_heating_com
set crb_model = 'supermarket'
where  crb_model = 'super_market';
-- 935

UPDATE diffusion_load_profiles.energy_plus_max_normalized_demand_water_heating_com
set crb_model = 'supermarket'
where  crb_model = 'super_market';
-- 935

UPDATE diffusion_load_profiles.energy_plus_max_normalized_demand_water_and_space_heating_com
set crb_model = 'supermarket'
where  crb_model = 'super_market';
-- 935

---------------------------------------------------------------------------------------------------
-- check that # of stations and building types is correct
-- there are 936 stations with 3 missing for com and 5 missing for res
-- there are 3 reference building for res -- expect 3*(936-5) = 2793 rows
-- there are 16 reference buildings for com -- expect 16*(936-3) = 14928 rows

-- RES -- expect 2793 for all
select count(*)
FROM diffusion_load_profiles.energy_plus_max_normalized_demand_space_cooling_res
UNION
select count(*)
FROM diffusion_load_profiles.energy_plus_max_normalized_demand_space_heating_res
UNION
select count(*)
FROM diffusion_load_profiles.energy_plus_max_normalized_demand_water_heating_res
UNION
select count(*)
FROM diffusion_load_profiles.energy_plus_max_normalized_demand_water_and_space_heating_res
UNION
select count(*)
FROM diffusion_load_profiles.energy_plus_normalized_space_cooling_res
UNION
select count(*)
FROM diffusion_load_profiles.energy_plus_normalized_space_heating_res
UNION
select count(*)
FROM diffusion_load_profiles.energy_plus_normalized_water_heating_res
UNION
select count(*)
FROM diffusion_load_profiles.energy_plus_normalized_water_and_space_heating_res;
-- 2793 -- all set

-- COM -- expect 14928 for all
select count(*)
FROM diffusion_load_profiles.energy_plus_max_normalized_demand_space_cooling_com
UNION
select count(*)
FROM diffusion_load_profiles.energy_plus_max_normalized_demand_space_heating_com
UNION
select count(*)
FROM diffusion_load_profiles.energy_plus_max_normalized_demand_water_heating_com
UNION
select count(*)
FROM diffusion_load_profiles.energy_plus_max_normalized_demand_water_and_space_heating_com;
-- 14960 -- what gives? = 16*(936-1) instead of the expected 16*(936-3)

-- apparently two of the ids have data that are absent from the electric load data
select *
from diffusion_load_profiles.energy_plus_max_normalized_demand_water_and_space_heating_com a
left join diffusion_load_profiles.energy_plus_max_normalized_demand_com b -- electricity
ON a.hdf_index = b.hdf_index
and a.crb_model = b.crb_model
where b.crb_model is null; 
-- hdf_index 189 and 190 -- so I think this should be all set
---------------------------------------------------------------------------------------------------


-- how about building types?
SELECT distinct(crb_model)
FROM diffusion_shared.energy_plus_max_normalized_demand_res;
-- 3 types
SELECT distinct(crb_model)
FROM diffusion_shared.energy_plus_max_normalized_demand_com;
-- 16 types
-- NOTE: need to fix 'super_market' to 'supermarket' for consistency with 
-- diffusion_shared.cbecs_2003_pba_to_eplus_crbs
-- and
-- diffusion_shared.cbecs_2003_crb_lookup 
UPDATE diffusion_shared.energy_plus_max_normalized_demand_com
set crb_model = 'supermarket'
where crb_model = 'super_market';

-- check that hte values are reasonable
-- residential
with a as
(
	select normalized_max_demand_kw_per_kw*annual_sum_kwh as max_demand_kw
	FROM diffusion_shared.energy_plus_max_normalized_demand_res
	where crb_model = 'reference'
)
SELECT min(max_demand_kw), avg(max_demand_kw), max(max_demand_kw)
FROM a;
-- seems within range of what we'd expect

-- commercial
with a as
(
	select normalized_max_demand_kw_per_kw*annual_sum_kwh as max_demand_kw
	FROM diffusion_shared.energy_plus_max_normalized_demand_com
)
SELECT min(max_demand_kw), avg(max_demand_kw), max(max_demand_kw)
FROM a;
-- also seems reasonable
---------------------------------------------------------------------------


