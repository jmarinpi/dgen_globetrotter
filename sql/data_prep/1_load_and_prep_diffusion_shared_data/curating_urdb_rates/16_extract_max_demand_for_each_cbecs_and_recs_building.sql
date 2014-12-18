-- calculate and load the max normalized demand for each building type
-- and TMY station to  diffusion_shared.energy_plus_max_normalized_demand_res
-- and diffusion_shared.energy_plus_max_normalized_demand_com
-- using python/load/extract_max_normalized_demand_from_eplus.py

---------------------------------------------------------------------------
-- check that the outputs make sense

-- first check that # of stations and building types is correct
-- there are 936 stations with 3 missing for com and 5 missing for res
-- there are 3 reference building for res -- expect 3*(936-5) = 2793 rows
-- there are 16 reference buildings for com -- expect 16*(936-3) = 14928 rows
select count(*)
FROM diffusion_shared.energy_plus_max_normalized_demand_res;
-- 2793

select count(*)
FROM diffusion_shared.energy_plus_max_normalized_demand_com;
-- 14928 rows
-- row counts match

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


---------------------------------------------------------------------------
-- produce tables with actual max demand values for cbecs and recs
-- based on cbecs/recs annual kwh totals and eplus max normalized demand values
-- for each tmy station and eia region

-- residential
DROP TABLE IF EXISTS diffusion_shared.recs_2009_max_demand_by_tmy_and_eia_region;
CREATE TABLE diffusion_shared.recs_2009_max_demand_by_tmy_and_eia_region AS
with a as
(
	-- find the distinct combinations of tmy stations and recs regions based on
	-- intersections with residential point locs
	SELECT a.hdf_load_index, b.recs_2009_reportable_domain
	FROM diffusion_shared.pt_grid_us_res a
	left join diffusion_shared.county_geom b
	on a.county_id = b.county_id
	group by a.hdf_load_index, b.recs_2009_reportable_domain
),
b as 
(
	-- extract the single family, owner occupied samples from RECS
	SELECT doeid, reportable_domain, kwh
	FROM diffusion_shared.eia_microdata_recs_2009
	where typehuq in (1,2)
	and kownrent = 1
)
SELECT  a.hdf_load_index, a.recs_2009_reportable_domain, b.doeid,
	b.kwh*c.normalized_max_demand_kw_per_kw as max_demand_kw
FROM a
LEFT JOIN b -- join the recs samples on reportable domain
ON a.recs_2009_reportable_domain = b.reportable_domain
LEFT JOIN diffusion_shared.energy_plus_max_normalized_demand_res c -- join the eplus normalized max demand
ON a.hdf_load_index = c.hdf_index
and c.crb_model = 'reference'; -- only pull from reference building for residential

-- make sure that no samples are missing max_demand_kw
SELECT count(*)
FROM diffusion_shared.recs_2009_max_demand_by_tmy_and_eia_region
where max_demand_kw is null;
-- 0 -- all set

-- commercial/industrial
DROP TABLE IF EXISTS diffusion_shared.cbecs_2003_max_demand_by_tmy_and_eia_region;
CREATE TABLE diffusion_shared.cbecs_2003_max_demand_by_tmy_and_eia_region AS
with a as
(
	-- find the distinct combinations of tmy stations and recs regions based on
	-- intersections with commercial and industrial point locs
	SELECT a.hdf_load_index, b.census_division_abbr
	FROM diffusion_shared.pt_grid_us_com a
	left join diffusion_shared.county_geom b
	on a.county_id = b.county_id
	group by a.hdf_load_index, b.census_division_abbr
	UNION -- this should prevent duplicate pairs from being found
	SELECT a.hdf_load_index, b.census_division_abbr
	FROM diffusion_shared.pt_grid_us_ind a
	left join diffusion_shared.county_geom b
	on a.county_id = b.county_id
	group by a.hdf_load_index, b.census_division_abbr
),
b as 
(
	-- extract the single family, owner occupied samples from RECS
	SELECT a.pubid8, a.census_division_abbr, b.crb_model,
		a.elcns8 as kwh
	FROM diffusion_shared.eia_microdata_cbecs_2003 a
	LEFT JOIN diffusion_shared.cbecs_2003_crb_lookup b
	ON a.pubid8 = b.pubid8
	where pba8 <> 1
)
SELECT  a.hdf_load_index, a.census_division_abbr, b.pubid8,
	b.crb_model,
	b.kwh*c.normalized_max_demand_kw_per_kw as max_demand_kw
FROM a
LEFT JOIN b -- join the recs samples on reportable domain
ON a.census_division_abbr = b.census_division_abbr
LEFT JOIN diffusion_shared.energy_plus_max_normalized_demand_com c -- join the eplus normalized max demand
ON a.hdf_load_index = c.hdf_index
and b.crb_model = c.crb_model; -- match to the correct crb model for the cbecs sample

-- make sure that no samples are missing max_demand_kw
SELECT count(*)
FROM diffusion_shared.cbecs_2003_max_demand_by_tmy_and_eia_region
where max_demand_kw is null;
-- 0 -- all set