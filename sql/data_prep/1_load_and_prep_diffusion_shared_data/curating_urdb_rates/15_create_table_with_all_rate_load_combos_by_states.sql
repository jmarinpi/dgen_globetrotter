

------------------------------------------------------------------------
-- find all multiplicative combinations by state: 
-- rates (assume each rate could apply anywhere in the state)
-- hdf indices
-- eia regions
DROP TABLE if exists diffusion_shared.applicable_rate_load_combinations_com;
CREATE TABLE diffusion_shared.applicable_rate_load_combinations_com AS
with a as 
(
	-- find the unique combinations of:
	-- state, hdf_load_index (tmy station), and eia_region
	-- for commercial point locs
	SELECT b.state_abbr, a.hdf_load_index, b.census_division_abbr
	from diffusion_shared.pt_grid_us_com a
	left join diffusion_shared.county_geom b
	on a.county_id = b.county_id
	group by b.state_abbr, a.hdf_load_index, b.census_division_abbr	
),
b as 
(
	-- find all unique rates in each state
	SELECT c.state_abbr, a.urdb_rate_id
	from diffusion_shared.curated_urdb_rates_lookup_pts_com a
	LEFT JOIN diffusion_shared.pt_grid_us_com b
	on a.pt_gid = b.gid
	left join diffusion_shared.county_geom c
	ON b.county_id = c.county_id
	group by c.state_abbr, a.urdb_rate_id
),
c as
(
	SELECT a.state_abbr, a.hdf_load_index, a.census_division_abbr,
		c.pubid8, c.max_demand_kw as eia_max_demand_kw,
		b.urdb_rate_id, 
		d.demand_min as urdb_demand_min, d.demand_max as urdb_demand_max,
		d.rate_type,
		(c.max_demand_kw >= d.demand_min  and c.max_demand_kw <= d.demand_max) as rate_applies_to_load
	FROM a
	LEFT JOIN b
	ON a.state_abbr = b.state_abbr
	LEFT JOIN diffusion_shared.cbecs_2003_max_demand_by_tmy_and_eia_region c
	ON a.hdf_load_index = c.hdf_load_index
	and a.census_division_abbr = c.census_division_abbr
	LEFT JOIN urdb_rates.combined_singular_verified_rates_lookup d
	on b.urdb_rate_id = d.urdb_rate_id
	and d.res_com = 'C'
)
SELECT state_abbr, hdf_load_index, census_division_abbr,
	pubid8, eia_max_demand_kw,
	urdb_demand_min, urdb_demand_max,
	rate_type
FROM c
where rate_applies_to_load = true;


