-- need to account for the following wonkiness in this process:
	-- naep = 0
		-- this is handled on input to scoe -- if aep = 0, infinity is returned
	-- maxheight_m_popdens = 0
		-- split these out of the analysis and stash them away some where
	-- load = 0
		-- could do this from the outset -- just ignore counties where 


-- randomly sample  100 points from each county (note: some counties will have fewer)
DROP TABLE IF EXISTS wind_ds.pt_grid_us_res_sample;
SET LOCAL SEED TO 1;
CREATE TABLE wind_ds.pt_grid_us_res_sample AS
WITH a as (
	SELECT *, ROW_NUMBER() OVER (PARTITION BY county_id order by random()) as row_number
	FROM wind_ds.pt_grid_us_res_joined)
SELECT *
FROM a
where row_number <= 100;

-- link each point to a load bin
-- use random weighted sampling on the load bins to ensure that countyies with <100 points
-- have a representative sample of load bins
DROP TABLE IF EXISTS wind_ds.pt_grid_us_res_sample_load;
SET LOCAL SEED TO 1;
CREATE TABLE wind_ds.pt_grid_us_res_sample_load AS
WITH weighted_county_sample as (
	SELECT a.county_id, row_number() OVER (PARTITION BY a.county_id ORDER BY random() * b.prob) as row_number, b.*
	FROM wind_ds.county_geom a
	LEFT JOIN wind_ds.binned_annual_load_kwh_100_bins b
	ON a.census_region = b.census_region
	AND b.sector = 'residential')
SELECT a.*, b.ann_cons_kwh, b.prob, b.weight,
	a.county_total_customers_2011 * b.weight/sum(weight) OVER (PARTITION BY a.county_id) as customers_in_bin, 
	a.county_total_load_mwh_2011 * 1000 * (b.ann_cons_kwh*b.weight)/sum(b.ann_cons_kwh*b.weight) OVER (PARTITION BY a.county_id) as load_kwh_in_bin
FROM wind_ds.pt_grid_us_res_sample a
LEFT JOIN weighted_county_sample b
ON a.county_id = b.county_id
and a.row_number = b.row_number
where county_total_load_mwh_2011 > 0;

-- these data will stay the same through the rest of the analysis, so its worht the overhead of indexing them
-- create indices
ALTER TABLE wind_ds.pt_grid_us_res_sample_load ADD PRIMARY Key (gid);
CREATE INDEX maxheight_btree ON wind_ds.pt_grid_us_res_sample_load USING btree(maxheight_m_popdens);

-- join in AEP values and calculate all valid resource values based on allowable turbine heights, power curves, and rated capacities
-- need to add in the following cost variables to this query from table g (need to ask Ben about these):
--            ic  - Installed Cost ($/kW)
--            fom - Fixed O&M ($/kW-yr)
--            vom - Variable O&M ($/kWh)
-- --         aep - Annual Elec Production (kWh/yr)
--            cap - Proposed capacity (kW)
--            aec - Annual Electricity Consumption (kWh/yr)
DROP TABLE wind_ds.sample;
-- EXPLAIN ANALYZE
CREATE TABLE wind_ds.sample AS
SELECT a.gid, 
	-- location descriptors
	a.county_id, a.state_abbr, a.census_division_abbr, a.census_region, a.row_number, 

	-- exclusions
	a.maxheight_m_popdens as max_height, 

	-- rates
	a.elec_rate_cents_per_kwh, 
	
	-- costs
	a.cap_cost_multiplier,
	g.fixed_om_dollars_per_kw_per_yr, 
	g.variable_om_dollars_per_kwh,
	g.installed_costs_dollars_per_kw * a.cap_cost_multiplier as installed_costs_dollars_per_kw,
	
	-- load and customers information
	a.ann_cons_kwh, a.prob, a.weight, 
	a.customers_in_bin, a.load_kwh_in_bin,
	case when a.customers_in_bin > 0 THEN a.load_kwh_in_bin/a.customers_in_bin 
	else 0
	end as load_kwh_per_customer_in_bin,

	-- wind resource data
	c.i, c.j, c.cf_bin, c.aep_scale_factor, 
	d.aep*c.aep_scale_factor as naep,
	e.turbine_size_kw,
	d.turbine_id, e.turbine_height_m,
	f.power_curve_id
	
FROM wind_ds.pt_grid_us_res_sample_load a
-- find all turbine sizes associated with heights allowed at this location
LEFT JOIN wind_ds.allowable_turbine_sizes e
ON e.turbine_height_m <= a.maxheight_m_popdens
-- based on the turbine sizes identified above, find which turbine power curves to use (this is based on the start year in the where clause below)
LEFT JOIN wind_ds.wind_performance_improvements f
ON e.turbine_size_kw = f.nameplate_capacity_kw 
and f.year = 2014
-- find the i, j, and cfbin to use for this location
LEFT JOIN wind_ds.ij_cfbin_lookup_res_pts_us c
on a.gid = c.pt_gid
-- link to the resource data based on:
	-- location: i, j, cf_bin
	-- allowable heights
	-- and turbine_power curve
LEFT JOIN wind_ds.wind_resource_annual d
ON c.i = d.i
and c.j = d.j
and c.cf_bin = d.cf_bin
and e.turbine_height_m = d.height
and f.power_curve_id = d.turbine_id
-- join in costs
LEFT JOIN wind_ds.turbine_costs_per_size_and_year g
ON e.turbine_size_kw = g.turbine_size_kw
and g.year = 2014

where a.maxheight_m_popdens > 0;

-- 123229 ms






-- next step would be to rank() on scoe partitioned by gid and pull distinct on gid ordered by scoe
-- then feed the data into Postgres as a data frame
DROP TABLE IF EXISTS wind_ds.sample2;
CREATE TABLE wind_ds.sample2 AS
with j as (
	SELECT a.gid, 
	-- location descriptors
	a.county_id, a.state_abbr, a.census_division_abbr, a.census_region, a.row_number, 

	-- exclusions
	a.maxheight_m_popdens as max_height, 

	-- rates
	a.elec_rate_cents_per_kwh, 
	
	-- costs
	a.cap_cost_multiplier,
	g.fixed_om_dollars_per_kw_per_yr, 
	g.variable_om_dollars_per_kwh,
	g.installed_costs_dollars_per_kw * a.cap_cost_multiplier::numeric as installed_costs_dollars_per_kw,
	
	-- load and customers information
	a.ann_cons_kwh, a.prob, a.weight, 
	a.customers_in_bin, a.load_kwh_in_bin,
	case when a.customers_in_bin > 0 THEN a.load_kwh_in_bin/a.customers_in_bin 
	else 0
	end as load_kwh_per_customer_in_bin,

	-- wind resource data
	c.i, c.j, c.cf_bin, c.aep_scale_factor, 
	d.aep*c.aep_scale_factor as naep,
	e.turbine_size_kw,
	d.turbine_id, e.turbine_height_m
	
	FROM wind_ds.pt_grid_us_res_sample_load a
	-- find all turbine sizes associated with heights allowed at this location
	LEFT JOIN wind_ds.allowable_turbine_sizes e
	ON e.turbine_height_m <= a.maxheight_m_popdens
	-- based on the turbine sizes identified above, find which turbine power curves to use (this is based on the start year in the where clause below)
	LEFT JOIN wind_ds.wind_performance_improvements f
	ON e.turbine_size_kw = f.nameplate_capacity_kw 
	and f.year = 2014
	-- find the i, j, and cfbin to use for this location
	LEFT JOIN wind_ds.ij_cfbin_lookup_res_pts_us c
	on a.gid = c.pt_gid
	-- link to the resource data based on:
		-- location: i, j, cf_bin
		-- allowable heights
		-- and turbine_power curve
	LEFT JOIN wind_ds.wind_resource_annual d
	ON c.i = d.i
	and c.j = d.j
	and c.cf_bin = d.cf_bin
	and e.turbine_height_m = d.height
	and f.power_curve_id = d.turbine_id
	-- join in costs
	LEFT JOIN wind_ds.turbine_costs_per_size_and_year g
	ON e.turbine_size_kw = g.turbine_size_kw
	and g.year = 2014

	where a.maxheight_m_popdens > 0),

k as (

SELECT j.*, wind_ds.scoe(j.installed_costs_dollars_per_kw, j.fixed_om_dollars_per_kw_per_yr, j.variable_om_dollars_per_kwh, j.naep , j.turbine_size_kw , j.load_kwh_per_customer_in_bin , 1.15, 0.5) as scoe
from j)

SELECT distinct on (k.gid) k.gid, k.county_id, k.state_abbr, k.census_division_abbr, k.census_region, 
       k.row_number, k.max_height, k.elec_rate_cents_per_kwh, k.cap_cost_multiplier, 
       k.fixed_om_dollars_per_kw_per_yr, k.variable_om_dollars_per_kwh, 
       k.installed_costs_dollars_per_kw, k.ann_cons_kwh, k.prob, k.weight, k.customers_in_bin, 
       k.load_kwh_in_bin, k.load_kwh_per_customer_in_bin, k.i, k.j, k.cf_bin, 
       k.aep_scale_factor, k.naep, k.turbine_size_kw, k.turbine_id, k.turbine_height_m, k.scoe
FROM k
order by k.gid, k.scoe asc;
-- 331143 ms
-- 273507 rows

-- check row count
SELECT distinct(gid)
FROM wind_ds.pt_grid_us_res_sample_load
where maxheight_m_popdens > 0
