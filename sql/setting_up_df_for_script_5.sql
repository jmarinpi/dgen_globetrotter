-- need to account for the following wonkiness in this process:
	-- naep = 0
		-- this is handled on input to scoe -- if aep = 0, infinity is returned
	-- maxheight_m_popdens = 0
		-- split these out of the analysis and stash them away some where
	-- load = 0
		-- could do this from the outset -- just ignore counties where 


-- randomly sample  100 points from each county (note: some counties will have fewer)
DROP TABLE IF EXISTS wind_ds.pt_grid_us_res_sample_10;
SET LOCAL SEED TO 1;
CREATE TABLE wind_ds.pt_grid_us_res_sample_10 AS
WITH a as (
	SELECT *, ROW_NUMBER() OVER (PARTITION BY county_id order by random()) as row_number
	FROM wind_ds.pt_grid_us_res_joined)
SELECT *
FROM a
where row_number <= 10;

-- link each point to a load bin
-- use random weighted sampling on the load bins to ensure that countyies with <100 points
-- have a representative sample of load bins
DROP TABLE IF EXISTS wind_ds.pt_grid_us_res_sample_load_10;
SET LOCAL SEED TO 1;
CREATE TABLE wind_ds.pt_grid_us_res_sample_load_10 AS
WITH weighted_county_sample as (
	SELECT a.county_id, row_number() OVER (PARTITION BY a.county_id ORDER BY random() * b.prob) as row_number, b.*
	FROM wind_ds.county_geom a
	LEFT JOIN wind_ds.binned_annual_load_kwh_10_bins b
	ON a.census_region = b.census_region
	AND b.sector = 'residential')
SELECT a.*, b.ann_cons_kwh, b.prob, b.weight,
	a.county_total_customers_2011 * b.weight/sum(weight) OVER (PARTITION BY a.county_id) as customers_in_bin, 
	a.county_total_load_mwh_2011 * 1000 * (b.ann_cons_kwh*b.weight)/sum(b.ann_cons_kwh*b.weight) OVER (PARTITION BY a.county_id) as load_kwh_in_bin
FROM wind_ds.pt_grid_us_res_sample_10 a
LEFT JOIN weighted_county_sample b
ON a.county_id = b.county_id
and a.row_number = b.row_number
where county_total_load_mwh_2011 > 0;



-- these data will stay the same through the rest of the analysis, so its worht the overhead of indexing them
-- create indices
ALTER TABLE wind_ds.pt_grid_us_res_sample_load_10 ADD PRIMARY Key (gid);
	--exclusions should be a variable
CREATE INDEX res_sample_load_10_maxheight_btree ON wind_ds.pt_grid_us_res_sample_load_10 USING btree(maxheight_m_popdens)
where maxheight_m_popdens > 0;
CREATE INDEX res_sample_load_10_census_division_abbr_btree ON wind_ds.pt_grid_us_res_sample_load_10 USING btree(census_division_abbr)



-- create yearly data
DROP TABLE IF EXISTS wind_ds.temporal_factors;
CREATE TABLE wind_ds.temporal_factors as 
SELECT a.year, a.nameplate_capacity_kw, a.power_curve_id,
	b.turbine_height_m,
	c.fixed_om_dollars_per_kw_per_yr, 
	c.variable_om_dollars_per_kwh,
	c.installed_costs_dollars_per_kw,
	d.census_division_abbr,
	d.sector,
	d.escalation_factor as rate_escalation_factor,
	d.source as rate_escalation_source,
	e.scenario as load_growth_scenario,
	e.load_multiplier	
FROM wind_ds.wind_performance_improvements a
-- find all turbine sizes associated with heights allowed at this location
LEFT JOIN wind_ds.allowable_turbine_sizes b
ON a.nameplate_capacity_kw = b.turbine_size_kw
-- join in costs
LEFT JOIN wind_ds.turbine_costs_per_size_and_year c
ON a.nameplate_capacity_kw = c.turbine_size_kw
and a.year = c.year
-- rate escalations
LEFT JOIN wind_ds.rate_escalations d
ON a.year = d.year
-- load growth
LEFT JOIN wind_ds.aeo_load_growth_projections e
ON d.census_division_abbr = e.census_division_abbr
and a.year = e.year;


CREATE INDEX temporal_factors_year_btree on wind_ds.temporal_factors using btree(year);
CREATE INDEX temporal_factors_turbine_height_m_btree on wind_ds.temporal_factors using btree(turbine_height_m);
CREATE INDEX temporal_factors_sector_btree ON wind_ds.temporal_factors using btree(sector);
CREATE INDEX temporal_factors_load_growth_scenario_btree ON wind_ds.temporal_factors using btree(load_growth_scenario);
CREATE INDEX temporal_factors_rate_escalation_source_btree ON wind_ds.temporal_factors USING btree(rate_escalation_source);
CREATE INDEX temporal_factors_census_division_abbr_btree ON wind_ds.temporal_factors USING btree(census_division_abbr);
-- 7 seconds to here


DROP TABLE IF EXISTS wind_ds.sample_all_years_10;
EXPLAIN ANALYZE
CREATE TABLE wind_ds.sample_all_years_10 AS

SELECT a.gid, 

	-- year
	f.year,

	-- location descriptors
	a.county_id, a.state_abbr, a.census_division_abbr, a.census_region, a.row_number, 

	-- exclusions
	a.maxheight_m_popdens as max_height, 

	-- rates
	a.elec_rate_cents_per_kwh, 
	i.escalation_factor,
	
	-- costs
	a.cap_cost_multiplier,
	g.fixed_om_dollars_per_kw_per_yr, 
	g.variable_om_dollars_per_kwh,
	g.installed_costs_dollars_per_kw * a.cap_cost_multiplier::numeric as installed_costs_dollars_per_kw,
	
	-- load and customers information
	a.ann_cons_kwh, a.prob, a.weight,
	j.load_multiplier * a.customers_in_bin as customers_in_bin, 
	a.customers_in_bin as initial_customers_in_bin, 
	j.load_multiplier * a.load_kwh_in_bin AS load_kwh_in_bin,
	a.load_kwh_in_bin AS initial_load_kwh_in_bin,
	
	case when a.customers_in_bin > 0 THEN a.load_kwh_in_bin/a.customers_in_bin 
	else 0
	end as load_kwh_per_customer_in_bin,

	-- wind resource data
	c.i, c.j, c.cf_bin, c.aep_scale_factor--, 
-- 	d.aep*c.aep_scale_factor*a.derate_factor as naep,
-- 	e.turbine_size_kw,
-- 	d.turbine_id, e.turbine_height_m
	
	FROM wind_ds.pt_grid_us_res_sample_load_10 a
	-- find all turbine sizes associated with heights allowed at this location
	LEFT JOIN wind_ds.allowable_turbine_sizes e
	ON e.turbine_height_m <= a.maxheight_m_popdens
	-- based on the turbine sizes identified above, find which turbine power curves to use (this is based on the start year in the where clause below)
	LEFT JOIN wind_ds.wind_performance_improvements f
	ON e.turbine_size_kw = f.nameplate_capacity_kw 
	-- find the i, j, and cfbin to use for this location
	LEFT JOIN wind_ds.ij_cfbin_lookup_res_pts_us c
	on a.gid = c.pt_gid
	-- link to the resource data based on:
		-- location: i, j, cf_bin
		-- allowable heights
		-- and turbine_power curve
-- 	LEFT JOIN wind_ds.wind_resource_annual d
-- 	ON c.i = d.i
-- 	and c.j = d.j
-- 	and c.cf_bin = d.cf_bin
-- 	and e.turbine_height_m = d.height
-- 	and f.power_curve_id = d.turbine_id
	-- join in costs
	LEFT JOIN wind_ds.turbine_costs_per_size_and_year g
	ON e.turbine_size_kw = g.turbine_size_kw
	and f.year = g.year
	-- rate escalations
	LEFT JOIN wind_ds.rate_escalations i
	ON f.year = i.year
		-- these will have to be variables in the script
	and i.sector = 'Residential'
	and i.source = 'AEO2014'
	and a.census_division_abbr = i.census_division_abbr
	-- load growth
	LEFT JOIN wind_ds.aeo_load_growth_projections j
	ON a.census_division_abbr = j.census_division_abbr
	and f.year = j.year
		-- this will be a variable in the script
	and j.scenario = 'AEO 2013 Reference Case'
		--exclusions should be a variable
	where a.maxheight_m_popdens > 0

-- 131999 ms

select distinct(scenario)
FROM wind_ds.aeo_load_growth_projections

-- next step would be to rank() on scoe partitioned by gid and pull distinct on gid ordered by scoe
-- then feed the data into Postgres as a data frame
DROP TABLE IF EXISTS wind_ds.sample_10;
CREATE TABLE wind_ds.sample_10 AS
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
	d.aep*c.aep_scale_factor*a.derate_factor as naep,
	e.turbine_size_kw,
	d.turbine_id, e.turbine_height_m
	
	FROM wind_ds.pt_grid_us_res_sample_load_10 a
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
