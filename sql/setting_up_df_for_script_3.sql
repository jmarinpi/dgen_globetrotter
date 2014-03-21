-- randomly sample  100 points from each county (note: some counties will have fewer)
DROP TABLE IF EXISTS wind_ds.wind_ds.pt_grid_us_res_sample;
CREATE TABLE wind_ds.wind_ds.pt_grid_us_res_sample
SET LOCAL SEED TO 1;
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
	a.county_total_load_mwh_2011 * (b.ann_cons_kwh*b.weight)/sum(b.ann_cons_kwh*b.weight) OVER (PARTITION BY a.county_id) as load_mwh_in_bin
FROM wind_ds.pt_grid_us_res_sample a
LEFT JOIN weighted_county_sample b
ON a.county_id = b.county_id
and a.row_number = b.row_number;

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
EXPLAIN ANALYZE
CREATE TABLE wind_ds.sample AS
SELECT a.gid, a.county_id, a.maxheight_m_popdens as max_height, 
	a.elec_rate_cents_per_kwh, a.cap_cost_multiplier,
	a.state_abbr, a.census_division_abbr, a.census_region, a.row_number, 
	a.ann_cons_kwh, a.prob, a.weight, 
	a.customers_in_bin, a.load_mwh_in_bin,
-- 	a.load_mwh_in_bin/a.customers_in_bin as load_mwh_per_customer_in_bin,
	c.i, c.j, c.cf_bin, c.aep_scale_factor, 
	d.aep*c.aep_scale_factor*turbine_size_kw as aep, 
	d.turbine_id, e.turbine_size_kw, e.turbine_height_m
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
LEFT JOIN wind_ds.wind_cost_projections g
ON e.turbine_size_kw = g.turbine_size_kw
and g.year = 2014;

-- next step would be to rank() on scoe partitioned by gid and pull distinct on gid ordered by scoe
-- then feed the data into Postgres as a data frame
-- could further subset on:
-- and e.turbine_size_kw*1000
--  cf_min * turbine_size_kw *(8760/1000) < oversize_factor * load_mwh