-- need to account for the following wonkiness in this process:
	-- naep = 0
		-- this should NEVER occur
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
CREATE INDEX res_sample_load_10_census_division_abbr_btree ON wind_ds.pt_grid_us_res_sample_load_10 USING btree(census_division_abbr);
CREATE INDEX res_sample_load_10_i_j_cf_bin ON wind_ds.pt_grid_us_res_sample_load_10 using btree(i,j,cf_bin);


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


-- CREATE INDEX temporal_factors_year_btree on wind_ds.temporal_factors using btree(year);
CREATE INDEX temporal_factors_turbine_height_m_btree on wind_ds.temporal_factors using btree(turbine_height_m);
CREATE INDEX temporal_factors_sector_btree ON wind_ds.temporal_factors using btree(sector);
CREATE INDEX temporal_factors_load_growth_scenario_btree ON wind_ds.temporal_factors using btree(load_growth_scenario);
CREATE INDEX temporal_factors_rate_escalation_source_btree ON wind_ds.temporal_factors USING btree(rate_escalation_source);
CREATE INDEX temporal_factors_census_division_abbr_btree ON wind_ds.temporal_factors USING btree(census_division_abbr);
-- 7 seconds to here


DROP TABLE IF EXISTS wind_ds.sample_all_years_10;
CREATE TABLE wind_ds.sample_all_years_10 AS
SELECT -- point descriptors
	a.gid, b.year, a.county_id, a.state_abbr, a.census_division_abbr, a.census_region, a.row_number, 
 	-- exclusions
 	a.maxheight_m_popdens as max_height, 
 
 	-- rates
	a.elec_rate_cents_per_kwh * b.rate_escalation_factor as elec_rate_cents_per_kwh, 

 	-- costs
	a.cap_cost_multiplier,
	b.fixed_om_dollars_per_kw_per_yr, 
	b.variable_om_dollars_per_kwh,
	b.installed_costs_dollars_per_kw * a.cap_cost_multiplier::numeric as installed_costs_dollars_per_kw,
	
	-- load and customers information
	a.ann_cons_kwh, a.prob, a.weight,
	b.load_multiplier * a.customers_in_bin as customers_in_bin, 
	a.customers_in_bin as initial_customers_in_bin, 
	b.load_multiplier * a.load_kwh_in_bin AS load_kwh_in_bin,
	a.load_kwh_in_bin AS initial_load_kwh_in_bin,

	-- load per customer stays static throughout time
	case when a.customers_in_bin > 0 THEN a.load_kwh_in_bin/a.customers_in_bin 
	else 0
	end as load_kwh_per_customer_in_bin,

	-- wind resource data
	a.i, a.j, a.cf_bin, a.aep_scale_factor, 
	c.aep*a.aep_scale_factor*a.derate_factor as naep,
	b.nameplate_capacity_kw,
	b.power_curve_id, 
	b.turbine_height_m
	
	FROM wind_ds.pt_grid_us_res_sample_load_10 a
	-- join in temporally varying factors
	LEFT JOIN wind_ds.temporal_factors b
	ON b.turbine_height_m <= a.maxheight_m_popdens 
	and a.census_division_abbr = b.census_division_abbr
	-- join in wind resource data
	LEFT JOIN wind_ds.wind_resource_annual c
	ON a.i = c.i
	and a.j = c.j
	and a.cf_bin = c.cf_bin
	and b.turbine_height_m = c.height
	and b.power_curve_id = c.turbine_id
	-- these will all be variables in the model script
	where b.sector = 'Residential'
	and a.maxheight_m_popdens > 0	
	and b.rate_escalation_source = 'AEO2014'
	and b.load_growth_scenario = 'AEO 2013 Reference Case';
	

-- 91 seconds after indexing wind resource child tables


-- compress down to best option for each year
-- DROP TABLE IF EXISTS wind_ds.sample_best_option_each_year_10;
-- EXPLAIN ANALYZE
-- CREATE TABLE wind_ds.sample_best_option_each_year_10 AS
-- 
-- with x as (
-- 	SELECT -- point descriptors
-- 		a.gid, b.year, a.county_id, a.state_abbr, a.census_division_abbr, a.census_region, a.row_number, 
-- 		-- exclusions
-- 		a.maxheight_m_popdens as max_height, 
-- 	 
-- 		-- rates
-- 		a.elec_rate_cents_per_kwh * b.rate_escalation_factor as elec_rate_cents_per_kwh, 
-- 
-- 		-- costs
-- 		a.cap_cost_multiplier,
-- 		b.fixed_om_dollars_per_kw_per_yr, 
-- 		b.variable_om_dollars_per_kwh,
-- 		b.installed_costs_dollars_per_kw * a.cap_cost_multiplier::numeric as installed_costs_dollars_per_kw,
-- 		
-- 		-- load and customers information
-- 		a.ann_cons_kwh, a.prob, a.weight,
-- 		b.load_multiplier * a.customers_in_bin as customers_in_bin, 
-- 		a.customers_in_bin as initial_customers_in_bin, 
-- 		b.load_multiplier * a.load_kwh_in_bin AS load_kwh_in_bin,
-- 		a.load_kwh_in_bin AS initial_load_kwh_in_bin,
-- 
-- 		-- load per customer stays static throughout time
-- 		case when a.customers_in_bin > 0 THEN a.load_kwh_in_bin/a.customers_in_bin 
-- 		else 0
-- 		end as load_kwh_per_customer_in_bin,
-- 
-- 		-- wind resource data
-- 		a.i, a.j, a.cf_bin, a.aep_scale_factor, 
-- 		c.aep*a.aep_scale_factor*a.derate_factor as naep,
-- 		b.nameplate_capacity_kw,
-- 		b.power_curve_id, 
-- 		b.turbine_height_m
-- 		
-- 		FROM wind_ds.pt_grid_us_res_sample_load_10 a
-- 		-- join in temporally varying factors
-- 		LEFT JOIN wind_ds.temporal_factors b
-- 		ON b.turbine_height_m <= a.maxheight_m_popdens 
-- 		and a.census_division_abbr = b.census_division_abbr
-- 		-- join in wind resource data
-- 		LEFT JOIN wind_ds.wind_resource_annual c
-- 		ON a.i = c.i
-- 		and a.j = c.j
-- 		and a.cf_bin = c.cf_bin
-- 		and b.turbine_height_m = c.height
-- 		and b.power_curve_id = c.turbine_id
-- 		-- these will all be variables in the model script
-- 		where b.sector = 'Residential'
-- 		and a.maxheight_m_popdens > 0	
-- 		and b.rate_escalation_source = 'AEO2014'
-- 		and b.load_growth_scenario = 'AEO 2013 Reference Case'
-- 	),
-- y as (
-- 				-- over and undersize factors should be variables in script
-- 	SELECT x.*, wind_ds.scoe(x.installed_costs_dollars_per_kw, x.fixed_om_dollars_per_kw_per_yr, x.variable_om_dollars_per_kwh, x.naep , x.nameplate_capacity_kw , x.load_kwh_per_customer_in_bin , 1.15, 0.5) as scoe
-- 	from x
-- 	)
-- 
-- 
-- SELECT distinct on (y.gid) y.*
-- FROM y
-- order by y.gid, y.scoe asc;
-- -- 1014038 ms (16 ms)

DROP TABLE IF EXISTS wind_ds.sample_best_option_each_year_10;
CREATE TABLE wind_ds.sample_best_option_each_year_10 AS
with y as (
	SELECT x.*, wind_ds.scoe(x.installed_costs_dollars_per_kw, x.fixed_om_dollars_per_kw_per_yr, x.variable_om_dollars_per_kwh, x.naep , x.nameplate_capacity_kw , x.load_kwh_per_customer_in_bin , 1.15, 0.5) as scoe
	from wind_ds.sample_all_years_10 x
	)


SELECT distinct on (y.gid) y.*
FROM y
order by y.gid, y.scoe asc;
-- 4500091 ms (7.5 mins)

-- when finished, need to create an index on the year field

-- to run all, takes 1002452 ms (16 ms)
