-- randomly sample  100 points from each county (note: some counties will have fewer)
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
SET LOCAL SEED TO 1;
WITH weighted_county_sample as (
	SELECT a.county_id, row_number() OVER (PARTITION BY a.county_id ORDER BY random() * b.prob) as row_number, b.*
	FROM wind_ds.county_geom a
	LEFT JOIN wind_ds.binned_annual_load_kwh_100_bins b
	ON a.census_region = b.census_region
	AND b.sector = 'industrial')
SELECT a.gid, b.ann_cons_kwh, b.prob, b.weight,
	a.county_total_customers_2011 * b.weight/sum(weight) OVER (PARTITION BY a.county_id) as customers_in_bin, 
	a.county_total_load_mwh_2011 * (b.ann_cons_kwh*b.weight)/sum(b.ann_cons_kwh*b.weight) OVER (PARTITION BY a.county_id) as load_mwh_in_bin
FROM wind_ds.pt_grid_us_res_sample a
LEFT JOIN weighted_county_sample b
ON a.county_id = b.county_id
and a.row_number = b.row_number;


-- join in AEP values and calculate all valid resource values based on allowable turbine heights, power curves, and rated capacities
explain ANALYZE
SELECT a.gid, a.maxheight_m_popdens, e.turbine_height_m, e.turbine_size_kw, f.power_curve_id,
	b.customers_in_bin, b.load_mwh_in_bin, 
	c.i, c.j, c.cf_bin, c.aep_scale_factor, 
	d.aep as normalized_aep_raw, 
	d.aep*c.aep_scale_factor as normalized_aep_adjusted, 
	d.aep*turbine_size_kw as aep_raw, 
	d.aep*c.aep_scale_factor*turbine_size_kw as aep_adjusted, 
	d.height, d.turbine_id, e.turbine_size_kw
FROM wind_ds.pt_grid_us_res_sample a

LEFT JOIN wind_ds.pt_grid_us_res_sample_load b
ON a.gid = b.gid

LEFT JOIN wind_ds.allowable_turbine_sizes e
ON e.turbine_height_m <= a.maxheight_m_popdens

LEFT JOIN wind_ds.wind_performance_improvements f
ON e.turbine_size_kw = f.nameplate_capacity_kw 

LEFT JOIN wind_ds.ij_cfbin_lookup_res_pts_us c
on a.gid = c.pt_gid

LEFT JOIN wind_ds.wind_resource_annual d
ON c.i = d.i
and c.j = d.j
and c.cf_bin = d.cf_bin
and e.turbine_height_m = d.height
and f.power_curve_id = d.turbine_id

where f.year = 2014

