
-- views of point data

-- ind
DROP VIEW IF EXISTS wind_ds.pt_grid_us_ind_joined;
CREATE OR REPLACE VIEW wind_ds.pt_grid_us_ind_joined AS
SELECT a.*, c.ind_cents_per_kwh * ind_derate_factor as elec_rate_cents_per_kwh, 
	b.total_customers_2011_industrial as county_total_customers_2011, 
	b.total_load_mwh_2011_industrial as county_total_load_mwh_2011,
	d.cap_cost_multiplier,
	e.state_abbr, e.census_division_abbr, e.census_region, f.derate_factor,
	g.i, g.j, g.cf_bin, g.aep_scale_factor
FROM wind_ds.pt_grid_us_ind a
-- county_load_and_customers
LEFT JOIN wind_ds.load_and_customers_by_county_us b
ON a.county_id = b.county_id
-- rates
LEFT JOIN wind_ds.annual_ave_elec_rates_2011 c
ON a.annual_rate_gid = c.gid
-- capital_costs
LEFT JOIN wind_ds.capital_cost_multipliers_us d
ON a.county_id = d.county_id
-- census region and division
LEFT JOIN wind_ds.county_geom e
ON a.county_id = e.county_id
-- join in derate factors
LEFT JOIN wind_ds.wind_derate_factors_by_state f
ON e.state_abbr = f.state_abbr
-- join in i,j,icf lookup
LEFT JOIN wind_ds.ij_cfbin_lookup_ind_pts_us g
on a.gid = g.pt_gid;;


-- res
DROP VIEW IF EXISTS wind_ds.pt_grid_us_res_joined;
CREATE OR REPLACE VIEW wind_ds.pt_grid_us_res_joined AS
SELECT a.*, c.res_cents_per_kwh as elec_rate_cents_per_kwh, 
	b.total_customers_2011_residential as county_total_customers_2011, 
	b.total_load_mwh_2011_residential as county_total_load_mwh_2011,
	d.cap_cost_multiplier,
	e.state_abbr, e.census_division_abbr, e.census_region, f.derate_factor,
	g.i, g.j, g.cf_bin, g.aep_scale_factor
FROM wind_ds.pt_grid_us_res a
-- county_load_and_customers
LEFT JOIN wind_ds.load_and_customers_by_county_us b
ON a.county_id = b.county_id
-- rates
LEFT JOIN wind_ds.annual_ave_elec_rates_2011 c
ON a.annual_rate_gid = c.gid
-- capital_costs
LEFT JOIN wind_ds.capital_cost_multipliers_us d
ON a.county_id = d.county_id
-- census region and division
LEFT JOIN wind_ds.county_geom e
ON a.county_id = e.county_id
-- join in derate factors
LEFT JOIN wind_ds.wind_derate_factors_by_state f
ON e.state_abbr = f.state_abbr
-- join in i,j,icf lookup
LEFT JOIN wind_ds.ij_cfbin_lookup_res_pts_us g
on a.gid = g.pt_gid;

-- comm
DROP VIEW IF EXISTS wind_ds.pt_grid_us_com_joined;
CREATE OR REPLACE VIEW wind_ds.pt_grid_us_com_joined AS
SELECT a.*, c.comm_cents_per_kwh * comm_derate_factor as elec_rate_cents_per_kwh, 
	b.total_customers_2011_commercial as county_total_customers_2011, 
	b.total_load_mwh_2011_commercial as county_total_load_mwh_2011,
	d.cap_cost_multiplier,
	e.state_abbr, e.census_division_abbr, e.census_region, f.derate_factor,
	g.i, g.j, g.cf_bin, g.aep_scale_factor
FROM wind_ds.pt_grid_us_com a
-- county_load_and_customers
LEFT JOIN wind_ds.load_and_customers_by_county_us b
ON a.county_id = b.county_id
-- rates
LEFT JOIN wind_ds.annual_ave_elec_rates_2011 c
ON a.annual_rate_gid = c.gid
-- capital_costs
LEFT JOIN wind_ds.capital_cost_multipliers_us d
ON a.county_id = d.county_id
-- census region and division
LEFT JOIN wind_ds.county_geom e
ON a.county_id = e.county_id
-- join in derate factors
LEFT JOIN wind_ds.wind_derate_factors_by_state f
ON e.state_abbr = f.state_abbr
-- join in i,j,icf lookup
LEFT JOIN wind_ds.ij_cfbin_lookup_com_pts_us g
on a.gid = g.pt_gid;


-- create view of the valid counties
CREATE OR REPLACE VIEW wind_ds.counties_to_model AS
SELECT county_id, census_region
FROM wind_ds.county_geom a
INNER JOIN wind_ds.scenario_options b
ON lower(a.state) = CASE WHEN b.region = 'United States' then lower(a.state)
		else lower(b.region)
		end
where a.state not in ('Hawaii','Alaska');

-- create view of sectors to model
CREATE OR REPLACE VIEW wind_ds.sectors_to_model AS
SELECT CASE WHEN markets = 'All' THEN 'res=>Residential,com=>Commercial,ind=>Industrial'::hstore
	    when markets = 'Only Residential' then 'res=>Residential'::hstore
	    when markets = 'Only Commercial' then 'com=>Commercial'::hstore
	    when markets = 'Only Industrial' then 'ind=>Industrial'::hstore
	   end as sectors
FROM wind_ds.scenario_options;

-- create view of sectors to model
CREATE OR REPLACE VIEW wind_ds.exclusions_to_model AS
SELECT CASE WHEN height_exclusions = 'Population Density Only' THEN 'maxheight_m_popdens'
	    when height_exclusions = 'Population Density & Canopy Cover (40%)' THEN 'maxheight_m_popdenscancov40pc'
	    when height_exclusions = 'Population Density & Canopy Cover (20%)' THEN 'maxheight_m_popdenscancov20pc'
	    when height_exclusions = 'No Exclusions' THEN NULL
       end as exclusions
FROM wind_ds.scenario_options;


