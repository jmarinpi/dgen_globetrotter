﻿-- create view of the valid counties
CREATE OR REPLACE VIEW wind_ds.counties_to_model AS
SELECT county_id, census_region
FROM wind_ds.county_geom a
INNER JOIN wind_ds.scenario_options b
ON lower(a.state) = CASE WHEN b.region = 'United States' then lower(a.state)
		else lower(b.region)
		end
where a.state not in ('Hawaii','Alaska');

-- view for carbon intensities
DROP VIEW IF EXISTS wind_ds.carbon_intensities_to_model;
CREATE OR REPLACE VIEW wind_ds.carbon_intensities_to_model AS
SELECT state_abbr,
	CASE WHEN b.carbon_price = 'No Carbon Price' THEN no_carbon_price_t_per_kwh
	WHEN b.carbon_price = 'Price Based On State Carbon Intensity' THEN state_carbon_price_t_per_kwh
	WHEN b.carbon_price = 'Price Based On NG Offset' THEN ng_offset_t_per_kwh
	END as carbon_intensity_t_per_kwh
FROM wind_ds.carbon_intensities a
CROSS JOIN wind_ds.scenario_options b;

-- view for net metering
DROP VIEW IF EXISTS wind_ds.net_metering_to_model;
CREATE OR REPLACE VIEW wind_ds.net_metering_to_model AS
WITH combined as (
SELECT a.sector, a.utility_type, a.nem_system_limit_kw, a.state_abbr,
	CASE WHEN b.overwrite_exist_nm = TRUE THEN False
	ELSE TRUE
	END as keep, 'ftg' as source
	
FROM wind_ds.net_metering_availability_2013 a
CROSS JOIN wind_ds.scenario_options b

UNION ALL

SELECT a.sector, a.utility_type, a.nem_system_limit_kw, a.state_abbr,
	CASE WHEN b.overwrite_exist_nm = TRUE THEN TRUE
	ELSE FALSE
	END as keep, 'man' as source


FROM wind_ds.manual_net_metering_availability a
CROSS JOIN wind_ds.scenario_options b)

SELECT sector, utility_type, nem_system_limit_kw, state_abbr
FROM combined
where keep = True;



-- views of point data
-- ind
DROP VIEW IF EXISTS wind_ds.pt_grid_us_ind_joined;
CREATE OR REPLACE VIEW wind_ds.pt_grid_us_ind_joined AS
SELECT a.gid, a.county_id, a.utility_type, a.maxheight_m_popdens,a.maxheight_m_popdenscancov20pc, a.maxheight_m_popdenscancov40pc, 
	a.annual_rate_gid, a.iiijjjicf_id, 'p' || a.pca_reg::text as pca_reg, a.reeds_reg,
        c.ind_cents_per_kwh * (1-n.ind_demand_charge_rate) as elec_rate_cents_per_kwh, 
	b.total_customers_2011_industrial as county_total_customers_2011, 
	b.total_load_mwh_2011_industrial as county_total_load_mwh_2011,
	d.cap_cost_multiplier,
	e.state_abbr, e.census_division_abbr, e.census_region,
	g.i, g.j, g.cf_bin, g.aep_scale_factor, l.carbon_intensity_t_per_kwh,
	m.nem_system_limit_kw
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
-- join in i,j,icf lookup
LEFT JOIN wind_ds.ij_cfbin_lookup_ind_pts_us g
on a.gid = g.pt_gid
-- subset to counties of interest
INNER JOIN wind_ds.counties_to_model h 
ON a.county_id = h.county_id
-- carbon intensities
LEFT JOIN wind_ds.carbon_intensities_to_model l
ON e.state_abbr = l.state_abbr
-- net metering policies
LEFT JOIN wind_ds.net_metering_to_model m
ON e.state_abbr = m.state_abbr
AND m.sector = 'ind'
AND a.utility_type = m.utility_type
-- manual demand charges
CROSS JOIN wind_ds.scenario_options n;


-- res
DROP VIEW IF EXISTS wind_ds.pt_grid_us_res_joined;
CREATE OR REPLACE VIEW wind_ds.pt_grid_us_res_joined AS
SELECT a.gid, a.county_id, a.utility_type, a.maxheight_m_popdens,a.maxheight_m_popdenscancov20pc, a.maxheight_m_popdenscancov40pc, 
	a.annual_rate_gid, a.iiijjjicf_id, 'p' || a.pca_reg::text as pca_reg, a.reeds_reg,
	c.res_cents_per_kwh as elec_rate_cents_per_kwh, 
	b.total_customers_2011_residential * k.perc_own_occu_1str_housing as county_total_customers_2011, 
	b.total_load_mwh_2011_residential * k.perc_own_occu_1str_housing as county_total_load_mwh_2011,
	d.cap_cost_multiplier,
	e.state_abbr, e.census_division_abbr, e.census_region,
	g.i, g.j, g.cf_bin, g.aep_scale_factor, l.carbon_intensity_t_per_kwh,
	m.nem_system_limit_kw
FROM wind_ds.pt_grid_us_res a
-- county_load_and_customers
LEFT JOIN wind_ds.load_and_customers_by_county_us b
ON a.county_id = b.county_id
-- county % owner occ housing
LEFT JOIN wind_ds.county_housing_units k
ON a.county_id = k.county_id
-- rates
LEFT JOIN wind_ds.annual_ave_elec_rates_2011 c
ON a.annual_rate_gid = c.gid
-- capital_costs
LEFT JOIN wind_ds.capital_cost_multipliers_us d
ON a.county_id = d.county_id
-- census region and division
LEFT JOIN wind_ds.county_geom e
ON a.county_id = e.county_id
-- join in i,j,icf lookup
LEFT JOIN wind_ds.ij_cfbin_lookup_res_pts_us g
on a.gid = g.pt_gid
-- subset to counties of interest
INNER JOIN wind_ds.counties_to_model h 
ON a.county_id = h.county_id
-- carbon intensities
LEFT JOIN wind_ds.carbon_intensities_to_model l
ON e.state_abbr = l.state_abbr
-- net metering policies
LEFT JOIN wind_ds.net_metering_to_model m
ON e.state_abbr = m.state_abbr
AND m.sector = 'res'
AND a.utility_type = m.utility_type;


-- comm
DROP VIEW IF EXISTS wind_ds.pt_grid_us_com_joined;
CREATE OR REPLACE VIEW wind_ds.pt_grid_us_com_joined AS
SELECT a.gid, a.county_id, a.utility_type, a.maxheight_m_popdens,a.maxheight_m_popdenscancov20pc, a.maxheight_m_popdenscancov40pc, 
	a.annual_rate_gid, a.iiijjjicf_id, 'p' || a.pca_reg::text as pca_reg, a.reeds_reg,
	c.comm_cents_per_kwh * (1-n.com_demand_charge_rate) as elec_rate_cents_per_kwh, 
	b.total_customers_2011_commercial as county_total_customers_2011, 
	b.total_load_mwh_2011_commercial as county_total_load_mwh_2011,
	d.cap_cost_multiplier,
	e.state_abbr, e.census_division_abbr, e.census_region, 
	g.i, g.j, g.cf_bin, g.aep_scale_factor, l.carbon_intensity_t_per_kwh,
	m.nem_system_limit_kw
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
-- join in i,j,icf lookup
LEFT JOIN wind_ds.ij_cfbin_lookup_com_pts_us g
on a.gid = g.pt_gid
-- subset to counties of interest
INNER JOIN wind_ds.counties_to_model h 
ON a.county_id = h.county_id
-- carbon intensities
LEFT JOIN wind_ds.carbon_intensities_to_model l
ON e.state_abbr = l.state_abbr
-- net metering policies
LEFT JOIN wind_ds.net_metering_to_model m
ON e.state_abbr = m.state_abbr
AND m.sector = 'com'
AND a.utility_type = m.utility_type
-- manual demand charges
CROSS JOIN wind_ds.scenario_options n;


-- create view of sectors to model
CREATE OR REPLACE VIEW wind_ds.sectors_to_model AS
SELECT CASE WHEN markets = 'All' THEN 'res=>Residential,com=>Commercial,ind=>Industrial'::hstore
	    when markets = 'Only Residential' then 'res=>Residential'::hstore
	    when markets = 'Only Commercial' then 'com=>Commercial'::hstore
	    when markets = 'Only Industrial' then 'ind=>Industrial'::hstore
	   end as sectors
FROM wind_ds.scenario_options;

-- create view of exclusions to model
CREATE OR REPLACE VIEW wind_ds.exclusions_to_model AS
SELECT CASE WHEN height_exclusions = 'Population Density Only' THEN 'maxheight_m_popdens'
	    when height_exclusions = 'Population Density & Canopy Cover (40%)' THEN 'maxheight_m_popdenscancov40pc'
	    when height_exclusions = 'Population Density & Canopy Cover (20%)' THEN 'maxheight_m_popdenscancov20pc'
	    when height_exclusions = 'No Exclusions' THEN NULL
       end as exclusions
FROM wind_ds.scenario_options;


-- max market share
CREATE OR REPLACE VIEW wind_ds.max_market_curves_to_model As
with user_inputs as (
	SELECT 'residential' as sector, res_max_market_curve as source
	FROM wind_ds.scenario_options
	UNION
	SELECT 'commercial' as sector, com_max_market_curve as source
	FROM wind_ds.scenario_options
	UNION
	SELECT 'industrial' as sector, ind_max_market_curve as source
	FROM wind_ds.scenario_options
),
all_maxmarket as (
	SELECT years_to_payback as year, sector, max_market_share_new as new, max_market_share_retrofit as retrofit, source
	FROM wind_ds.max_market_share


	UNION

	SELECT year, sector, new, retrofit, 'User Defined' as source
	FROM wind_ds.user_defined_max_market_share)
SELECT a.*
FROM all_maxmarket a
INNER JOIN user_inputs b
ON a.sector = b.sector
and a.source = b.source
order by year, sector;

-- create view for rate escalations
CREATE OR REPLACE VIEW wind_ds.rate_escalations_to_model AS
WITH cdas as (
	SELECT distinct(census_division_abbr) as census_division_abbr
	FROM wind_ds.county_geom
	),

esc_combined AS (
	-- convert user defined rate projections to format consistent with wind_ds.rate_escalations
	SELECT b.census_division_abbr, a.year, 'Residential'::text as sector, 
		a.user_defined_res_rate_escalations as escalation_factor,
		'User Defined'::text as source
	FROM wind_ds.market_projections a
	CROSS JOIN cdas b

	UNION

	SELECT b.census_division_abbr, a.year, 'Commercial'::text as sector, 
		a.user_defined_com_rate_escalations as escalation_factor,
		'User Defined'::text as source
	FROM wind_ds.market_projections a
	CROSS JOIN cdas b

	UNION

	SELECT b.census_division_abbr, a.year, 'Industrial'::text as sector, 
		a.user_defined_ind_rate_escalations as escalation_factor,
		'User Defined'::text as source
	FROM wind_ds.market_projections a
	CROSS JOIN cdas b

	UNION
	-- create No Growth projections using the same method
	SELECT b.census_division_abbr, a.year, 'Residential'::text as sector, 
		1::numeric as escalation_factor,
		'No Growth'::text as source
	FROM wind_ds.market_projections a
	CROSS JOIN cdas b

	UNION

	SELECT b.census_division_abbr, a.year, 'Commercial'::text as sector, 
		1::numeric as escalation_factor,
		'No Growth'::text as source
	FROM wind_ds.market_projections a
	CROSS JOIN cdas b

	UNION

	SELECT b.census_division_abbr, a.year, 'Industrial'::text as sector, 
		1::numeric as escalation_factor,
		'No Growth'::text as source
	FROM wind_ds.market_projections a
	CROSS JOIN cdas b

	UNION

	-- add in the prestaged AEO2014 projections
	SELECT census_division_abbr, year, sector, escalation_factor, source
	FROM wind_ds.rate_escalations

	order by year, sector, census_division_abbr, source),

inp_opts AS (
	SELECT 'Residential'::text as sector, res_rate_escalation as source
	FROM wind_ds.scenario_options
	UNION
	SELECT 'Commercial'::text as sector, com_rate_escalation as source
	FROM wind_ds.scenario_options
	UNION
	SELECT 'Industrial'::text as sector, ind_rate_escalation as source
	FROM wind_ds.scenario_options)

SELECT a.census_division_abbr, a.year, a.sector, a.escalation_factor, a.source
FROM esc_combined a
INNER JOIN inp_opts b
ON a.sector = b.sector
and a.source = b.source;


-- costs for all turbine sizes and years
DROP VIEW IF EXISTS wind_ds.turbine_costs_per_size_and_year;
CREATE OR REPLACE VIEW wind_ds.turbine_costs_per_size_and_year AS
 SELECT a.turbine_size_kw, a.turbine_height_m, b.year, 
    -- normalized costs (i.e., costs per kw)
    b.capital_cost_dollars_per_kw,
    b.fixed_om_dollars_per_kw_per_yr,
    b.variable_om_dollars_per_kwh,
    b.cost_for_higher_towers_dollars_per_kw_per_m,
    b.cost_for_higher_towers_dollars_per_kw_per_m * (a.turbine_height_m - b.default_tower_height_m) as tower_cost_adder_dollars_per_kw,
    b.capital_cost_dollars_per_kw + (b.cost_for_higher_towers_dollars_per_kw_per_m * (a.turbine_height_m - b.default_tower_height_m)) AS installed_costs_dollars_per_kw
FROM wind_ds.allowable_turbine_sizes a
LEFT JOIN wind_ds.wind_cost_projections b  --this join will repeat the cost projections for each turbine height associated with each size
ON a.turbine_size_kw = b.turbine_size_kw;


