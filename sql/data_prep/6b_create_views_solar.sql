SET ROLE 'diffusion-writers';

-- create view of the valid counties
CREATE OR REPLACE VIEW diffusion_solar.counties_to_model AS
SELECT county_id, census_region
FROM diffusion_shared.county_geom a
INNER JOIN diffusion_solar.scenario_options b
ON lower(a.state) = CASE WHEN b.region = 'United States' then lower(a.state)
		else lower(b.region)
		end
where a.state not in ('Hawaii','Alaska');

-- view for carbon intensities
DROP VIEW IF EXISTS diffusion_solar.carbon_intensities_to_model;
CREATE OR REPLACE VIEW diffusion_solar.carbon_intensities_to_model AS
SELECT state_abbr,
	CASE WHEN b.carbon_price = 'No Carbon Price' THEN no_carbon_price_t_per_kwh
	WHEN b.carbon_price = 'Price Based On State Carbon Intensity' THEN state_carbon_price_t_per_kwh
	WHEN b.carbon_price = 'Price Based On NG Offset' THEN ng_offset_t_per_kwh
	END as carbon_intensity_t_per_kwh
FROM diffusion_shared.carbon_intensities a
CROSS JOIN diffusion_solar.scenario_options b;

-- view for net metering
DROP VIEW IF EXISTS diffusion_solar.net_metering_to_model;
CREATE OR REPLACE VIEW diffusion_solar.net_metering_to_model AS
WITH combined as (
SELECT a.sector, a.utility_type, a.nem_system_limit_kw, a.state_abbr,
	CASE WHEN b.overwrite_exist_nm = TRUE THEN False
	ELSE TRUE
	END as keep, 'ftg' as source
	
FROM diffusion_shared.net_metering_availability_2013 a
CROSS JOIN diffusion_solar.scenario_options b

UNION ALL

SELECT a.sector, a.utility_type, a.nem_system_limit_kw, a.state_abbr,
	CASE WHEN b.overwrite_exist_nm = TRUE THEN TRUE
	ELSE FALSE
	END as keep, 'man' as source


FROM diffusion_solar.manual_net_metering_availability a
CROSS JOIN diffusion_solar.scenario_options b)

SELECT sector, utility_type, nem_system_limit_kw, state_abbr
FROM combined
where keep = True;



-- views of point data
-- ind
DROP VIEW IF EXISTS diffusion_solar.point_microdata_ind_us_joined;
CREATE OR REPLACE VIEW diffusion_solar.point_microdata_ind_us_joined AS
SELECT a.micro_id, a.county_id, a.utility_type, 
	a.annual_rate_gid, a.pca_reg, a.reeds_reg, a.solar_incentive_array_id,
        c.ind_cents_per_kwh * (1-n.ind_demand_charge_rate) as elec_rate_cents_per_kwh, 
	b.total_customers_2011_industrial as county_total_customers_2011, 
	b.total_load_mwh_2011_industrial as county_total_load_mwh_2011,
	d.pv_20mw_cap_cost_multplier as cap_cost_multiplier,
	e.state_abbr, e.census_division_abbr, e.census_region,
	a.solar_re_9809_gid, 
	l.carbon_intensity_t_per_kwh,
	m.nem_system_limit_kw
FROM diffusion_solar.point_microdata_ind_us a
-- county_load_and_customers
LEFT JOIN diffusion_shared.load_and_customers_by_county_us b
ON a.county_id = b.county_id
-- rates
LEFT JOIN diffusion_shared.annual_ave_elec_rates_2011 c
ON a.annual_rate_gid = c.gid
-- capital_costs
LEFT JOIN diffusion_shared.capital_cost_multipliers_us d
ON a.county_id = d.county_id
-- census region and division
LEFT JOIN diffusion_shared.county_geom e
ON a.county_id = e.county_id
-- subset to counties of interest
INNER JOIN diffusion_solar.counties_to_model h 
ON a.county_id = h.county_id
-- carbon intensities
LEFT JOIN diffusion_solar.carbon_intensities_to_model l
ON e.state_abbr = l.state_abbr
-- net metering policies
LEFT JOIN diffusion_solar.net_metering_to_model m
ON e.state_abbr = m.state_abbr
AND m.sector = 'ind'
AND a.utility_type = m.utility_type
-- manual demand charges
CROSS JOIN diffusion_solar.scenario_options n;


-- res
DROP VIEW IF EXISTS diffusion_solar.point_microdata_res_us_joined;
CREATE OR REPLACE VIEW diffusion_solar.point_microdata_res_us_joined AS
SELECT a.micro_id, a.county_id, a.utility_type, 
	a.annual_rate_gid, a.pca_reg, a.reeds_reg, a.solar_incentive_array_id,
	c.res_cents_per_kwh as elec_rate_cents_per_kwh, 
	b.total_customers_2011_residential * k.perc_own_occu_1str_housing as county_total_customers_2011, 
	b.total_load_mwh_2011_residential * k.perc_own_occu_1str_housing as county_total_load_mwh_2011,
	d.pv_20mw_cap_cost_multplier as cap_cost_multiplier,
	e.state_abbr, e.census_division_abbr, e.census_region,
	a.solar_re_9809_gid, 
	l.carbon_intensity_t_per_kwh,
	m.nem_system_limit_kw
FROM diffusion_solar.point_microdata_res_us a
-- county_load_and_customers
LEFT JOIN diffusion_shared.load_and_customers_by_county_us b
ON a.county_id = b.county_id
-- county % owner occ housing
LEFT JOIN diffusion_shared.county_housing_units k
ON a.county_id = k.county_id
-- rates
LEFT JOIN diffusion_shared.annual_ave_elec_rates_2011 c
ON a.annual_rate_gid = c.gid
-- capital_costs
LEFT JOIN diffusion_shared.capital_cost_multipliers_us d
ON a.county_id = d.county_id
-- census region and division
LEFT JOIN diffusion_shared.county_geom e
ON a.county_id = e.county_id
-- subset to counties of interest
INNER JOIN diffusion_solar.counties_to_model h 
ON a.county_id = h.county_id
-- carbon intensities
LEFT JOIN diffusion_solar.carbon_intensities_to_model l
ON e.state_abbr = l.state_abbr
-- net metering policies
LEFT JOIN diffusion_solar.net_metering_to_model m
ON e.state_abbr = m.state_abbr
AND m.sector = 'res'
AND a.utility_type = m.utility_type;


-- comm
DROP VIEW IF EXISTS diffusion_solar.point_microdata_com_us_joined;
CREATE OR REPLACE VIEW diffusion_solar.point_microdata_com_us_joined AS
SELECT a.micro_id, a.county_id, a.utility_type, 
	a.annual_rate_gid, a.pca_reg, a.reeds_reg, a.solar_incentive_array_id,
	c.comm_cents_per_kwh * (1-n.com_demand_charge_rate) as elec_rate_cents_per_kwh, 
	b.total_customers_2011_commercial as county_total_customers_2011, 
	b.total_load_mwh_2011_commercial as county_total_load_mwh_2011,
	d.pv_20mw_cap_cost_multplier as cap_cost_multiplier,
	e.state_abbr, e.census_division_abbr, e.census_region, 
	a.solar_re_9809_gid,
	l.carbon_intensity_t_per_kwh,
	m.nem_system_limit_kw
FROM diffusion_solar.point_microdata_com_us a
-- county_load_and_customers
LEFT JOIN diffusion_shared.load_and_customers_by_county_us b
ON a.county_id = b.county_id
-- rates
LEFT JOIN diffusion_shared.annual_ave_elec_rates_2011 c
ON a.annual_rate_gid = c.gid
-- capital_costs
LEFT JOIN diffusion_shared.capital_cost_multipliers_us d
ON a.county_id = d.county_id
-- census region and division
LEFT JOIN diffusion_shared.county_geom e
ON a.county_id = e.county_id
-- subset to counties of interest
INNER JOIN diffusion_solar.counties_to_model h 
ON a.county_id = h.county_id
-- carbon intensities
LEFT JOIN diffusion_solar.carbon_intensities_to_model l
ON e.state_abbr = l.state_abbr
-- net metering policies
LEFT JOIN diffusion_solar.net_metering_to_model m
ON e.state_abbr = m.state_abbr
AND m.sector = 'com'
AND a.utility_type = m.utility_type
-- manual demand charges
CROSS JOIN diffusion_solar.scenario_options n;


-- create view of sectors to model
CREATE OR REPLACE VIEW diffusion_solar.sectors_to_model AS
SELECT CASE WHEN markets = 'All' THEN 'res=>Residential,com=>Commercial,ind=>Industrial'::hstore
	    when markets = 'Only Residential' then 'res=>Residential'::hstore
	    when markets = 'Only Commercial' then 'com=>Commercial'::hstore
	    when markets = 'Only Industrial' then 'ind=>Industrial'::hstore
	   end as sectors
FROM diffusion_solar.scenario_options;


-- max market share
CREATE OR REPLACE VIEW diffusion_solar.max_market_curves_to_model As
with user_inputs as (
	SELECT 'residential' as sector, res_max_market_curve as source
	FROM diffusion_solar.scenario_options
	UNION
	SELECT 'commercial' as sector, com_max_market_curve as source
	FROM diffusion_solar.scenario_options
	UNION
	SELECT 'industrial' as sector, ind_max_market_curve as source
	FROM diffusion_solar.scenario_options
),
all_maxmarket as (
	SELECT years_to_payback as year, sector, max_market_share_new as new, max_market_share_retrofit as retrofit, source
	FROM diffusion_shared.max_market_share


	UNION

	SELECT year, sector, new, retrofit, 'User Defined' as source
	FROM diffusion_solar.user_defined_max_market_share)
SELECT a.*
FROM all_maxmarket a
INNER JOIN user_inputs b
ON a.sector = b.sector
and a.source = b.source
order by year, sector;

-- create view for rate escalations
CREATE OR REPLACE VIEW diffusion_solar.rate_escalations_to_model AS
With cdas AS (
	SELECT distinct(census_division_abbr) as census_division_abbr, generate_series(2014,2080) as year
	FROM diffusion_shared.county_geom
	order by year, census_division_abbr
),
user_defined_gaps_res AS 
(
	SELECT b.census_division_abbr, b.year, 'Residential'::text as sector,
		a.user_defined_res_rate_escalations as escalation_factor,
		lag(a.user_defined_res_rate_escalations,1) OVER (PARTITION BY b.census_division_abbr ORDER BY b.year asc) as lag_factor,
		lead(a.user_defined_res_rate_escalations,1) OVER (PARTITION BY b.census_division_abbr ORDER BY b.year asc) as lead_factor,
		(array_agg(a.user_defined_res_rate_escalations) OVER (PARTITION BY b.census_division_abbr ORDER BY b.year ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[37] as last_factor,
		'User Defined'::text as source
	FROM cdas b
	LEFT JOIN diffusion_solar.market_projections a
       on a.year = b.year
),
user_defined_gaps_com AS 
(
	SELECT b.census_division_abbr, b.year, 'Commercial'::text as sector,
		a.user_defined_com_rate_escalations as escalation_factor,
		lag(a.user_defined_com_rate_escalations,1) OVER (PARTITION BY b.census_division_abbr ORDER BY b.year asc) as lag_factor,
		lead(a.user_defined_com_rate_escalations,1) OVER (PARTITION BY b.census_division_abbr ORDER BY b.year asc) as lead_factor,
		(array_agg(a.user_defined_com_rate_escalations) OVER (PARTITION BY b.census_division_abbr ORDER BY b.year ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[37] as last_factor,
		'User Defined'::text as source
	FROM cdas b
	LEFT JOIN diffusion_solar.market_projections a
       on a.year = b.year
),
user_defined_gaps_ind AS 
(
	SELECT b.census_division_abbr, b.year, 'Industrial'::text as sector,
		a.user_defined_ind_rate_escalations as escalation_factor,
		lag(a.user_defined_ind_rate_escalations,1) OVER (PARTITION BY b.census_division_abbr ORDER BY b.year asc) as lag_factor,
		lead(a.user_defined_ind_rate_escalations,1) OVER (PARTITION BY b.census_division_abbr ORDER BY b.year asc) as lead_factor,
		(array_agg(a.user_defined_ind_rate_escalations) OVER (PARTITION BY b.census_division_abbr ORDER BY b.year ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[37] as last_factor,
		'User Defined'::text as source
	FROM cdas b
	LEFT JOIN diffusion_solar.market_projections a
       on a.year = b.year
),
user_defined_gaps_all AS
(
	SELECT *
	FROM user_defined_gaps_res
	UNION
	SELECT *
	FROM user_defined_gaps_com
	UNION
	SELECT *
	FROM user_defined_gaps_ind
),
user_defined_filled AS
(
	SELECT census_division_abbr, year, sector,
	CASE WHEN escalation_factor is null and year <= 2050 THEN (lag_factor+lead_factor)/2
	     WHEN escalation_factor is null and year > 2050 THEN last_factor
	     ELSE escalation_factor
	END as escation_factor,
	source
	FROM user_defined_gaps_all
),
no_growth AS (
SELECT census_division_abbr, year, unnest(array['Residential','Commercial','Industrial'])::text as sector,
		1::numeric as escalation_factor,
		'No Growth'::text as source
	FROM cdas
),
aeo AS 
(
	SELECT census_division_abbr, year, sector, 
		escalation_factor, 
		source
	FROM diffusion_shared.rate_escalations
	where year >= 2014
),
esc_combined AS
(
	SELECT *
	FROM aeo

	UNION 

	SELECT *
	FROM no_growth

	UNION 

	SELECT *
	FROM user_defined_filled
),
inp_opts AS 
(
	SELECT 'Residential'::text as sector, res_rate_escalation as source
	FROM diffusion_solar.scenario_options
	UNION
	SELECT 'Commercial'::text as sector, com_rate_escalation as source
	FROM diffusion_solar.scenario_options
	UNION
	SELECT 'Industrial'::text as sector, ind_rate_escalation as source
	FROM diffusion_solar.scenario_options
)

SELECT a.census_division_abbr, a.year, a.sector, a.escalation_factor, a.source
FROM esc_combined a
INNER JOIN inp_opts b
ON a.sector = b.sector
and a.source = b.source;

-- SELECT *
-- FROM diffusion_solar.rate_escalations_to_model
-- order by year;

-- costs for all turbine sizes and years
-- DROP VIEW IF EXISTS diffusion_solar.turbine_costs_per_size_and_year;
-- CREATE OR REPLACE VIEW diffusion_solar.turbine_costs_per_size_and_year AS
--  SELECT a.turbine_size_kw, a.turbine_height_m, b.year, 
--     -- normalized costs (i.e., costs per kw)
--     b.capital_cost_dollars_per_kw,
--     b.fixed_om_dollars_per_kw_per_yr,
--     b.variable_om_dollars_per_kwh,
--     b.cost_for_higher_towers_dollars_per_kw_per_m,
--     b.cost_for_higher_towers_dollars_per_kw_per_m * (a.turbine_height_m - b.default_tower_height_m) as tower_cost_adder_dollars_per_kw,
--     b.capital_cost_dollars_per_kw + (b.cost_for_higher_towers_dollars_per_kw_per_m * (a.turbine_height_m - b.default_tower_height_m)) AS installed_costs_dollars_per_kw
-- FROM diffusion_solar.allowable_turbine_sizes a
-- LEFT JOIN diffusion_solar.wind_cost_projections b  --this join will repeat the cost projections for each turbine height associated with each size
-- ON a.turbine_size_kw = b.turbine_size_kw;


