SET ROLE 'diffusion-writers';

-- create view of the valid counties
CREATE OR REPLACE VIEW diffusion_solar.counties_to_model AS
SELECT county_id, census_region, state_abbr, census_division_abbr, recs_2009_reportable_domain as reportable_domain
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
SELECT a.sector, a.utility_type, a.nem_system_limit_kw, a.state_abbr
FROM diffusion_shared.net_metering_availability_2013 a;


-- views of point data
-- ind
DROP VIEW IF EXISTS diffusion_solar.point_microdata_ind_us_joined;
CREATE OR REPLACE VIEW diffusion_solar.point_microdata_ind_us_joined AS
SELECT a.micro_id, a.county_id, a.utility_type, a.hdf_load_index,
	a.pca_reg, a.reeds_reg, a.incentive_array_id, a.ranked_rate_array_id,
	b.total_customers_2011_industrial as county_total_customers_2011, 
	b.total_load_mwh_2011_industrial as county_total_load_mwh_2011,
	d.pv_20mw_cap_cost_multplier as cap_cost_multiplier,
	e.state_abbr, e.state_fips, e.census_division_abbr, e.census_region,
	a.solar_re_9809_gid, 
	l.carbon_intensity_t_per_kwh,
	m.nem_system_limit_kw
FROM diffusion_solar.point_microdata_ind_us a
-- county_load_and_customers
LEFT JOIN diffusion_shared.load_and_customers_by_county_us b
ON a.county_id = b.county_id
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
SELECT a.micro_id, a.county_id, a.utility_type, a.hdf_load_index,
	a.pca_reg, a.reeds_reg, a.incentive_array_id, a.ranked_rate_array_id,
	b.total_customers_2011_residential * k.perc_own_occu_1str_housing as county_total_customers_2011, 
	b.total_load_mwh_2011_residential * k.perc_own_occu_1str_housing as county_total_load_mwh_2011,
	d.pv_20mw_cap_cost_multplier as cap_cost_multiplier,
	e.state_abbr, e.state_fips, e.census_division_abbr, e.census_region,
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
SELECT a.micro_id, a.county_id, a.utility_type, a.hdf_load_index,
	a.pca_reg, a.reeds_reg, a.incentive_array_id, a.ranked_rate_array_id,
	b.total_customers_2011_commercial as county_total_customers_2011, 
	b.total_load_mwh_2011_commercial as county_total_load_mwh_2011,
	d.pv_20mw_cap_cost_multplier as cap_cost_multiplier,
	e.state_abbr, e.state_fips, e.census_division_abbr, e.census_region, 
	a.solar_re_9809_gid,
	l.carbon_intensity_t_per_kwh,
	m.nem_system_limit_kw
FROM diffusion_solar.point_microdata_com_us a
-- county_load_and_customers
LEFT JOIN diffusion_shared.load_and_customers_by_county_us b
ON a.county_id = b.county_id
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
DROP VIEW IF EXISTS diffusion_solar.max_market_curves_to_model;
CREATE OR REPLACE VIEW diffusion_solar.max_market_curves_to_model As
with user_inputs as 
(
	-- user selections for host owned curves
	SELECT 'residential' as sector, 'host_owned' as business_model,
		res_max_market_curve as source
	FROM diffusion_solar.scenario_options
	UNION
	SELECT 'commercial' as sector, 'host_owned' as business_model,
		com_max_market_curve as source
	FROM diffusion_solar.scenario_options
	UNION
	SELECT 'industrial' as sector, 'host_owned' as business_model,
		ind_max_market_curve as source
	FROM diffusion_solar.scenario_options
	UNION
	-- default selections for third party owned curves (only one option -- NREL)
	SELECT unnest(array['residential','commercial','industrial']) as sector, 
		'tpo' as business_model,
		'NREL' as source	
),
all_maxmarket as 
(
	SELECT metric_value, sector, max_market_share, metric, 
		source, business_model
	FROM diffusion_shared.max_market_share_revised
)
SELECT a.*
FROM all_maxmarket a
INNER JOIN user_inputs b
ON a.sector = b.sector
and a.source = b.source
and a.business_model = b.business_model
order by sector, metric, metric_value;


-- cost projections
CREATE OR REPLACE VIEW diffusion_solar.cost_projections_to_model As
WITH a as 
(
	SELECT year, capital_cost_dollars_per_kw, inverter_cost_dollars_per_kw, 
	       fixed_om_dollars_per_kw_per_yr, variable_om_dollars_per_kwh, 
	       sector, 'User Defined'::text as source
	FROM diffusion_solar.solar_cost_projections

	UNION ALL

	SELECT year, capital_cost_dollars_per_kw, inverter_cost_dollars_per_kw, 
		fixed_om_dollars_per_kw_per_yr, variable_om_dollars_per_kwh, 
		sector, 'Solar Program Targets'::text as source
	FROM diffusion_solar.solar_program_target_cost_projections
)
SELECT a.*
FROM a
INNER JOIN diffusion_solar.scenario_options b
ON a.source = b.cost_assumptions;


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
	SELECT b.census_division_abbr, b.year, 'res'::text as sector,
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
	SELECT b.census_division_abbr, b.year, 'com'::text as sector,
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
	SELECT b.census_division_abbr, b.year, 'ind'::text as sector,
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
SELECT census_division_abbr, year, unnest(array['res','com','ind'])::text as sector,
		1::numeric as escalation_factor,
		'No Growth'::text as source
	FROM cdas
),
aeo AS 
(
	SELECT census_division_abbr, year, sector_abbr::text as sector, 
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
	SELECT 'res'::text as sector, res_rate_escalation as source
	FROM diffusion_solar.scenario_options
	UNION
	SELECT 'com'::text as sector, com_rate_escalation as source
	FROM diffusion_solar.scenario_options
	UNION
	SELECT 'ind'::text as sector, ind_rate_escalation as source
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


set role 'diffusion-writers';
-- create a view of all of the different types of rates
DROP VIEW IF EXISTS diffusion_solar.all_rate_jsons;
CREATE VIEW diffusion_solar.all_rate_jsons AS
-- urdb3 complex rates
SELECT 'urdb3'::character varying(5) as rate_source,
	rate_id_alias, 
	sam_json
FROM diffusion_shared.urdb3_rate_sam_jsons
UNION ALL
-- annual average flat rates (residential)
SELECT 'aares'::character varying(5) as rate_source,
	a.county_id as rate_id_alias, 
	('{"ur_flat_buy_rate" : ' || round(res_rate_cents_per_kwh/100,2)::text || '}')::JSON as sam_json
FROM diffusion_shared.ann_ave_elec_rates_by_county_2012 a
UNION ALL
-- annual average flat rates (commercial)
SELECT 'aacom'::character varying(5) as rate_source,
	a.county_id as rate_id_alias, 
	('{"ur_flat_buy_rate" : ' || round(com_rate_cents_per_kwh/100,2)::text || '}')::JSON as sam_json
FROM diffusion_shared.ann_ave_elec_rates_by_county_2012 a
UNION ALL
-- annual average flat rates (industrial)
SELECT 'aaind'::character varying(5) as rate_source,
	a.county_id as rate_id_alias, 
	('{"ur_flat_buy_rate" : ' || round(ind_rate_cents_per_kwh/100,2)::text || '}')::JSON as sam_json
FROM diffusion_shared.ann_ave_elec_rates_by_county_2012 a
UNION ALL
-- user-defined flat rates (residential)
SELECT 'udres'::character varying(5) as rate_source,
	a.state_fips as rate_id_alias, 
	('{"ur_flat_buy_rate" : ' || round(res_rate_dlrs_per_kwh,2)::text || '}')::JSON as sam_json
FROM diffusion_solar.user_defined_electric_rates a
UNION ALL
-- user-defined flat rates (commercial)
SELECT 'udcom'::character varying(5) as rate_source,
	a.state_fips as rate_id_alias, 
	('{"ur_flat_buy_rate" : ' || round(com_rate_dlrs_per_kwh,2)::text || '}')::JSON as sam_json
FROM diffusion_solar.user_defined_electric_rates a
UNION ALL
-- user-defined flat rates (industrial)
SELECT 'udind'::character varying(5) as rate_source,
	a.state_fips as rate_id_alias, 
	('{"ur_flat_buy_rate" : ' || round(ind_rate_dlrs_per_kwh,2)::text || '}')::JSON as sam_json
FROM diffusion_solar.user_defined_electric_rates a;
