SET ROLE 'diffusion-writers';

-- create view of the valid counties
DROP VIEW IF EXISTS diffusion_template.counties_to_model CASCADE;
CREATE OR REPLACE VIEW diffusion_template.counties_to_model AS
SELECT county_id, census_region, census_division_abbr, recs_2009_reportable_domain as reportable_domain,
	a.climate_zone_building_america, a.climate_zone_cbecs_2003
FROM diffusion_shared.county_geom a
INNER JOIN diffusion_template.input_main_scenario_options b
ON lower(a.state) = CASE WHEN b.region = 'United States' then lower(a.state)
		else lower(b.region)
		end
where a.state not in ('Hawaii','Alaska');

-- view for carbon intensities
DROP VIEW IF EXISTS diffusion_template.carbon_intensities_to_model CASCADE;
CREATE OR REPLACE VIEW diffusion_template.carbon_intensities_to_model AS
SELECT state_abbr,
	CASE WHEN b.carbon_price = 'No Carbon Price' THEN no_carbon_price_t_per_kwh
	WHEN b.carbon_price = 'Price Based On State Carbon Intensity' THEN state_carbon_price_t_per_kwh
	WHEN b.carbon_price = 'Price Based On NG Offset' THEN ng_offset_t_per_kwh
	END as carbon_intensity_t_per_kwh
FROM diffusion_shared.carbon_intensities a
CROSS JOIN diffusion_template.input_main_scenario_options b;


------------------------------------------------------------------------------------
-- joined point microdata

-- ind
DROP VIEW IF EXISTS diffusion_template.point_microdata_ind_us_joined;
CREATE OR REPLACE VIEW diffusion_template.point_microdata_ind_us_joined AS
SELECT  a.micro_id,
	a.county_id,
	a.utility_type,
	a.hdf_load_index,
	a.pca_reg,
	a.reeds_reg,
	e.state_abbr,
	e.state_fips,
	e.census_division_abbr,
	e.census_region,
	e.climate_zone_cbecs_2003 as climate_zone,
	a.ranked_rate_array_id,
	b.total_customers_2011_industrial as county_total_customers_2011,
	b.total_load_mwh_2011_industrial as county_total_load_mwh_2011,
	l.carbon_intensity_t_per_kwh,
	-- solar only
	a.ulocale,
	a.solar_re_9809_gid,
	a.solar_incentive_array_id as incentive_array_id_solar,
	d.pv_20mw_cap_cost_multplier as cap_cost_multiplier_solar,
	-- wind only
	a.hi_dev_pct,
	a.acres_per_hu,
	a.canopy_ht_m,
	a.canopy_pct_hi,
	a.i,
	a.j,
	a.cf_bin,
	a.wind_incentive_array_id as incentive_array_id_wind,
	d.onshore_wind_cap_cost_multiplier as cap_cost_multiplier_wind
FROM diffusion_shared.point_microdata_ind_us a
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
INNER JOIN diffusion_template.counties_to_model h 
ON a.county_id = h.county_id
-- carbon intensities
LEFT JOIN diffusion_template.carbon_intensities_to_model l
ON e.state_abbr = l.state_abbr;


-- res
DROP VIEW IF EXISTS diffusion_template.point_microdata_res_us_joined;
CREATE OR REPLACE VIEW diffusion_template.point_microdata_res_us_joined AS
SELECT 	a.micro_id,
	a.county_id,
	a.utility_type,
	a.hdf_load_index,
	a.pca_reg,
	a.reeds_reg,
	e.state_abbr,
	e.state_fips,
	e.census_division_abbr,
	e.census_region,
	e.climate_zone_building_america as climate_zone,
	a.ranked_rate_array_id,
	b.total_customers_2011_residential * k.perc_own_occu_1str_housing as county_total_customers_2011,
	b.total_load_mwh_2011_residential * k.perc_own_occu_1str_housing as county_total_load_mwh_2011,
	l.carbon_intensity_t_per_kwh,
	-- solar only
	a.ulocale,
	a.solar_re_9809_gid,
	a.solar_incentive_array_id as incentive_array_id_solar,
	d.pv_20mw_cap_cost_multplier as cap_cost_multiplier_solar,
	-- wind only
	a.hi_dev_pct,
	a.acres_per_hu,
	a.canopy_ht_m,
	a.canopy_pct_hi,
	a.i,
	a.j,
	a.cf_bin,
	a.wind_incentive_array_id as incentive_array_id_wind,
	d.onshore_wind_cap_cost_multiplier as cap_cost_multiplier_wind
FROM diffusion_shared.point_microdata_res_us a
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
INNER JOIN diffusion_template.counties_to_model h 
ON a.county_id = h.county_id
-- carbon intensities
LEFT JOIN diffusion_template.carbon_intensities_to_model l
ON e.state_abbr = l.state_abbr;


-- comm
DROP VIEW IF EXISTS diffusion_template.point_microdata_com_us_joined;
CREATE OR REPLACE VIEW diffusion_template.point_microdata_com_us_joined AS
SELECT 	a.micro_id,
	a.county_id,
	a.utility_type,
	a.hdf_load_index,
	a.pca_reg,
	a.reeds_reg,
	e.state_abbr,
	e.state_fips,
	e.census_division_abbr,
	e.census_region,
	e.climate_zone_cbecs_2003 as climate_zone,
	a.ranked_rate_array_id,
	b.total_customers_2011_commercial as county_total_customers_2011,
	b.total_load_mwh_2011_commercial as county_total_load_mwh_2011,
	l.carbon_intensity_t_per_kwh,
	-- solar only
	a.ulocale,
	a.solar_re_9809_gid,
	a.solar_incentive_array_id as incentive_array_id_solar,
	d.pv_20mw_cap_cost_multplier as cap_cost_multiplier_solar,
	-- wind only
	a.hi_dev_pct,
	a.acres_per_hu,
	a.canopy_ht_m,
	a.canopy_pct_hi,
	a.i,
	a.j,
	a.cf_bin,
	a.wind_incentive_array_id as incentive_array_id_wind,
	d.onshore_wind_cap_cost_multiplier as cap_cost_multiplier_wind
FROM diffusion_shared.point_microdata_com_us a
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
INNER JOIN diffusion_template.counties_to_model h 
ON a.county_id = h.county_id
-- carbon intensities
LEFT JOIN diffusion_template.carbon_intensities_to_model l
ON e.state_abbr = l.state_abbr;
------------------------------------------------------------------------

-- create view of sectors to model
DROP VIEW IF EXIStS diffusion_template.sectors_to_model;
CREATE OR REPLACE VIEW diffusion_template.sectors_to_model AS
SELECT CASE WHEN markets = 'All' THEN 'res=>Residential,com=>Commercial,ind=>Industrial'::hstore
	    when markets = 'Only Residential' then 'res=>Residential'::hstore
	    when markets = 'Only Commercial' then 'com=>Commercial'::hstore
	    when markets = 'Only Industrial' then 'ind=>Industrial'::hstore
	   end as sectors
FROM diffusion_template.input_main_scenario_options;

set role 'diffusion-writers';
-- max market share
DROP VIEW IF EXISTS diffusion_template.max_market_curves_to_model;
CREATE OR REPLACE VIEW diffusion_template.max_market_curves_to_model As
with user_inputs as 
(
	-- user selections for host owned curves
	SELECT 'residential' as sector, 'res'::character varying(3) as sector_abbr, 'host_owned' as business_model,
		res_max_market_curve as source
	FROM diffusion_template.input_main_scenario_options
	UNION
	SELECT 'commercial' as sector, 'com'::character varying(3) as sector_abbr, 'host_owned' as business_model,
		com_max_market_curve as source
	FROM diffusion_template.input_main_scenario_options
	UNION
	SELECT 'industrial' as sector, 'ind'::character varying(3) as sector_abbr, 'host_owned' as business_model,
		ind_max_market_curve as source
	FROM diffusion_template.input_main_scenario_options
	UNION
	-- default selections for third party owned curves (only one option -- NREL)
	SELECT unnest(array['residential','commercial','industrial']) as sector, 
		unnest(array['res','com','ind']) as sector_abbr, 
		'tpo' as business_model,
		'NREL' as source	
),
all_maxmarket as 
(
	SELECT metric_value, sector, sector_abbr, max_market_share, metric, 
		source, business_model
	FROM diffusion_shared.max_market_share
)
SELECT a.*
FROM all_maxmarket a
INNER JOIN user_inputs b
ON a.sector = b.sector
and a.source = b.source
and a.business_model = b.business_model
order by sector, metric, metric_value;

-- create view for rate escalations
DROP VIEW IF EXISTS diffusion_template.rate_escalations_to_model;
CREATE OR REPLACE VIEW diffusion_template.rate_escalations_to_model AS
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
	LEFT JOIN diffusion_template.input_main_market_projections a
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
	LEFT JOIN diffusion_template.input_main_market_projections a
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
	LEFT JOIN diffusion_template.input_main_market_projections a
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
	SELECT census_division_abbr, year, sector_abbr as sector, 
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
	FROM diffusion_template.input_main_scenario_options
	UNION
	SELECT 'com'::text as sector, com_rate_escalation as source
	FROM diffusion_template.input_main_scenario_options
	UNION
	SELECT 'ind'::text as sector, ind_rate_escalation as source
	FROM diffusion_template.input_main_scenario_options
)

SELECT a.census_division_abbr, a.year, a.sector, a.escalation_factor, a.source
FROM esc_combined a
INNER JOIN inp_opts b
ON a.sector = b.sector
and a.source = b.source;


-- costs for all turbine sizes and years
DROP VIEW IF EXISTS diffusion_template.turbine_costs_per_size_and_year;
CREATE OR REPLACE VIEW diffusion_template.turbine_costs_per_size_and_year AS
 SELECT a.turbine_size_kw, a.turbine_height_m, b.year, 
    -- normalized costs (i.e., costs per kw)
    b.capital_cost_dollars_per_kw,
    b.fixed_om_dollars_per_kw_per_yr,
    b.variable_om_dollars_per_kwh,
    b.cost_for_higher_towers_dollars_per_kw_per_m,
    b.cost_for_higher_towers_dollars_per_kw_per_m * (a.turbine_height_m - b.default_tower_height_m) as tower_cost_adder_dollars_per_kw,
    b.capital_cost_dollars_per_kw + (b.cost_for_higher_towers_dollars_per_kw_per_m * (a.turbine_height_m - b.default_tower_height_m)) AS installed_costs_dollars_per_kw
FROM diffusion_wind.allowable_turbine_sizes a
LEFT JOIN diffusion_template.input_wind_cost_projections b  --this join will repeat the cost projections for each turbine height associated with each size
ON a.turbine_size_kw = b.turbine_size_kw;



-- create a view of all of the different types of rates
DROP VIEW IF EXISTS diffusion_template.all_rate_jsons;
CREATE VIEW diffusion_template.all_rate_jsons AS
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
FROM diffusion_template.input_main_market_flat_electric_rates a
UNION ALL
-- user-defined flat rates (commercial)
SELECT 'udcom'::character varying(5) as rate_source,
	a.state_fips as rate_id_alias, 
	('{"ur_flat_buy_rate" : ' || round(com_rate_dlrs_per_kwh,2)::text || '}')::JSON as sam_json
FROM diffusion_template.input_main_market_flat_electric_rates a
UNION ALL
-- user-defined flat rates (industrial)
SELECT 'udind'::character varying(5) as rate_source,
	a.state_fips as rate_id_alias, 
	('{"ur_flat_buy_rate" : ' || round(ind_rate_dlrs_per_kwh,2)::text || '}')::JSON as sam_json
FROM diffusion_template.input_main_market_flat_electric_rates a;


------------------------------------------------------------------------------------------------
-- finances
set role 'diffusion-writers';

DROP VIEW IF EXISTS diffusion_template.input_finances;
CREATE VIEW diffusion_template.input_finances AS
select *, 'wind'::text as tech
from diffusion_template.input_wind_finances
UNION ALL
select *, 'solar'::text as tech
from diffusion_template.input_solar_finances;

------------------------------------------------------------------------------------------------
-- finances
set role 'diffusion-writers';

DROP VIEW IF EXISTS diffusion_template.input_financial_parameters;
CREATE VIEW diffusion_template.input_financial_parameters AS
select ownership_model, loan_term_yrs, loan_rate, down_payment, 
       discount_rate, tax_rate, length_of_irr_analysis_yrs, 'wind'::text as tech,
	case   when sector = 'residential' then 'res'::CHARACTER VARYING(3)
		when sector = 'commercial' then 'com'::CHARACTER VARYING(3)
		when sector = 'industrial' then 'ind'::CHARACTER VARYING(3)
	end as sector_abbr
from diffusion_template.input_wind_finances
UNION ALL
select ownership_model, loan_term_yrs, loan_rate, down_payment, 
       discount_rate, tax_rate, length_of_irr_analysis_yrs, 'solar'::text as tech,
	case   when sector = 'residential' then 'res'::CHARACTER VARYING(3)
		when sector = 'commercial' then 'com'::CHARACTER VARYING(3)
		when sector = 'industrial' then 'ind'::CHARACTER VARYING(3)
	end as sector_abbr
from diffusion_template.input_solar_finances;

------------------------------------------------------------------------------------------------
-- manual incentive options
DROP VIEW IF EXISTS diffusion_template.input_incentive_options;
CREATE VIEW diffusion_template.input_incentive_options AS
SELECT overwrite_exist_inc, incentive_start_year, 'wind'::text as tech
FROM diffusion_template.input_wind_incentive_options
UNION ALL
SELECT overwrite_exist_inc, incentive_start_year, 'solar'::text as tech
FROM diffusion_template.input_solar_incentive_options;

------------------------------------------------------------------------------------------------
-- depreciation schedule
DROP VIEW IF EXISTS diffusion_template.input_finances_depreciation_schedule;
CREATE VIEW diffusion_template.input_finances_depreciation_schedule AS
SELECT *, 'wind'::text as tech
FROM diffusion_template.input_wind_finances_depreciation_schedule 
UNION ALL
SELECT *, 'solar'::text as tech
FROM diffusion_template.input_solar_finances_depreciation_schedule;

------------------------------------------------------------------------------------------------
-- annual system degradation
DROP VIEW IF EXISTS diffusion_template.input_performance_annual_system_degradation;
CREATE VIEW diffusion_template.input_performance_annual_system_degradation AS
SELECT 0::numeric as ann_system_degradation, 'wind'::text as tech
UNION ALL
SELECT ann_system_degradation, 'solar'::text as tech
FROM diffusion_template.input_solar_performance_annual_system_degradation;
