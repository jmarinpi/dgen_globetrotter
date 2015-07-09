﻿set role 'server-superusers';
select add_schema('diffusion_config','diffusion');

set role 'diffusion-writers';

------------------------------------------------------------------------
-- carbon price
DROP TABLE IF EXISTS diffusion_config.sceninp_carbon_price;
CREATE TABLE diffusion_config.sceninp_carbon_price
(
  val text unique not null
);

INSERT INTO diffusion_config.sceninp_carbon_price
select *
from unnest(array[
	'Price Based On NG Offset',
	'No Carbon Price',
	'Price Based On State Carbon Intensity'
	]);
------------------------------------------------------------------------


select *
FROM diffusion_wind_config.sceninp_height_exclusions

------------------------------------------------------------------------
-- max market curve
DROP TABLE IF EXISTS diffusion_config.sceninp_max_market_curve;
CREATE TABLE diffusion_config.sceninp_max_market_curve
(
  val text unique not null
);

INSERT INTO diffusion_config.sceninp_max_market_curve
select *
from unnest(array[
	'NEMS',
	'Navigant',
	'User Fit',
	'RW Beck'
	]);
------------------------------------------------------------------------


------------------------------------------------------------------------
-- rate escalations
DROP TABLE IF EXISTS diffusion_config.sceninp_rate_escalation;
CREATE TABLE diffusion_config.sceninp_rate_escalation
(
  val text unique not null
);

INSERT INTO diffusion_config.sceninp_rate_escalation
select *
from unnest(array[
	'AEO2014',
	'No Growth',
	'User Defined'
	]);
------------------------------------------------------------------------


------------------------------------------------------------------------
-- rate structures
DROP TABLE IF EXISTS diffusion_config.sceninp_rate_structure;
CREATE TABLE diffusion_config.sceninp_rate_structure
(
  val text unique not null
);

INSERT INTO diffusion_config.sceninp_rate_structure
select *
from unnest(array[
	'Complex Rates',
	'Flat (Annual Average)',
	'Flat (User-Defined)'
	]);
------------------------------------------------------------------------


------------------------------------------------------------------------
-- solar cost assumptions
DROP TABLE IF EXISTS diffusion_config.sceninp_cost_assumptions_solar;
CREATE TABLE diffusion_config.sceninp_cost_assumptions_solar
(
  val text unique not null
);

INSERT INTO diffusion_config.sceninp_cost_assumptions_solar
select *
from unnest(array[
	'Solar Program Targets',
	'AEO 2014',
	'User Defined'
	]);
------------------------------------------------------------------------


------------------------------------------------------------------------
-- model year range (end year and incentive start year)
DROP TABLE IF EXISTS diffusion_config.sceninp_year_range;
CREATE TABLE diffusion_config.sceninp_year_range
(
  val integer unique not null
);

INSERT INTO diffusion_config.sceninp_year_range
select *
from unnest(array[
	2014,
	2016,
	2018,
	2020,
	2022,
	2024,
	2026,
	2028,
	2030,
	2032,
	2034,
	2036,
	2038,
	2040,
	2042,
	2044,
	2046,
	2048,
	2050
	]);
------------------------------------------------------------------------


------------------------------------------------------------------------
-- load growth scenario
DROP TABLE IF EXISTS diffusion_config.sceninp_load_growth_scenario;
CREATE TABLE diffusion_config.sceninp_load_growth_scenario
(
  val text unique not null
);

INSERT INTO diffusion_config.sceninp_load_growth_scenario
select *
from unnest(array[
	'AEO 2014 No Load growth after 2014',
	'AEO 2014 Low Growth Case',
	'AEO 2014 Reference Case',
	'AEO 2014 High Growth Case',
	'AEO 2014 2x Growth Rate of Reference Case'
	]);
------------------------------------------------------------------------


------------------------------------------------------------------------
-- markets
DROP TABLE IF EXISTS diffusion_config.sceninp_markets;
CREATE TABLE diffusion_config.sceninp_markets
(
  val text unique not null
);

INSERT INTO diffusion_config.sceninp_markets
select *
from unnest(array[
		'All',
		'Only Residential',
		'Only Commercial',
		'Only Industrial'
	]);
------------------------------------------------------------------------


------------------------------------------------------------------------
-- regions
DROP TABLE IF EXISTS diffusion_config.sceninp_region;
CREATE TABLE diffusion_config.sceninp_region
(
  val text unique not null
);

INSERT INTO diffusion_config.sceninp_region
select *
from unnest(array[
		'United States',
		'Alabama',
		'Arizona',
		'Arkansas',
		'California',
		'Colorado',
		'Connecticut',
		'Delaware',
		'Florida',
		'Georgia',
		'Idaho',
		'Illinois',
		'Indiana',
		'Iowa',
		'Kansas',
		'Kentucky',
		'Louisiana',
		'Maine',
		'Maryland',
		'Massachusetts',
		'Michigan',
		'Minnesota',
		'Mississippi',
		'Missouri',
		'Montana',
		'Nebraska',
		'Nevada',
		'New Hampshire',
		'New Jersey',
		'New Mexico',
		'New York',
		'North Carolina',
		'North Dakota',
		'Ohio',
		'Oklahoma',
		'Oregon',
		'Pennsylvania',
		'Rhode Island',
		'South Carolina',
		'South Dakota',
		'Tennessee',
		'Texas',
		'Utah',
		'Vermont',
		'Virginia',
		'Washington',
		'West Virginia',
		'Wisconsin',
		'Wyoming',
		'District of Columbia'
	]);
------------------------------------------------------------------------

