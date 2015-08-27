set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_template.input_main_nem_utility_types CASCADE;
CREATE TABLE diffusion_template.input_main_nem_utility_types 
(
	utility_type_iou boolean NOT NULL,
	utility_type_muni boolean NOT NULL,
	utility_type_coop boolean NOT NULL,
	utility_type_allother boolean NOT NULL
);


DROP VIEW IF EXISTS diffusion_template.input_main_nem_utility_types_tidy;
CREATE VIEW diffusion_template.input_main_nem_utility_types_tidy AS
with a as
(
	select unnest(array[	case when utility_type_iou = True then 'IOU'
			END,
			case when utility_type_muni = True then 'Muni'
			END,
			case when utility_type_coop = True then 'Coop'
			END,
			case when utility_type_allother = True then 'All Other'
			ENd
		]) as utility_type
	from diffusion_template.input_main_nem_utility_types 
)
SELECT *
FROM a
where utility_type is not null;


DROP TABLE IF EXISTS diffusion_template.input_main_nem_selected_scenario;
CREATE TABLE diffusion_template.input_main_nem_selected_scenario 
(
	val text not null,
	CONSTRAINT input_main_nem_selected_scenario_fkey FOREIGN KEY (val)
		REFERENCES diffusion_config.sceninp_nem_scenario (val) MATCH SIMPLE
		ON DELETE RESTRICT
);


DROP VIEW IF EXISTS diffusion_template.input_main_nem_avoided_costs;
CREATE VIEW diffusion_template.input_main_nem_avoided_costs AS
SELECT 	a.year, 
	b.state_abbr,
	unnest(array['res','com','ind']) as sector_abbr,
	unnest(array['All Other', 'Coop', 'IOU', 'Muni']) as utility_type,
	0::double precision as system_size_limit_kw,
	0::numeric as year_end_excess_sell_rate_dlrs_per_kwh,
	a.avoided_costs_dollars_per_kwh as hourly_excess_sell_rate_dlrs_per_kwh
FROM diffusion_template.input_main_market_projections a
CROSS JOIN diffusion_shared.state_fips_lkup b
WHERE b.state_abbr <> 'PR';


DROP TABLE IF EXISTS diffusion_template.input_main_state_wholesale_elec_prices_raw;
CREATE TABLE diffusion_template.input_main_state_wholesale_elec_prices_raw
(
  state_abbr character(2),
  year integer,
  wholesale_elec_price_dollars_per_kwh numeric
);

DROP VIEW IF EXISTS diffusion_template.input_main_state_wholesale_elec_prices;
CREATE VIEW diffusion_template.input_main_state_wholesale_elec_prices AS
SELECT 	a.year, 
	a.state_abbr,
	unnest(array['res','com','ind']) as sector_abbr,
	unnest(array['All Other', 'Coop', 'IOU', 'Muni']) as utility_type,
	0::double precision as system_size_limit_kw,
	0::numeric as year_end_excess_sell_rate_dlrs_per_kwh,
	a.wholesale_elec_price_dollars_per_kwh as hourly_excess_sell_rate_dlrs_per_kwh
FROM diffusion_template.input_main_state_wholesale_elec_prices_raw a;

DROP TABLE IF EXISTS diffusion_template.input_main_nem_expiration_rate;
CREATE TABLE diffusion_template.input_main_nem_expiration_rate 
(
	val text not null,
	CONSTRAINT input_main_state_wholesale_elec_prices_fkey FOREIGN KEY (val)
		REFERENCES diffusion_config.sceninp_nem_expiration_rate (val) MATCH SIMPLE
		ON DELETE RESTRICT
);


DROP VIEW IF EXISTS diffusion_template.input_main_nem_flat_sell_rates CASCADE;
CREATE VIEW diffusion_template.input_main_nem_flat_sell_rates AS
with a as
(
	SELECT 	*, 'State Wholesale'::text as expiration_rate
	FROM diffusion_template.input_main_state_wholesale_elec_prices
	UNION ALL
	select *, 'Avoided Cost'::text as expiration_rate
	from diffusion_template.input_main_nem_avoided_costs
)
select a.year, a.state_abbr, a.sector_abbr, a.utility_type, 
	a.system_size_limit_kw, a.year_end_excess_sell_rate_dlrs_per_kwh, a.hourly_excess_sell_rate_dlrs_per_kwh
FROM a
inner join diffusion_template.input_main_nem_expiration_rate b
ON a.expiration_rate = b.val;


DROP TABLE IF EXISTS diffusion_template.input_main_nem_user_defined_scenario_raw;
CREATE TABLE diffusion_template.input_main_nem_user_defined_scenario_raw
(
	state_abbr character varying(2) NOT NULL,
	
	system_size_limit_kw_res double precision  NOT NULL,
	state_cap_limit_mw_res double precision  NOT NULL,
	year_end_excess_sell_rate_dlrs_per_kwh_res numeric NOT NULL, 
	hourly_excess_sell_rate_dlrs_per_kwh_res numeric NOT NULL,

	system_size_limit_kw_com double precision NOT NULL,
	state_cap_limit_mw_com double precision NOT NULL,
	year_end_excess_sell_rate_dlrs_per_kwh_com numeric NOT NULL,
	hourly_excess_sell_rate_dlrs_per_kwh_com numeric NOT NULL,

	system_size_limit_kw_ind double precision NOT NULL,
	state_cap_limit_mw_ind double precision NOT NULL,
	year_end_excess_sell_rate_dlrs_per_kwh_ind numeric NOT NULL,
	hourly_excess_sell_rate_dlrs_per_kwh_ind numeric NOT NULL,

	first_year integer NOT NULL,
	last_year integer NOT NULL,

	CONSTRAINT input_main_nem_user_defined_scenario_first_year_fkey FOREIGN KEY (first_year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON DELETE RESTRICT,

	CONSTRAINT input_main_nem_user_defined_scenario_last_year_fkey FOREIGN KEY (last_year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON DELETE RESTRICT

);


DROP VIEW IF EXISTS diffusion_template.input_main_nem_user_defined_scenario CASCADE;
CREATE VIEW diffusion_template.input_main_nem_user_defined_scenario AS
with a as
(
	SELECT generate_series(first_year, last_year, 2) as year,
		state_abbr,
		'res'::text as sector_abbr,
		system_size_limit_kw_res as system_size_limit_kw,
		year_end_excess_sell_rate_dlrs_per_kwh_res as year_end_excess_sell_rate_dlrs_per_kwh,
		hourly_excess_sell_rate_dlrs_per_kwh_res as hourly_excess_sell_rate_dlrs_per_kwh
	FROM diffusion_template.input_main_nem_user_defined_scenario_raw
	UNION ALL
	SELECT generate_series(first_year, last_year, 2) as year,
		state_abbr,
		'com'::text as sector_abbr,
		system_size_limit_kw_com as system_size_limit_kw,
		year_end_excess_sell_rate_dlrs_per_kwh_com as year_end_excess_sell_rate_dlrs_per_kwh,
		hourly_excess_sell_rate_dlrs_per_kwh_com as hourly_excess_sell_rate_dlrs_per_kwh
	FROM diffusion_template.input_main_nem_user_defined_scenario_raw
	UNION ALL
	SELECT generate_series(first_year, last_year, 2) as year,
		state_abbr,
		'ind'::text as sector_abbr,
		system_size_limit_kw_ind as system_size_limit_kw,
		year_end_excess_sell_rate_dlrs_per_kwh_ind as year_end_excess_sell_rate_dlrs_per_kwh,
		hourly_excess_sell_rate_dlrs_per_kwh_ind as hourly_excess_sell_rate_dlrs_per_kwh
	FROM diffusion_template.input_main_nem_user_defined_scenario_raw
),
b as
(
	SELECT a.year, a.state_abbr, a.sector_abbr, b.utility_type,
		a.system_size_limit_kw, a.year_end_excess_sell_rate_dlrs_per_kwh, a.hourly_excess_sell_rate_dlrs_per_kwh
	FROM a
	CROSS join diffusion_template.input_main_nem_utility_types_tidy b
)
select c.year, c.state_abbr, c.sector_abbr, c.utility_type, c.system_size_limit_kw,
	c.year_end_excess_sell_rate_dlrs_per_kwh, c.hourly_excess_sell_rate_dlrs_per_kwh
from diffusion_template.input_main_nem_flat_sell_rates c
left join b
	ON b.year = c.year
	AND b.state_abbr = c.state_abbr
	AND b.sector_abbr = c.sector_abbr
	AND b.utility_type = c.utility_type
where b.year IS NULL
UNION ALL
SELECT b.year, b.state_abbr, b.sector_abbr, b.utility_type, b.system_size_limit_kw,
	b.year_end_excess_sell_rate_dlrs_per_kwh, b.hourly_excess_sell_rate_dlrs_per_kwh
from b;


DROP VIEW IF EXISTS diffusion_template.input_main_nem_scenario;
CREATE VIEW diffusion_template.input_main_nem_scenario AS
with a as
(
	SELECT *, 'BAU' as scenario
	FROM diffusion_shared.nem_scenario_bau 
	UNION ALL
	SELECT *, 'Full Everywhere' as scenario
	FROM diffusion_shared.nem_scenario_full_everywhere 
	UNION ALL
	SELECT *, 'None Everywhere' as scenario
	FROM diffusion_shared.nem_scenario_none_everywhere
	UNION ALL
	SELECT *, 'Avoided Costs' as scenario
	FROM diffusion_template.input_main_nem_avoided_costs
	UNION ALL
	SELECT *, 'User-Defined' as scenario
	FROM diffusion_template.input_main_nem_user_defined_scenario
)
SELECT a.year, a.state_abbr, a.sector_abbr, a.utility_type, a.system_size_limit_kw, 
       a.year_end_excess_sell_rate_dlrs_per_kwh, a.hourly_excess_sell_rate_dlrs_per_kwh
FROM a
INNER JOIN diffusion_template.input_main_nem_selected_scenario b
ON a.scenario = b. val;