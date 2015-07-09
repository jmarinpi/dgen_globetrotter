set role 'diffusion-writers';


DROP TABLE IF EXISTS diffusion_template.input_main_inflation;
CREATE TABLE diffusion_template.input_main_inflation
(
	ann_inflation numeric NOT NULL
);


DROP TABLE if exists diffusion_template.input_main_market_projections;
CREATE TABLE diffusion_template.input_main_market_projections
(
	year integer,
	avoided_costs_dollars_per_kwh numeric,
	carbon_dollars_per_ton numeric,
	user_defined_res_rate_escalations numeric,
	user_defined_com_rate_escalations numeric,
	user_defined_ind_rate_escalations numeric,
	default_rate_escalations numeric
);


DROP TABLE if exists diffusion_template.input_main_flat_electric_rates_raw;
CREATE TABLE diffusion_template.input_main_flat_electric_rates_raw
(
  state_abbr character varying(2),
  res_rate_dlrs_per_kwh numeric,
  com_rate_dlrs_per_kwh numeric,
  ind_rate_dlrs_per_kwh numeric
);

DROP VIEW IF EXISTS diffusion_template.input_main_flat_electric_rates;
CREATE VIEW diffusion_template.input_main_flat_electric_rates AS
SELECT b.state_fips, a.*
FROM diffusion_template.input_main_flat_electric_rates_raw a
LEFT JOIN diffusion_shared.state_fips_lkup b
	ON a.state_abbr = b.state_abbr;

