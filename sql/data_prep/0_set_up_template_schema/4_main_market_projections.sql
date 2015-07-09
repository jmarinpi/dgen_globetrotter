set role 'diffusion-writers';


DROP TABLE IF EXISTS diffusion_template.input_main_inflation;
CREATE TABLE diffusion_template.input_main_inflation
(
	ann_inflation numeric NOT NULL
);


DROP TABLE if exists diffusion_template.input_main_market_projections;
CREATE TABLE diffusion_template.input_main_market_projections
(
	year integer NOT NULL,
	avoided_costs_dollars_per_kwh numeric NOT NULL,
	carbon_dollars_per_ton numeric NOT NULL,
	user_defined_res_rate_escalations numeric NOT NULL,
	user_defined_com_rate_escalations numeric NOT NULL,
	user_defined_ind_rate_escalations numeric NOT NULL,
	default_rate_escalations numeric NOT NULL
);


DROP TABLE if exists diffusion_template.input_main_flat_electric_rates_raw;
CREATE TABLE diffusion_template.input_main_flat_electric_rates_raw
(
  state_abbr character varying(2) NOT NULL,
  res_rate_dlrs_per_kwh numeric NOT NULL,
  com_rate_dlrs_per_kwh numeric NOT NULL,
  ind_rate_dlrs_per_kwh numeric NOT NULL
);

DROP VIEW IF EXISTS diffusion_template.input_main_flat_electric_rates;
CREATE VIEW diffusion_template.input_main_flat_electric_rates AS
SELECT b.state_fips, a.*
FROM diffusion_template.input_main_flat_electric_rates_raw a
LEFT JOIN diffusion_shared.state_fips_lkup b
	ON a.state_abbr = b.state_abbr;


DROP TABLE IF EXISTS diffusion_template.input_main_rate_type_weights_raw CASCADE;
CREATE TABLE diffusion_template.input_main_rate_type_weights_raw
(
	rate_type_desc text  NOT NULL,
	res_weight numeric NOT NULL,
	com_ind_weight numeric NOT NULL
);

DROP VIEW IF EXISTS diffusion_template.input_main_rate_type_weights;
CREATE VIEW diffusion_template.input_main_rate_type_weights AS
Select b.rate_type, a.rate_type_desc, 
	a.res_weight, a.com_ind_weight as com_weight, a.com_ind_weight as ind_weight
from diffusion_template.input_main_rate_type_weights_raw a
LEFT JOIN diffusion_shared.rate_type_desc_lkup b
ON a.rate_type_desc = b.rate_type_desc;
