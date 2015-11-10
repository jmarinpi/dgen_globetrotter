SET ROLE 'diffusion-writers';
-- residential
DROP TABLE IF EXISTS diffusion_template.outputs_res;
CREATE TABLE diffusion_template.outputs_res
(
  micro_id integer,
  county_id integer,
  bin_id integer,
  year integer,
  business_model text,
  loan_term_yrs integer,
  loan_rate numeric,
  down_payment numeric,
  discount_rate numeric,
  tax_rate numeric,
  carbon_price_cents_per_kwh NUMERIC,
  fixed_om_dollars_per_kw_per_yr NUMERIC,
  variable_om_dollars_per_kwh NUMERIC,
  installed_costs_dollars_per_kw NUMERIC,
  inverter_cost_dollars_per_kw NUMERIC,
  length_of_irr_analysis_yrs integer,
  market_share_last_year numeric,
  number_of_adopters_last_year numeric,
  installed_capacity_last_year numeric,
  market_value_last_year numeric,
  value_of_increment numeric,
  value_of_pbi_fit numeric,
  value_of_ptc numeric,
  pbi_fit_length numeric,
  ptc_length integer,
  value_of_rebate numeric,
  value_of_tax_credit_or_deduction numeric,
  ic numeric,
  metric text,
  metric_value numeric,
  lcoe numeric,
  max_market_share numeric,
  diffusion_market_share numeric,
  new_market_share numeric,
  new_adopters numeric,
  new_capacity numeric,
  new_market_value numeric,
  market_share numeric,
  number_of_adopters numeric,
  installed_capacity numeric,
  market_value numeric,
  first_year_bill_with_system numeric,
  first_year_bill_without_system numeric,
  npv4 numeric,
  excess_generation_percent numeric,
  total_value_of_incentives numeric,
  value_of_itc numeric,
  tech text,
  selected_option boolean
);

CREATE INDEX outputs_res_join_fields_btree ON diffusion_template.outputs_res USING btree(county_id,bin_id,year);


-- commercial
DROP TABLE IF EXISTS diffusion_template.outputs_com;
CREATE TABLE diffusion_template.outputs_com
(
  micro_id integer,
  county_id integer,
  bin_id integer,
  year integer,
  business_model text,
  loan_term_yrs integer,
  loan_rate numeric,
  down_payment numeric,
  discount_rate numeric,
  tax_rate numeric,
  carbon_price_cents_per_kwh NUMERIC,
  fixed_om_dollars_per_kw_per_yr NUMERIC,
  variable_om_dollars_per_kwh NUMERIC,
  installed_costs_dollars_per_kw NUMERIC,
  inverter_cost_dollars_per_kw NUMERIC,
  length_of_irr_analysis_yrs integer,
  market_share_last_year numeric,
  number_of_adopters_last_year numeric,
  installed_capacity_last_year numeric,
  market_value_last_year numeric,
  value_of_increment numeric,
  value_of_pbi_fit numeric,
  value_of_ptc numeric,
  pbi_fit_length numeric,
  ptc_length integer,
  value_of_rebate numeric,
  value_of_tax_credit_or_deduction numeric,
  ic numeric,
  metric text,
  metric_value numeric,
  lcoe numeric,
  max_market_share numeric,
  diffusion_market_share numeric,
  new_market_share numeric,
  new_adopters numeric,
  new_capacity numeric,
  new_market_value numeric,
  market_share numeric,
  number_of_adopters numeric,
  installed_capacity numeric,
  market_value numeric,
  first_year_bill_with_system numeric,
  first_year_bill_without_system numeric,
  npv4 numeric,
  excess_generation_percent numeric,
  total_value_of_incentives numeric,
  value_of_itc numeric,
  tech text,
  selected_option boolean
);

CREATE INDEX outputs_com_join_fields_btree ON diffusion_template.outputs_com USING btree(county_id,bin_id,year);


-- industrial
DROP TABLE IF EXISTS diffusion_template.outputs_ind;
CREATE TABLE diffusion_template.outputs_ind
(
  micro_id integer,
  county_id integer,
  bin_id integer,
  year integer,
  business_model text,
  loan_term_yrs integer,
  loan_rate numeric,
  down_payment numeric,
  discount_rate numeric,
  tax_rate numeric,
  carbon_price_cents_per_kwh NUMERIC,
  fixed_om_dollars_per_kw_per_yr NUMERIC,
  variable_om_dollars_per_kwh NUMERIC,
  installed_costs_dollars_per_kw NUMERIC,
  inverter_cost_dollars_per_kw NUMERIC,
  length_of_irr_analysis_yrs integer,
  market_share_last_year numeric,
  number_of_adopters_last_year numeric,
  installed_capacity_last_year numeric,
  market_value_last_year numeric,
  value_of_increment numeric,
  value_of_pbi_fit numeric,
  value_of_ptc numeric,
  pbi_fit_length numeric,
  ptc_length integer,
  value_of_rebate numeric,
  value_of_tax_credit_or_deduction numeric,
  ic numeric,
  metric text,
  metric_value numeric,
  lcoe numeric,
  max_market_share numeric,
  diffusion_market_share numeric,
  new_market_share numeric,
  new_adopters numeric,
  new_capacity numeric,
  new_market_value numeric,
  market_share numeric,
  number_of_adopters numeric,
  installed_capacity numeric,
  market_value numeric,
  first_year_bill_with_system numeric,
  first_year_bill_without_system numeric,
  npv4 numeric,
  excess_generation_percent numeric,
  total_value_of_incentives numeric,
  value_of_itc numeric,
  tech text,
  selected_option boolean
);

CREATE INDEX outputs_ind_join_fields_btree ON diffusion_template.outputs_ind USING btree(county_id,bin_id,year);

------------------------------------------------------------------------------------------------------------
-- tables to hold results from each previous model year 

DROP TABLE IF EXISTS diffusion_template.output_market_last_year;
CREATE TABLE diffusion_template.output_market_last_year
(
	county_id INTEGER,
	bin_id INTEGER,
	market_share_last_year NUMERIC,
	max_market_share_last_year NUMERIC,
	number_of_adopters_last_year NUMERIC,
	installed_capacity_last_year NUMERIC,
	market_value_last_year NUMERIC,
	tech text,
	sector_abbr character varying(3)
);
