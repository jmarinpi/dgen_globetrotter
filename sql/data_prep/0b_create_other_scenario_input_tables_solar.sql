DROP TABLE IF EXISTS diffusion_solar.solar_performance_improvements;
CREATE TABLE diffusion_solar.solar_performance_improvements
(
	year integer,
	pv_efficiency_pct numeric,
	user_pv_efficiency_pct numeric,
	density_w_per_sqft numeric,
	user_density_w_per_sqft numeric,
	inverter_lifetime_yr integer
);



DROP TABLE IF EXISTS diffusion_solar.financial_parameters;
CREATE TABLE diffusion_solar.financial_parameters
(
  sector text,
  ownership_model text,
  loan_term_yrs integer,
  loan_rate numeric,
  down_payment numeric,
  discount_rate numeric,
  tax_rate numeric,
  length_of_irr_analysis_yrs integer
);


DROP TABLE IF EXISTS diffusion_solar.user_defined_max_market_share;
CREATE TABLE diffusion_solar.user_defined_max_market_share
(
  year numeric,
  sector text,
  new double precision,
  retrofit double precision
);


DROP TABLE IF EXISTS diffusion_solar.depreciation_schedule;
CREATE TABLE diffusion_solar.depreciation_schedule
(
  year integer,
  macrs numeric,
  standard numeric
);


DROP TABLE IF EXISTS diffusion_solar.market_projections;
CREATE TABLE diffusion_solar.market_projections
(
  year integer,
  nat_gas_dollars_per_mmbtu numeric,
  coal_price_dollars_per_mmbtu numeric,
  carbon_dollars_per_ton numeric,
  user_defined_res_rate_escalations numeric,
  user_defined_com_rate_escalations numeric,
  user_defined_ind_rate_escalations numeric,
  default_rate_escalations numeric
);

DROP TABLE IF EXISTS diffusion_solar.statewide_carbon_and_ng_intensities ;
CREATE TABLE diffusion_solar.statewide_carbon_and_ng_intensities 
(
	state_abbr character varying(2),
	co2_intensity_tons_per_kwh numeric,
	ng_intensity numeric
);


DROP TABLE IF EXISTS diffusion_solar.manual_incentives;
CREATE TABLE diffusion_solar.manual_incentives
(
  region text,
  type text,
  sector text,
  incentive double precision,
  cap numeric,
  expire numeric,
  incentives_c_per_kwh double precision,
  no_years numeric,
  dol_per_kw double precision,
  total_budget numeric,
  utility_type character varying(9)
);

DROP TABLE IF EXISTS diffusion_solar.manual_net_metering_availability;
CREATE TABLE diffusion_solar.manual_net_metering_availability
(
  sector character varying(3) NOT NULL,
  utility_type character varying(9) NOT NULL,
  nem_system_limit_kw double precision,
  state_abbr character varying(2) NOT NULL,
  CONSTRAINT manual_net_metering_availability_pkey PRIMARY KEY (state_abbr, sector, utility_type)
);


DROP TABLE IF EXISTS diffusion_solar.solar_cost_projections;
CREATE TABLE diffusion_solar.solar_cost_projections
(
	year integer,
	capital_cost_dollars_per_kw numeric,
	inverter_cost_dollars_per_kw numeric,
	fixed_om_dollars_per_kw_per_yr numeric,
	variable_om_dollars_per_kwh numeric,
	sector character varying(3)
);

DROP TABLE IF EXISTS diffusion_solar.learning_rates;
CREATE TABLE diffusion_solar.learning_rates
(
	year integer,
	learning_rate numeric,
	us_frac_of_global_mkt numeric
);
