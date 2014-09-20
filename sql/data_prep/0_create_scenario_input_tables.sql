SET ROLE 'diffusion-writers';

-- main scenarion options table
DROP TABLE IF EXISTS diffusion_solar.scenario_options;

CREATE TABLE diffusion_solar.scenario_options
(
  region text,
  end_year integer,
  markets text,
  cost_assumptions text, -- added
  cust_exp_elec_rates text,
  load_growth_scenario text,
  res_rate_structure text,
  res_rate_escalation text,
  res_max_market_curve text,
  com_rate_structure text,
  com_rate_escalation text,
  com_max_market_curve text,
  com_demand_charge_rate numeric,
  ind_rate_structure text,
  ind_rate_escalation text,
  ind_max_market_curve text,
  ind_demand_charge_rate numeric,
  net_metering_availability text,
  carbon_price text,
  rooftop_availability text,
  system_sizing text,
  random_generator_seed integer,
  ann_inflation numeric,
  ann_system_degradation numeric,
  scenario_name text,
  overwrite_exist_inc boolean,
  incentive_start_year numeric,
  utility_type_iou boolean,
  utility_type_muni boolean,
  utility_type_coop boolean,
  utility_type_allother boolean,
  overwrite_exist_nm boolean
);

-- create config constraint tables for these
-- region
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_region;
CREATE TABLE diffusion_solar_config.sceninp_region
(
  region text,
  CONSTRAINT z_sceninp_region_region_key UNIQUE (region)
);
INSERT INTO diffusion_solar_config.sceninp_region
VALUES
('United States'), ('Alabama'), ('Arizona'), ('Arkansas'), ('California'), ('Colorado'), ('Connecticut'), ('Delaware'), ('Florida'), ('Georgia'), ('Idaho'), ('Illinois'), ('Indiana'), ('Iowa'), ('Kansas'), ('Kentucky'), ('Louisiana'), 
('Maine'), ('Maryland'), ('Massachusetts'), ('Michigan'), ('Minnesota'), ('Mississippi'), ('Missouri'), ('Montana'), ('Nebraska'), ('Nevada'), ('New Hampshire'), ('New Jersey'), ('New Mexico'), ('New York'), ('North Carolina'), ('North Dakota'), ('Ohio'), ('Oklahoma'), ('Oregon'), ('Pennsylvania'),
('Rhode Island'), ('South Carolina'), ('South Dakota'), ('Tennessee'), ('Texas'), ('Utah'), ('Vermont'), ('Virginia'), ('Washington'), ('West Virginia'), ('Wisconsin'), ('Wyoming'), ('District of Columbia');

ALTER TABLE diffusion_solar.scenario_options
  ADD CONSTRAINT scenario_options_region_fkey FOREIGN KEY (region)
      REFERENCES diffusion_solar_config.sceninp_region (region) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;

--   end_year
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_end_year;
CREATE TABLE diffusion_solar_config.sceninp_end_year
(
  end_year integer,
  CONSTRAINT z_sceninp_end_year_end_year_key UNIQUE (end_year)
);
INSERT INTO diffusion_solar_config.sceninp_end_year
SELECT generate_series(2010,2050);

ALTER TABLE diffusion_solar.scenario_options
  ADD CONSTRAINT scenario_options_end_year_fkey FOREIGN KEY (end_year)
      REFERENCES diffusion_solar_config.sceninp_end_year (end_year) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;

--   markets
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_markets;
CREATE TABLE diffusion_solar_config.sceninp_markets
(
  markets text,
  CONSTRAINT z_sceninp_markets_markets_key UNIQUE (markets)
);

INSERT INTO diffusion_solar_config.sceninp_markets
VALUES ('All'),('Only Residential'),('Only Commercial'),('Only Industrial');


ALTER TABLE diffusion_solar.scenario_options
  ADD CONSTRAINT scenario_options_markets_fkey FOREIGN KEY (markets)
      REFERENCES diffusion_solar_config.sceninp_markets (markets) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;

--   cost_assumptions
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_cost_assumptions;
CREATE TABLE diffusion_solar_config.sceninp_cost_assumptions
(
  cost_assumptions text,
  CONSTRAINT z_sceninp_cost_assumption_key UNIQUE (cost_assumptions)
);

INSERT INTO diffusion_solar_config.sceninp_cost_assumptions 
VALUES ('Solar Program Targets'),('AEO 2014'),('User Defined');

ALTER TABLE diffusion_solar.scenario_options
  ADD CONSTRAINT scenario_options_cost_assumption_fkey FOREIGN KEY (cost_assumptions)
      REFERENCES diffusion_solar_config.sceninp_cost_assumptions (cost_assumptions) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;

--   cust_exp_elec_rates
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_cust_exp_elec_rates;
CREATE TABLE diffusion_solar_config.sceninp_cust_exp_elec_rates
(
  cust_exp_elec_rates text,
  CONSTRAINT z_sceninp_cust_exp_elec_rates_cust_exp_elec_rates_key UNIQUE (cust_exp_elec_rates)
);

inSERT INTO diffusion_solar_config.sceninp_cust_exp_elec_rates
VALUES ('AEO2014'),
('No Growth'),
('User Defined');

ALTER TABLE diffusion_solar.scenario_options
  ADD CONSTRAINT scenario_options_cust_exp_elec_rates_fkey FOREIGN KEY (cust_exp_elec_rates)
      REFERENCES diffusion_solar_config.sceninp_cust_exp_elec_rates (cust_exp_elec_rates) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;

--------------------------
--   load_growth_scenario
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_load_growth_scenario;
CREATE TABLE diffusion_solar_config.sceninp_load_growth_scenario
(
  load_growth_scenario text NOT NULL,
  CONSTRAINT sceninp_load_growth_scenario_load_growth_scenario_key UNIQUE (load_growth_scenario)
);

INSERT INTO diffusion_solar_config.sceninp_load_growth_scenario
VALUES
('AEO 2013 No Load growth after 2014'),
('AEO 2013 Low Growth Case'),
('AEO 2013 Reference Case'),
('AEO 2013 High Growth Case'),
('AEO 2013 2x Growth Rate of Reference Case');

ALTER TABLE diffusion_solar.scenario_options
  ADD CONSTRAINT scenario_options_load_growth_scenario_fkey FOREIGN KEY (load_growth_scenario)
      REFERENCES diffusion_solar_config.sceninp_load_growth_scenario (load_growth_scenario) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;

--   res_rate_structure
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_res_rate_structure;
CREATE TABLE diffusion_solar_config.sceninp_res_rate_structure
(
  res_rate_structure text,
  CONSTRAINT z_sceninp_res_rate_structure_res_rate_structure_key UNIQUE (res_rate_structure)
);

INSERT INTO diffusion_solar_config.sceninp_res_rate_structure
VALUES ('BAU');

ALTER TABLE diffusion_solar.scenario_options
  ADD CONSTRAINT scenario_options_res_rate_structure_fkey FOREIGN KEY (res_rate_structure)
      REFERENCES diffusion_solar_config.sceninp_res_rate_structure (res_rate_structure) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;

--   res_rate_escalation
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_res_rate_escalation;
CREATE TABLE diffusion_solar_config.sceninp_res_rate_escalation
(
  res_rate_escalation text,
  CONSTRAINT z_sceninp_res_rate_escalation_res_rate_escalation_key UNIQUE (res_rate_escalation)
);

INSERT INTO diffusion_solar_config.sceninp_res_rate_escalation
VALUES ('AEO2014'),
('No Growth'),
('User Defined');

ALTER TABLE diffusion_solar.scenario_options
  ADD CONSTRAINT scenario_options_res_rate_escalation_fkey FOREIGN KEY (res_rate_escalation)
      REFERENCES diffusion_solar_config.sceninp_res_rate_escalation (res_rate_escalation) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;
      
--   res_max_market_curve
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_res_max_market_curve;
CREATE TABLE diffusion_solar_config.sceninp_res_max_market_curve
(
  res_max_market_curve text,
  CONSTRAINT z_sceninp_res_max_market_curve_res_max_market_curve_key UNIQUE (res_max_market_curve)
);

INSErT INTO diffusion_solar_config.sceninp_res_max_market_curve
VALUES 
('NEMS'),
('Navigant'),
('User Fit'),
('RW Beck');

ALTER TABLE diffusion_solar.scenario_options
  ADD CONSTRAINT scenario_options_res_max_market_curve_fkey FOREIGN KEY (res_max_market_curve)
      REFERENCES diffusion_solar_config.sceninp_res_max_market_curve (res_max_market_curve) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;

--   com_rate_structure
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_com_rate_structure;
CREATE TABLE diffusion_solar_config.sceninp_com_rate_structure
(
  com_rate_structure text,
  CONSTRAINT z_sceninp_com_rate_structure_com_rate_structure_key UNIQUE (com_rate_structure)
);

INSERT INTO diffusion_solar_config.sceninp_com_rate_structure
VALUES ('BAU');

ALTER TABLE diffusion_solar.scenario_options
  ADD CONSTRAINT scenario_options_com_rate_structure_fkey FOREIGN KEY (com_rate_structure)
      REFERENCES diffusion_solar_config.sceninp_com_rate_structure (com_rate_structure) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;

--   com_rate_escalation
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_com_rate_escalation;
CREATE TABLE diffusion_solar_config.sceninp_com_rate_escalation
(
  com_rate_escalation text,
  CONSTRAINT z_sceninp_com_rate_escalation_com_rate_escalation_key UNIQUE (com_rate_escalation)
);

INSERT INTO diffusion_solar_config.sceninp_com_rate_escalation
VALUES 
('AEO2014'),
('No Growth'),
('User Defined');

ALTER TABLE diffusion_solar.scenario_options
  ADD CONSTRAINT scenario_options_com_rate_escalation_fkey FOREIGN KEY (com_rate_escalation)
      REFERENCES diffusion_solar_config.sceninp_com_rate_escalation (com_rate_escalation) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;

--   com_max_market_curve
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_com_max_market_curve;
CREATE TABLE diffusion_solar_config.sceninp_com_max_market_curve
(
  com_max_market_curve text,
  CONSTRAINT z_sceninp_com_max_market_curve_com_max_market_curve_key UNIQUE (com_max_market_curve)
);

INSERT INTO diffusion_solar_config.sceninp_com_max_market_curve
VALUES
('NEMS'),
('Navigant'),
('User Fit'),
('RW Beck');

ALTER TABLE diffusion_solar.scenario_options
  ADD CONSTRAINT scenario_options_com_max_market_curve_fkey FOREIGN KEY (com_max_market_curve)
      REFERENCES diffusion_solar_config.sceninp_com_max_market_curve (com_max_market_curve) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;

--   com_demand_charge_rate
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_com_demand_charge_rates;
CREATE TABLE diffusion_solar_config.sceninp_com_demand_charge_rates
(
  demand_charge_rate numeric NOT NULL,
  CONSTRAINT sceninp_com_demand_charge_rates_pkey PRIMARY KEY (demand_charge_rate)
);

INSERT INTO diffusion_solar_config.sceninp_com_demand_charge_rates
SELECT round(generate_series(0,10)::numeric/10,1);

ALTER TABLE diffusion_solar.scenario_options
  ADD CONSTRAINT scenario_options_com_demand_charge_rates_fkey FOREIGN KEY (com_demand_charge_rate)
      REFERENCES diffusion_solar_config.sceninp_com_demand_charge_rates (demand_charge_rate) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;

--   ind_rate_structure
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_ind_rate_structure;
CREATE TABLE diffusion_solar_config.sceninp_ind_rate_structure
(
  ind_rate_structure text,
  CONSTRAINT sceninp_ind_rate_structure_ind_rate_structure UNIQUE (ind_rate_structure)
);

INSERT INTO diffusion_solar_config.sceninp_ind_rate_structure
VALUES ('BAU');

ALTER TABLE diffusion_solar.scenario_options
  ADD CONSTRAINT scenario_options_ind_rate_structure_fkey FOREIGN KEY (ind_rate_structure)
      REFERENCES diffusion_solar_config.sceninp_ind_rate_structure (ind_rate_structure) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;


--   ind_rate_escalation
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_ind_rate_escalation;
CREATE TABLE diffusion_solar_config.sceninp_ind_rate_escalation
(
  ind_rate_escalation text,
  CONSTRAINT sceninp_ind_rate_escalation_ind_rate_escalation UNIQUE (ind_rate_escalation)
);

INSERT INTO diffusion_solar_config.sceninp_ind_rate_escalation
VALUES 
('AEO2014'),
('No Growth'),
('User Defined');

ALTER TABLE diffusion_solar.scenario_options
  ADD CONSTRAINT scenario_options_ind_rate_escalation_fkey FOREIGN KEY (ind_rate_escalation)
      REFERENCES diffusion_solar_config.sceninp_ind_rate_escalation (ind_rate_escalation) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;

--   ind_max_market_curve
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_ind_max_market_curve;
CREATE TABLE diffusion_solar_config.sceninp_ind_max_market_curve
(
  ind_max_market_curve text,
  CONSTRAINT sceninp_ind_max_market_curve_ind_max_market_curve UNIQUE (ind_max_market_curve)
);

INSERT INTO diffusion_solar_config.sceninp_ind_max_market_curve
VALUES 
('NEMS'),
('Navigant'),
('User Fit'),
('RW Beck');

ALTER TABLE diffusion_solar.scenario_options
  ADD  CONSTRAINT scenario_options_ind_max_market_curve_fkey FOREIGN KEY (ind_max_market_curve)
      REFERENCES diffusion_solar_config.sceninp_ind_max_market_curve (ind_max_market_curve) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;

--   ind_demand_charge_rate
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_ind_demand_charge_rates;
CREATE TABLE diffusion_solar_config.sceninp_ind_demand_charge_rates
(
  demand_charge_rate numeric NOT NULL,
  CONSTRAINT sceninp_ind_demand_charge_rates_pkey PRIMARY KEY (demand_charge_rate)
);

INSERT INTO diffusion_solar_config.sceninp_ind_demand_charge_rates
SELECT round(generate_series(0,10)::numeric/10,1);

ALTER TABLE diffusion_solar.scenario_options
  ADD CONSTRAINT scenario_options_ind_demand_charge_rates_fkey FOREIGN KEY (ind_demand_charge_rate)
      REFERENCES diffusion_solar_config.sceninp_ind_demand_charge_rates (demand_charge_rate) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;

--   net_metering_availability
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_net_metering_availability;
CREATE TABLE diffusion_solar_config.sceninp_net_metering_availability
(
  net_metering_availability text,
  CONSTRAINT z_sceninp_net_metering_availabili_net_metering_availability_key UNIQUE (net_metering_availability)
);

INSERT INTO diffusion_solar_config.sceninp_net_metering_availability
VALUES 
('Partial - Avoided Cost'),
('Partial - No Outflows'),
('No Net Metering Anywhere'),
('Full_Net_Metering_Everywhere'),
('Partial_Avoided_Cost'),
('Partial_No_Outflows'),
('No_Net_Metering_Anywhere');

ALTER TABLE diffusion_solar.scenario_options
  ADD CONSTRAINT scenario_options_net_metering_availability_fkey FOREIGN KEY (net_metering_availability)
      REFERENCES diffusion_solar_config.sceninp_net_metering_availability (net_metering_availability) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;

--   carbon_price
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_carbon_price;
CREATE TABLE diffusion_solar_config.sceninp_carbon_price
(
  carbon_price text,
  CONSTRAINT z_sceninp_carbon_price_carbon_price_key UNIQUE (carbon_price)
);

INSERT INTO diffusion_solar_config.sceninp_carbon_price
VALUeS 
('Price Based On NG Offset'),
('No Carbon Price'),
('Price Based On State Carbon Intensity');

ALTER TABLE diffusion_solar.scenario_options
  ADD CONSTRAINT scenario_options_carbon_price_fkey FOREIGN KEY (carbon_price)
      REFERENCES diffusion_solar_config.sceninp_carbon_price (carbon_price) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;

--   rooftop_availability
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_rooftop_availability;
CREATE TABLE diffusion_solar_config.sceninp_rooftop_availability
(
  rooftop_availability text,
  CONSTRAINT z_sceninp_rooftop_availability_key UNIQUE (rooftop_availability)
);

INSERT INTO diffusion_solar_config.sceninp_rooftop_availability
VALUES
('Option A'),
('Option B');

ALTER TABLE diffusion_solar.scenario_options
  ADD CONSTRAINT scenario_options_rooftop_availability_fkey FOREIGN KEY (rooftop_availability)
      REFERENCES diffusion_solar_config.sceninp_rooftop_availability (rooftop_availability) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;
      
--   system_sizing
DROP TABLE IF EXISTS diffusion_solar_config.sceninp_system_sizing;
CREATE TABLE diffusion_solar_config.sceninp_system_sizing
(
  system_sizing text,
  CONSTRAINT z_sceninp_system_sizing_key UNIQUE (system_sizing)
);

INSERT INTO diffusion_solar_config.sceninp_system_sizing
VALUes
('Option A'),('Option B');

ALTER TABLE diffusion_solar.scenario_options
  ADD CONSTRAINT scenario_options_system_sizing_fkey FOREIGN KEY (system_sizing)
      REFERENCES diffusion_solar_config.sceninp_system_sizing (system_sizing) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;

--   random_generator_seed
-- don't need one for this: only constraint is that random_generator_seed must be an integer

--   ann_inflation
-- no constraint here

--   scenario_name
-- no constraint

--   overwrite_exist_inc 
-- no constraint -- must be boolean

--   incentive_start_year
-- no constraint

--   utility_type_iou
-- no constraint -- must be boolean

--   utility_type_muni
-- no constraint -- must be boolean

--   utility_type_coop
-- no constraint -- must be boolean

--   utility_type_allother
-- no constraint -- must be boolean

--   overwrite_exist_nm
-- no constraint -- must be boolean
