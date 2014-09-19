-- main scenarion options table
DROP TABLE diffusion_solar.scenario_options;

CREATE TABLE diffusion_solar.scenario_options
(
  region text,
  end_year integer,
  markets text,
  cost_assumptions text, -- changed
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


  CONSTRAINT scenario_options_carbon_price_fkey FOREIGN KEY (carbon_price)
      REFERENCES diffusion_wind_config.sceninp_carbon_price (carbon_price) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_com_demand_charge_rates_fkey FOREIGN KEY (com_demand_charge_rate)
      REFERENCES diffusion_wind_config.sceninp_com_demand_charge_rates (demand_charge_rate) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_com_max_market_curve_fkey FOREIGN KEY (com_max_market_curve)
      REFERENCES diffusion_wind_config.sceninp_com_max_market_curve (com_max_market_curve) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_com_rate_escalation_fkey FOREIGN KEY (com_rate_escalation)
      REFERENCES diffusion_wind_config.sceninp_com_rate_escalation (com_rate_escalation) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_com_rate_structure_fkey FOREIGN KEY (com_rate_structure)
      REFERENCES diffusion_wind_config.sceninp_com_rate_structure (com_rate_structure) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_cust_exp_elec_rates_fkey FOREIGN KEY (cust_exp_elec_rates)
      REFERENCES diffusion_wind_config.sceninp_cust_exp_elec_rates (cust_exp_elec_rates) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_end_year_fkey FOREIGN KEY (end_year)
      REFERENCES diffusion_wind_config.sceninp_end_year (end_year) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_height_exclusion_fkey FOREIGN KEY (height_exclusions)
      REFERENCES diffusion_wind_config.sceninp_height_exclusions (height_exclusions) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_incentive_start_year_fkey FOREIGN KEY (incentive_start_year)
      REFERENCES diffusion_wind_config.sceninp_incentive_start_year (incentive_start_year) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_ind_demand_charge_rates_fkey FOREIGN KEY (ind_demand_charge_rate)
      REFERENCES diffusion_wind_config.sceninp_ind_demand_charge_rates (demand_charge_rate) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_ind_max_market_curve_fkey FOREIGN KEY (ind_max_market_curve)
      REFERENCES diffusion_wind_config.sceninp_ind_max_market_curve (ind_max_market_curve) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_ind_rate_escalation_fkey FOREIGN KEY (ind_rate_escalation)
      REFERENCES diffusion_wind_config.sceninp_ind_rate_escalation (ind_rate_escalation) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_ind_rate_structure_fkey FOREIGN KEY (ind_rate_structure)
      REFERENCES diffusion_wind_config.sceninp_ind_rate_structure (ind_rate_structure) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_load_growth_scenario_fkey FOREIGN KEY (load_growth_scenario)
      REFERENCES diffusion_wind_config.sceninp_load_growth_scenario (load_growth_scenario) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_markets_fkey FOREIGN KEY (markets)
      REFERENCES diffusion_wind_config.sceninp_markets (markets) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_net_metering_availability_fkey FOREIGN KEY (net_metering_availability)
      REFERENCES diffusion_wind_config.sceninp_net_metering_availability (net_metering_availability) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_overwrite_exist_inc_fkey FOREIGN KEY (overwrite_exist_inc)
      REFERENCES diffusion_wind_config.sceninp_overwrite_exist_inc (overwrite_exist_inc) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_region_fkey FOREIGN KEY (region)
      REFERENCES diffusion_wind_config.sceninp_region (region) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_res_max_market_curve_fkey FOREIGN KEY (res_max_market_curve)
      REFERENCES diffusion_wind_config.sceninp_res_max_market_curve (res_max_market_curve) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_res_rate_escalation_fkey FOREIGN KEY (res_rate_escalation)
      REFERENCES diffusion_wind_config.sceninp_res_rate_escalation (res_rate_escalation) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_res_rate_structure_fkey FOREIGN KEY (res_rate_structure)
      REFERENCES diffusion_wind_config.sceninp_res_rate_structure (res_rate_structure) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_utility_type_allother_fkey FOREIGN KEY (utility_type_allother)
      REFERENCES diffusion_wind_config.sceninp_utility_type_allother (utility_type_allother) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_utility_type_coop_fkey FOREIGN KEY (utility_type_coop)
      REFERENCES diffusion_wind_config.sceninp_utility_type_coop (utility_type_coop) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_utility_type_iou_fkey FOREIGN KEY (utility_type_iou)
      REFERENCES diffusion_wind_config.sceninp_utility_type_iou (utility_type_iou) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT scenario_options_utility_type_muni_fkey FOREIGN KEY (utility_type_muni)
      REFERENCES diffusion_wind_config.sceninp_utility_type_muni (utility_type_muni) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT
)