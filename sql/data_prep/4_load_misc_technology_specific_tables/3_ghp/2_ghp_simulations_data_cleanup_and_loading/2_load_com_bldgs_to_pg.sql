set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_geo.ghp_simulations_com;
CREATE TABLE diffusion_geo.ghp_simulations_com
(
	building_type TEXT,
	gtc_btu_per_hftF NUMERIC,
	baseline_electricity_consumption_kwh NUMERIC,
	baseline_natural_gas_consumption_mbtu NUMERIC,
	baseline_site_energy_mbtu NUMERIC,
	baseline_source_energy_mbtu NUMERIC,
	baseline_carbon_emissions_mt NUMERIC,
	baseline_energy_cost NUMERIC,
	baseline_peak_electricity_demand_kw NUMERIC,
	gshp_electricity_consumption_kwh NUMERIC,
	gshp_natural_gas_consumption_mbtu NUMERIC,
	gshp_site_energy_mbtu NUMERIC,
	gshp_source_energy_mbtu NUMERIC,
	gshp_carbon_emissions_mt NUMERIC,
	gshp_energy_cost NUMERIC,
	gshp_peak_electricity_demand_kw NUMERIC,
	savings_abs_electricity_consumption_kwh NUMERIC,
	savings_abs_natural_gas_consumption_mbtu NUMERIC,
	savings_abs_site_energy_mbtu NUMERIC,
	savings_abs_source_energy_mbtu NUMERIC,
	savings_abs_carbon_emissions_mt NUMERIC,
	savings_abs_energy_cost NUMERIC,
	savings_abs_peak_electricity_demand_kw NUMERIC,
	savings_pct_electricity_consumption_kwh NUMERIC,
	savings_pct_natural_gas_consumption_mbtu NUMERIC,
	savings_pct_site_energy_mbtu NUMERIC,
	savings_pct_source_energy_mbtu NUMERIC,
	savings_pct_carbon_emissions_mt NUMERIC,
	savings_pct_energy_cost NUMERIC,
	savings_pct_peak_electricity_demand_kw NUMERIC,
	total_ghx_length NUMERIC,
	cooling_capacity_ton NUMERIC,
	length_per_ton_of_capacity_ft_ton NUMERIC,
	max_lft_f NUMERIC,
	min_lft_f NUMERIC,
	city TEXT,
	climate_zone TEXT
);

\COPY diffusion_geo.ghp_simulations_com FROM '/Users/mgleason/NREL_Projects/github/diffusion/sql/data_prep/4_load_misc_technology_specific_tables/3_ghp/2_ghp_simulations_data_cleanup_and_loading/output/ghp_results_2016_07_20.csv' with csv header;

-- add primary key on building type, gtc, and city
ALTER TABLE diffusion_geo.ghp_simulations_com
ADD PRIMARY KEY (building_type, city, gtc_btu_per_hftF);

-- fill in the pct savings values
UPDATE diffusion_geo.ghp_simulations_com
set savings_pct_electricity_consumption_kwh =
(baseline_electricity_consumption_kwh - gshp_electricity_consumption_kwh)/
baseline_electricity_consumption_kwh;

UPDATE diffusion_geo.ghp_simulations_com
set savings_pct_natural_gas_consumption_mbtu =
COALESCE((baseline_natural_gas_consumption_mbtu - gshp_natural_gas_consumption_mbtu)/
NULLIF(baseline_natural_gas_consumption_mbtu, 0), 0);

UPDATE diffusion_geo.ghp_simulations_com
set savings_pct_site_energy_mbtu =
(baseline_site_energy_mbtu - gshp_site_energy_mbtu)/
baseline_site_energy_mbtu;

UPDATE diffusion_geo.ghp_simulations_com
set savings_pct_source_energy_mbtu =
(baseline_source_energy_mbtu - gshp_source_energy_mbtu)/
baseline_source_energy_mbtu;

UPDATE diffusion_geo.ghp_simulations_com
set savings_pct_carbon_emissions_mt =
(baseline_carbon_emissions_mt - gshp_carbon_emissions_mt)/
baseline_carbon_emissions_mt;

UPDATE diffusion_geo.ghp_simulations_com
set savings_pct_energy_cost =
(baseline_energy_cost - gshp_energy_cost)/
baseline_energy_cost;
	
UPDATE diffusion_geo.ghp_simulations_com
set savings_pct_peak_electricity_demand_kw =
(baseline_peak_electricity_demand_kw - gshp_peak_electricity_demand_kw)/
baseline_peak_electricity_demand_kw;

-- look at results
select *
FROM diffusion_geo.ghp_simulations_com;