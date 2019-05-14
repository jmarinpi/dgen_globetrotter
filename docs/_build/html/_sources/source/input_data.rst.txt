==========
Input Data
==========

A folder of :file:`.csv` files should be copied into :file:`input_scenarios/`, the name of the folder should match the name of the scenario run. An example input scenario folder is within :file:`reference_data/example_data/mex_high_costs/`. The folder contains the following files:

:file:`agent_core_attributes_all.csv`
-------------------------------------
	Core set of attributes to instantiate agents.
	
	Fields:
		:agent_id: unique agent identifier *(float)*
		:control_reg: unique common control region name *(string)*
		:control_reg_id: unique common identifier linked to one control region name *(float)*
		:state: unique common state name *(string)*. Note each control region may have one or more associated states, but each state can only belong to one control region
		:state_id: unique common identifier linked to one state name *(float)*
		:sector_abbr: sector abbreviation *(str)*. Must be one of ['res','com','ind']
		:tariff_class: unique common identifier for the agent's electric tariff type *(str)*
		:country_abbr: common country abbreviation *(str)*
		:customers_in_bin: count of number of DPV customers that the agent represents *(float)*
		:load_in_bin_kwh: total annual energy that the agent represents in kWh *(float)*
		:load_per_customer_in_bin_kwh: per capita annual energy consumption in kWh *(float)* 
		:max_demand_kw: peak agent demand in kW  *(float)* 
		:avg_monthly_kwh: average agent monthly energy use in kWh *(float)* 
		:owner_occupancy_status: code lookup for owner status where 1 = owner-occupied and 2 = leased *(int)*
		:cap_cost_multiplier: agent capital cost multiplier
		:developable_buildings_pct: percent of agent's buildings that are suitable for DPV development  *(float)*
		:bldg_size_class: size of agent's building *(str)*. Must be one of ['small','med','large']
		:rate_id_alias: common electric rate identifier *(int)*. Must match similar field in urdb3_rates.csv

	.. :note:
		'common' fields should have uniform and linkable values across all csv's

:file:`avoided_cost_rates.csv`
------------------------------
	Prices used is assessing value when net metering is activated and the input sheet Net Metering Scenario is specified as 'Avoided Cost'. 

	Fields:
		:country_abbr: common country abbreviation *(str)*
		:control_reg_id: unique common control region identifier *(int)*
		:2014 -> 2050: Avoided cost in US Dollars / kWh *(float)*

:file:`carbon_intensities_grid.csv`
-----------------------------------
	Carbon intensities used when input sheet specifies Carbon Price as 'Price Based On State Carbon Intensity'.

	Fields:
		:country_abbr: common country abbreviation *(str)*
		:control_reg_id: unique common control region identifier *(int)*
		:2014 -> 2050:  Tons of CO2 per kWh of electricity *(float)*

:file:`carbon_intensities_ng.csv`
---------------------------------
	Carbon intensities used when input sheet specifies Carbon Price as 'Price Based On NG Offset'. Values are in US Dollars.
		
	Fields:
		:country_abbr: common country abbreviation *(str)*
		:control_reg_id: unique common control region identifier *(int)*
		:2014 -> 2050: Tons of CO2 per kWh of electricity *(float)*

:file:`load_growth_projections.csv`
-----------------------------------
	Yearly multipliers to use in assessing load growth relative to starting conditions in agent_core_attributes.csv sorted by country and control region. 
		
	Fields:
		:scenario: scenario description that matches input spreadsheet main tab dropdown *(str)*
		:year: scenario year *(int)*
		:sector_abbr: sector abbreviation *(str)*. Must be one of ['res','com','ind']
		:country_abbr: common country abbreviation *(str)*
		:control_reg_id: unique common control region identifier *(int)*
		:load_multiplier: load multiplier percent *(float)*

:file:`max_market_share_settings.csv`
-------------------------------------
	Market curves to use in analysis while will be filtered by the source specified in input sheet sector-specific Market Curve settings.

	Fields:
		:metric: data source name *(str)*. Must be one of ['percent_monthly_bill_savings','payback_period','rate_of_return','net_present_value']
		:metric_value: value of metric *(float)*
		:max_market_share: max DPV share *(float)*
		:source: data source name *(str)*. Must be one of ['RWBeck','NEMS','NREL','Navigant']
		:business_model: business model name *(str)*. Must be one of ['tpo','host_owned']
		:sector_abbr: sector abbreviation *(str)*. Must be one of ['res','com','ind']

:file:`nem_settings.csv`
------------------------
	Net metering projections by year, country and control region.

	Fields:
		:sector_abbr: sector abbreviation *(str)*. Must be one of ['res','com','ind']
		:country_abbr: common country abbreviation *(str)*
		:control_reg_id: unique common control region identifier *(int)*
		:nem_system_size_limit_kw: net metering system size limit kW *(float)*
		:year_end_excess_sell_rate_usd_per_kwh: excess sell rate for net metering in US Dollars *(float)*
		:year: scenario year *(int)*

:file:`normalized_load.csv`
---------------------------
	Normalized load profile by country and control region. Will be scaled by the load specified in agent_core_attributes_all.csv.

	Fields:
		:country_abbr: common country abbreviation *(str)*
		:control_reg_id: unique common control region identifier *(int)*
		:kwh: Array of length 8760 for the fraction of yearly energy consumption in each hour multiplied by a offset factor of 1e9 *(int)*

:file:`pv_bass.csv`
-------------------
	Bass Parameter settings by sector, country and control region.

	Fields:
		:sector_abbr: sector abbreviation *(str)*. Must be one of ['res','com','ind']
		:country_abbr: common country abbreviation *(str)*
		:control_reg_id: unique common control region identifier *(int)*
		:state_id: unique common state identifier *(int)*
		:p: bass diffusion parameter defining the coefficient of innovation.
		:q: bass diffusion parameter defining the coefficient of imitation.
		:teq_yr1: number of years since the diffusion model began.

:file:`pv_state_starting_capacities.csv`
----------------------------------------
	Cumulative DPV penetration by state in 2015.

	Fields:
		:sector_abbr: sector abbreviation *(str)*. Must be one of ['res','com','ind']
		:country_abbr: common country abbreviation *(str)*
		:control_reg_id: unique common control region identifier *(int)*
		:state_id: unique common state identifier *(int)*
		:tariff_class: unique common identifier electric tariff type *(str)*
		:pv_capacity_mw: cumulative DPV capacity in the state in 2015, mW
		:pv_systems_count: cumulative number of DPV systems in the state in 2015

:file:`rate_escalations.csv`
----------------------------
	Yearly multipliers to use in assessing electricity rate growth relative to starting conditions sorted by country and control region.

	Fields:
		:source: data source name *(str)*. Must be one of ['Planning','High','Low']
		:year: scenario year *(int)*
		:sector_abbr: sector abbreviation *(str)*. Must be one of ['res','com','ind']
		:country_abbr: common country abbreviation *(str)*
		:control_reg_id: unique common control region identifier *(int)*
		:escalation_factor: rate escalation percent *(float)*

:file:`solar_resource_hourly.csv`
---------------------------------
	Hourly solar resource profiles by country and control region.

	Fields:
		:country_abbr: common country abbreviation *(str)*
		:control_reg_id: unique common control region identifier *(int)*
		:state_id: unique common state identifier *(int)*
		:cf: Array of length 8760 for the hourly solar insolation *(int)*

:file:`urdb3_rates.csv`
-----------------------
	`Complete urdb rate information <https://openei.org/wiki/Utility_Rate_Database>`_ for agent rates specificed in core_agent_attributes_all.csv.

	Fields:
		:rate_id_alias: rate id matching that of agent_core_attributes_all.csv *(int)*
		:rate_json: complete urdb rate *(json)*

:file:`wholesale_rates.csv`
---------------------------
	Yearly wholesale electricity rates by country and control region. Used in analysis and when input sheet net metering scenario is set to 'State Wholesale'.

	Fields:
		:country_abbr: common country abbreviation *(str)*
		:control_reg_id: unique common control region identifier *(int)*
		:2014 -> 2050: Average wholesale electricity price in US Dollars/kWh *(float)*