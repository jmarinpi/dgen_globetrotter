This folder contains the following country specific input parameters:
	- agent_core_attributes_all.csv
			Core set of attributes to instantiate agents.
	- avoided_cost_rates.csv
			Prices used is assessing value when net metering is activated and the input sheet Net Metering Scenario is specified as 'Avoided Cost'.
	- carbon_intensities_grid.csv
			Carbon intensities used when input sheet specifies Carbon Price as 'Price Based On State Carbon Intensity'.
	- carbon_intensities_ng.csv
			Carbon intensities used when input sheet specifies Carbon Price as 'Price Based On NG Offset'.
	- load_growth_projections.csv
			Yearly multipliers to use in assessing load growth relative to starting conditions in agent_core_attributes.csv sorted by country and control region.
	- max_market_share_settings.csv
			Market curves to use in analysis while will be filtered by the source specified in input sheet sector-specific Market Curve settings.
	- nem_settings.csv
			Net metering projections by year, country and control region.
	- normalized_load.csv
			Normalized load profile by country and control region. Will be scaled by the load specified in agent_core_attributes_all.csv.
	- pv_bass.csv
			Bass Parameter settings by sector, country and control region.
	- pv_state_starting_capacities.csv
			Cumulative DPV penetration by state in 2015.
	- rate_escalations.csv
			Yearly multipliers to use in assessing electricity rate growth relative to starting conditions sorted by country and control region.
	- solar_hourly_resource.csv
			Hourly solar resource profiles by country and control region.
	- urdb3_rates.csv
			Complete urdb rate information (https://openei.org/wiki/Utility_Rate_Database) for agent rates specificed in core_agent_attributes_all.csv.
	- wholesale_rates.csv
			Yearly wholesale electricity rates by country and control region. Used in analysis and when input sheet net metering scenario is set to 'State Wholesale'.

