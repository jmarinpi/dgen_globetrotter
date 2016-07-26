set role 'diffusion-writers';

-- create a dummy table for now that we can use to link to agents
-- until we get better guidance from ORNL on how to link from simulations
-- to cbecs/recs
DROP VIEW IF EXISTS diffusion_geo.ghp_simulations_dummy;
CREATE VIEW diffusion_geo.ghp_simulations_dummy AS
select b.crb_model,
	a.climate_zone as iecc_climate_zone, 
	a.gtc_btu_per_hftf, 
	a.savings_pct_electricity_consumption_kwh as savings_pct_electricity_consumption, 
	a.savings_pct_natural_gas_consumption_mbtu as savings_pct_natural_gas_consumption, 
	a.total_ghx_length as crb_ghx_length_ft, 
	a.cooling_capacity_ton as crb_cooling_capacity_ton, 
	a.tot_sqft as crb_totsqft,
	a.cooling_capacity_ton/a.tot_sqft as cooling_ton_per_sqft,
	a.total_ghx_length/a.cooling_capacity_ton as ghx_length_ft_per_cooling_ton
from diffusion_geo.ghp_simulations_com a
cross join diffusion_geo.crb_building_names_and_sizes b
where a.crb_model = 'secondary_school';
