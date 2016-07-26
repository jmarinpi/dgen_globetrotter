set role 'diffusion-writers';

-- need to create a differetn version of the tc by cliamte zone summary because xiaobing
-- used different values in simulations than i did
-- the real table we SHOULD be using is diffusion_geo.thermal_conductivity_summary_by_climate_zone

DROP TABLE IF EXISTS diffusion_geo.thermal_conductivity_summary_by_climate_zone_ornl;
CREATE TABLE diffusion_geo.thermal_conductivity_summary_by_climate_zone_ornl AS
with a AS
(
	select distinct climate_zone, gtc_btu_per_hftf
	from diffusion_geo.ghp_simulations_com
),
b as
(
	select climate_zone, array_agg(gtc_btu_per_hftf) as gtc_vals
	from a
	group by climate_zone
)
select climate_zone, 
	gtc_vals[1] as q25, 
	gtc_vals[2] as q50, 
	gtc_vals[3] as q75
from b;
