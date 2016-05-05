set role 'diffusion-writers';


ALTER TABLE diffusion_data_wind.block_turbine_size_selected
ADD PRIMARY KEY (pgid);

-- create indices on turbine_id, iiijjjcfbin_id, and hub_height_m
CREATE INDEX block_turbine_size_selected_btree_turbine_id
ON diffusion_data_wind.block_turbine_size_selected
USING BTREE(turbine_id);

CREATE INDEX block_turbine_size_selected_btree_iiijjjicf_id
ON diffusion_data_wind.block_turbine_size_selected
USING BTREE(iiijjjicf_id);

CREATE INDEX block_turbine_size_selected_btree_hub_height_m
ON diffusion_data_wind.block_turbine_size_selected
USING BTREE(hub_height_m);

-- join to resource
DROP TABLE IF EXISTS diffusion_data_wind.block_turbine_size_selected_w_aep;
CREATE TABLE diffusion_data_wind.block_turbine_size_selected_w_aep AS
select a.*, c.aep as naep, 
	c.aep * turbine_size_kw as kwh_per_turbine, 
	c.aep * total_capacity_kw as total_generation_kwh
from diffusion_data_wind.block_turbine_size_selected a
LEFT JOIN aws_2014.iii_jjj_cfbin_raster_lookup b
ON a.iiijjjicf_id = b.raster_value
LEFT JOIN diffusion_wind.wind_resource_annual c
ON b.i = c.i
and b.j = c.j
and b.icf/10 = c.cf_bin
AND a.hub_height_m = c.height
and a.turbine_id = c.turbine_id;

-- find the sum 
select sum(bldg_count_all) as systems, sum(total_capacity_kw)/1000/1000 as cap_gw, sum(total_generation_kwh)/1000/1000/1000 as gen_twh -- 335.17233087607
FROM diffusion_data_wind.block_turbine_size_selected_w_aep;
-- 208057,165.032775,335.17233087607
-- compared to http://www.nrel.gov/docs/fy12osti/51946.pdf, these are not totally unreasonable