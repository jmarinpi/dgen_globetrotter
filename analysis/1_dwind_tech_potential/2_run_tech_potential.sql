set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_data_wind.tech_pot_block_turbine_size_options;
CREATE TABLE diffusion_data_wind.tech_pot_block_turbine_size_options AS
WITH blocks as
(
 select a.pgid,
              a.state_abbr, a.state_fips, a.county_fips,
              a.aland_sqm/1000/1000 as aland_sqkm,
              --a.the_poly_96703, a.the_point_96703,
              c.acres_per_bldg,
              d.canopy_pct,
              e.canopy_ht_m,
              f.bldg_count_all,
              g.iiijjjicf_id,
              CASE WHEN d.canopy_pct >= h.canopy_pct_requiring_clearance * 100 THEN e.canopy_ht_m + h.canopy_clearance_static_adder_m
		   ELSE 0
	       END as min_allowable_blade_height_m,
	      CASE WHEN c.acres_per_bldg <= h.required_parcel_size_cap_acres THEN sqrt(c.acres_per_bldg * 4046.86)/(2 * h.blade_height_setback_factor)
	      ELSE 'Infinity'::double precision
	       end as max_allowable_blade_height_m
        FROM  diffusion_blocks.block_geoms a
        INNER JOIN diffusion_blocks.blocks_with_buildings b
        ON a.pgid = b.pgid
        LEFT JOIN diffusion_blocks.block_parcel_size c
        ON a.pgid = c.pgid
        LEFT JOIN diffusion_blocks.block_canopy_cover d
        on a.pgid = d.pgid
        LEFT JOIN diffusion_blocks.block_canopy_height e
        on a.pgid = e.pgid
        LEFT JOIN diffusion_blocks.block_bldg_counts f
        on a.pgid = f.pgid
        LEFT JOIN diffusion_blocks.block_resource_id_wind g
        ON a.pgid = g.pgid
        CROSS JOIN diffusion_data_wind.tech_pot_settings h
        where a.state_abbr not in ('AK', 'HI')
),
turbine_sizes as
(
	select a.*, 
		a.hub_height_m - a.rotor_radius_m * b.canopy_clearance_rotor_factor as effective_min_blade_height_m,
		a.hub_height_m + a.rotor_radius_m as effective_max_blade_height_m
	from diffusion_data_wind.tech_pot_turbine_sizes a
	CROSS JOIN diffusion_data_wind.tech_pot_settings b
)
select a.*, 
	b.*, 
	b.turbine_size_kw * a.bldg_count_all as total_capacity_kw
from blocks a
inner JOIN turbine_sizes b
	ON b.effective_min_blade_height_m >= a.min_allowable_blade_height_m 
	AND b.effective_max_blade_height_m <= a.max_allowable_blade_height_m;
-- runs in 348 seconds (~6 mins)

-- create indices on join columns
CREATE INDEX tech_pot_block_turbine_size_options_btree_iiijjjicf_id
ON  diffusion_data_wind.tech_pot_block_turbine_size_options
USING BTREE(iiijjjicf_id);

CREATE INDEX tech_pot_block_turbine_size_options_btree_hub_height_m
ON  diffusion_data_wind.tech_pot_block_turbine_size_options
USING BTREE(hub_height_m);

CREATE INDEX tech_pot_block_turbine_size_options_btree_turbine_id
ON  diffusion_data_wind.tech_pot_block_turbine_size_options
USING BTREE(turbine_id);

-- join to resource and select the max resource available
DROP TABLE IF EXISTS diffusion_data_wind.tech_pot_block_turbine_size_selected;
CREATE TABLE diffusion_data_wind.tech_pot_block_turbine_size_selected AS
with a as
(
	select a.*, 	
		c.aep as naep,
		c.aep * a.turbine_size_kw as kwh_per_turbine,
		c.aep * a.total_capacity_kw as total_generation_kwh,
		a.bldg_count_all as systems_count,
		a.total_capacity_kw/1000/a.aland_sqkm as power_density_mw_per_sq_km
	FROM diffusion_data_wind.tech_pot_block_turbine_size_options a
	LEFT JOIN aws_2014.iii_jjj_cfbin_raster_lookup b
		ON a.iiijjjicf_id = b.raster_value
	LEFT JOIN diffusion_wind.wind_resource_annual c
		ON b.i = c.i
		and b.j = c.j
		and b.icf/10 = c.cf_bin
		AND a.hub_height_m = c.height
		and a.turbine_id = c.turbine_id
)
select distinct on (a.pgid) a.*
from a
order by a.pgid, kwh_per_turbine desc;
-- order by a.pgid, hub_height_m desc, turbine_size_kw desc;
-- takes about 18 mins to run

-- back up the table
DROP TABLE IF EXISTS diffusion_data_wind.tech_pot_block_turbine_size_selected_backup;
CREATE TABLE diffusion_data_wind.tech_pot_block_turbine_size_selected_backup AS
SELECT *
FROM diffusion_data_wind.tech_pot_block_turbine_size_selected;

-- cap blocks to 3 MW where power_density exceed 3 MW / sq m (consistent with Wind Vision)
UPDATE diffusion_data_wind.tech_pot_block_turbine_size_selected
SET systems_count = round(3000./turbine_size_kw::NUMERIC, 0)::INTEGER
where power_density_mw_per_sq_km > 3;
-- 1698129 rows

UPDATE diffusion_data_wind.tech_pot_block_turbine_size_selected
SET (total_capacity_kw, total_generation_kwh) = (systems_count * turbine_size_kw, systems_count * turbine_size_kw * naep)
where power_density_mw_per_sq_km > 3;
-- 1698129 rows

-- summarize results
select sum(systems_count) as systems, sum(total_capacity_kw)/1000/1000 as cap_gw, sum(total_generation_kwh)/1000/1000/1000 as gen_twh
from diffusion_data_wind.tech_pot_block_turbine_size_selected;
-- 49,500,751 systems
-- 8128 GW
-- 18,691 TWH

-- this is less than utility scale onshore wind Lopez et al. (http://www.nrel.gov/docs/fy12osti/51946.pdf):
-- 11,000 GW
-- 32,700 TWH

-- and wind vision:
-- 10,640 GW
-- 30,950 TWH

