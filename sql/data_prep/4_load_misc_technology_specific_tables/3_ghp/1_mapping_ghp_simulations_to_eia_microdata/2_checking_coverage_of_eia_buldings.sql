-- test join for commercial
select -- a.*,
-- 	b.building_type as ornl_building_type,
-- 	c.baseline_heating as ornl_baseline_heating_type,
-- 	d.baseline_cooling as ornl_baseline_cooling_type,
-- 	e.simulation_id
	sum(sample_wt * (kbtu_space_heat + kbtu_space_cool))
from diffusion_shared.cbecs_recs_expanded_combined a
LEFT JOIN diffusion_geo.ornl_building_type_lkup b
	ON a.pba = b.pba
	and a.sector_abbr = b.sector_abbr
LEFT JOIN diffusion_geo.ornl_baseline_heating_lkup c
	ON a.space_heat_equip = c.eia_system_type
	and a.space_heat_fuel = c.eia_fuel_type
	and a.sector_abbr = c.sector_abbr
LEFT JOIN diffusion_geo.ornl_baseline_cooling_lkup d
	ON a.space_cool_equip = d.eia_system_type
	and a.space_cool_fuel = d.eia_fuel_type
	and a.sector_abbr = d.sector_abbr
LEFT JOIN diffusion_geo.ornl_simulations_lkup e
	ON b.building_type = e.building_type
	and c.baseline_heating = e.baseline_heating
	and d.baseline_cooling = e.baseline_cooling
	--and e.provided = True
WHERE a.sector_abbr = 'com'
AND e.simulation_id is not null;
-- 966859170784.26 provided so far, 1722476896213.78 eventually
-- vs
select sum(sample_wt * (kbtu_space_heat + kbtu_space_cool))
from diffusion_shared.cbecs_recs_expanded_combined a
WHERE a.sector_abbr = 'com';
-- 2902688529218.28
select 1722476896213/2902688529218.; -- 59% covered eventually
select 966859170784/2902688529218.; -- 33% covered so far

-- to see what's not covered:
-- with a as
-- (
-- 	select a.*
-- 	from diffusion_shared.cbecs_recs_expanded_combined a
-- 	LEFT JOIN diffusion_geo.ornl_building_type_lkup b
-- 		ON a.pba = b.pba
-- 		and a.sector_abbr = b.sector_abbr
-- 	LEFT JOIN diffusion_geo.ornl_baseline_heating_lkup c
-- 		ON a.space_heat_equip = c.eia_system_type
-- 		and a.space_heat_fuel = c.eia_fuel_type
-- 		and a.sector_abbr = c.sector_abbr
-- 	LEFT JOIN diffusion_geo.ornl_baseline_cooling_lkup d
-- 		ON a.space_cool_equip = d.eia_system_type
-- 		and a.space_cool_fuel = d.eia_fuel_type
-- 		and a.sector_abbr = d.sector_abbr
-- 	LEFT JOIN diffusion_geo.ornl_simulations_lkup e
-- 		ON b.building_type = e.building_type
-- 		and c.baseline_heating = e.baseline_heating
-- 		and d.baseline_cooling = e.baseline_cooling
-- 	WHERE a.sector_abbr = 'com'
-- 	AND e.simulation_id is null
-- )
-- select space_cool_equip, space_cool_fuel,
-- 	space_heat_equip, space_heat_fuel,
-- 	pba,
-- 	round(sum(sample_wt * (kbtu_space_heat + kbtu_space_cool))::NUMERIC, 0) as total_kbtu
-- from a
-- where pba not in (6 ,4, 11)
-- group by space_cool_equip, space_cool_fuel,
-- 	space_heat_equip, space_heat_fuel,
-- 	pba
-- order by total_kbtu desc;

----------------------------------------------------------------------------
-- test join for residential
select -- a.*,
-- 	b.building_type as ornl_building_type,
-- 	c.baseline_heating as ornl_baseline_heating_type,
-- 	d.baseline_cooling as ornl_baseline_cooling_type,
-- 	e.simulation_id
	sum(sample_wt * (kbtu_space_heat + kbtu_space_cool))
from diffusion_shared.cbecs_recs_expanded_combined a
LEFT JOIN diffusion_geo.ornl_building_type_lkup b
	ON a.typehuq = b.typehuq
	and a.sector_abbr = b.sector_abbr
LEFT JOIN diffusion_geo.ornl_baseline_heating_lkup c
	ON a.space_heat_equip = c.eia_system_type
	and a.space_heat_fuel = c.eia_fuel_type
	and a.sector_abbr = c.sector_abbr
LEFT JOIN diffusion_geo.ornl_baseline_cooling_lkup d
	ON a.space_cool_equip = d.eia_system_type
	and a.space_cool_fuel = d.eia_fuel_type
	and a.sector_abbr = c.sector_abbr
LEFT JOIN diffusion_geo.ornl_simulations_lkup e
	ON b.building_type = e.building_type
	and c.baseline_heating = e.baseline_heating
	and d.baseline_cooling = e.baseline_cooling
	and e.provided = True
WHERE a.sector_abbr = 'res'
	and e.simulation_id is not null;
-- 3635994524189.14 eventually, 3320713162463.02 provided so far
-- vs
select  sum(sample_wt * (kbtu_space_heat + kbtu_space_cool))
from diffusion_shared.cbecs_recs_expanded_combined a
WHERE a.sector_abbr = 'res';
-- 4870316830087.82 total
select 3635994524189.14/4870316830087.; -- 75% eventually
select 3320713162463.02/4870316830087.; -- 68% -- so far
-- seems sufficient, but possibly low?