set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_geo.ornl_simulations_lkup;
CREATE TABLE diffusion_geo.ornl_simulations_lkup
(
	simulation_id integer primary key,
	building_type text,
	baseline_cooling text,
	baseline_heating text,
	provided boolean
);

\COPY diffusion_geo.ornl_simulations_lkup FROM '/Volumes/Staff/mgleason/dGeo/Data/Source_Data/ORNL_GHP_CRB_Simulations/lkup_tables/csvs/crb_descriptions.csv' with csv header;

----------------------------------------------------------------------------

DROP TABLE IF EXISTS diffusion_geo.ornl_building_type_lkup;
CREATE TABLE diffusion_geo.ornl_building_type_lkup
(
	building_type text,
	sector_abbr varchar(3),
	pba integer,
	pba_desc text,
	typehuq integer,
	typehuq_desc text
);

\COPY diffusion_geo.ornl_building_type_lkup FROM '/Volumes/Staff/mgleason/dGeo/Data/Source_Data/ORNL_GHP_CRB_Simulations/lkup_tables/csvs/building_type_to_pba_lkup.csv' with csv header;

----------------------------------------------------------------------------

DROP TABLE IF EXISTS diffusion_geo.ornl_baseline_heating_lkup;
CREATE TABLE diffusion_geo.ornl_baseline_heating_lkup
(
	sector_abbr varchar(3),
	baseline_heating text,
	analog_type text,
	eia_system_type text,
	eia_fuel_type text
);


\COPY diffusion_geo.ornl_baseline_heating_lkup FROM '/Volumes/Staff/mgleason/dGeo/Data/Source_Data/ORNL_GHP_CRB_Simulations/lkup_tables/csvs/baseline_heating_system_lkup.csv' with csv header;

----------------------------------------------------------------------------

DROP TABLE IF EXISTS diffusion_geo.ornl_baseline_cooling_lkup;
CREATE TABLE diffusion_geo.ornl_baseline_cooling_lkup
(
	sector_abbr varchar(3),
	baseline_cooling text,
	analog_type text,
	eia_system_type text,
	eia_fuel_type text
);

\COPY diffusion_geo.ornl_baseline_cooling_lkup FROM '/Volumes/Staff/mgleason/dGeo/Data/Source_Data/ORNL_GHP_CRB_Simulations/lkup_tables/csvs/baseline_cooling_system_lkup.csv' with csv header;

----------------------------------------------------------------------------
-- test join for commercial
select -- a.*,
-- 	b.building_type as ornl_building_type,
-- 	c.baseline_heating as ornl_baseline_heating_type,
-- 	d.baseline_cooling as ornl_baseline_cooling_type,
-- 	e.simulation_id
	sum(sample_wt),
	sum(sample_wt * (kbtu_space_heat + kbtu_space_cool))
from diffusion_shared.cbecs_recs_expanded_combined a
LEFT JOIN diffusion_geo.ornl_building_type_lkup b
ON a.pba = b.pba
LEFT JOIN diffusion_geo.ornl_baseline_heating_lkup c
ON a.space_heat_equip = c.eia_system_type
and a.space_heat_fuel = c.eia_fuel_type
LEFT JOIN diffusion_geo.ornl_baseline_cooling_lkup d
ON a.space_cool_equip = d.eia_system_type
and a.space_cool_fuel = d.eia_fuel_type
LEFT JOIN diffusion_geo.ornl_simulations_lkup e
ON b.building_type = e.building_type
and c.baseline_heating = e.baseline_heating
and d.baseline_cooling = e.baseline_cooling
and e.provided = True
WHERE a.sector_abbr = 'com'
AND e.simulation_id is not null;
-- 1519237.44 matched, 694821061111.669 kbtu
-- vs
select sum(sample_wt),
       sum(sample_wt * (kbtu_space_heat + kbtu_space_cool))
from diffusion_shared.cbecs_recs_expanded_combined a
WHERE a.sector_abbr = 'com';
-- 4776510.51000004 total, 2902688529218.28 kbtu
select 1519237.44/4776510.51000004; -- = 31%
select 694821061111.669/2902688529218.28; -- 23%

----------------------------------------------------------------------------
-- test join for residential
select -- a.*,
-- 	b.building_type as ornl_building_type,
-- 	c.baseline_heating as ornl_baseline_heating_type,
-- 	d.baseline_cooling as ornl_baseline_cooling_type,
-- 	e.simulation_id
	sum(sample_wt),
	sum(sample_wt * (kbtu_space_heat + kbtu_space_cool))
from diffusion_shared.cbecs_recs_expanded_combined a
LEFT JOIN diffusion_geo.ornl_building_type_lkup b
ON a.typehuq = b.typehuq
LEFT JOIN diffusion_geo.ornl_baseline_heating_lkup c
ON a.space_heat_equip = c.eia_system_type
and a.space_heat_fuel = c.eia_fuel_type
LEFT JOIN diffusion_geo.ornl_baseline_cooling_lkup d
ON a.space_cool_equip = d.eia_system_type
and a.space_cool_fuel = d.eia_fuel_type
LEFT JOIN diffusion_geo.ornl_simulations_lkup e
ON b.building_type = e.building_type
and c.baseline_heating = e.baseline_heating
and d.baseline_cooling = e.baseline_cooling
and e.provided = True
WHERE a.sector_abbr = 'res'
and e.simulation_id is not null;
-- 46679602.2974764, 3320713162463.02 kbtu
-- vs
select sum(sample_wt),
	sum(sample_wt * (kbtu_space_heat + kbtu_space_cool))
from diffusion_shared.cbecs_recs_expanded_combined a
WHERE a.sector_abbr = 'res';
-- 90253484.7116147 total, -- 4870316830087.82 kbtu
select 46679602.2974764/90253484.7116147; -- 51%
select 3320713162463.02/4870316830087.82; -- 68%