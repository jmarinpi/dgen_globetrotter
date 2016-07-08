SET ROLE 'diffusion-writers';
DROP TABLE IF EXISTS diffusion_geo.eia_microdata_to_baseline_system_types_lkup;
CREATE TABLE diffusion_geo.eia_microdata_to_baseline_system_types_lkup
(
	sector_abbr varchar(3),
	space_heat_equip text,
	space_heat_fuel	text,
	space_cool_equip text,
	space_cool_fuel text,
	baseline_type text,
	test numeric
);

\COPY diffusion_geo.eia_microdata_to_baseline_system_types_lkup FROM '/Users/mmooney/Desktop/spht_cool_scenario_mapping[2].csv' with csv header;



update diffusion_geo.eia_microdata_to_baseline_system_types_lkup
set baseline_type = NULL where baseline_type = 'NA';

alter table diffusion_geo.eia_microdata_to_baseline_system_types_lkup
drop column test,
alter column baseline_type type integer using (baseline_type::integer);