----------------------------------------
-- A. Create Lookup Table for Hydro Poly
----------------------------------------
-- 1. Add unique integer id (can't use uid because it is text)
alter table diffusion_geo.resources_hydrothermal_poly drop column row_id add row_id serial;

-- 2. Create empty lkup table
drop table if exists diffusion_geo.hydro_poly_lkup;
create table diffusion_geo.hydro_poly_lkup 
	(tract_id_alias integer,
	resource_uid text,
	area_of_intersection_sqkm numeric);
-- 3. Intersect Tracts by Resource and then Resource by Tracts
select parsel_2('dav-gis', 'mmooney', 'mmooney', 
	'diffusion_geo.resources_hydrothermal_poly', --split table
	'row_id', -- splitting unique id
	'select t.tract_id_alias, r.uid,
		round(cast(st_area(st_intersection(r.the_geom_96703, t.the_geom_96703))/1000000 as numeric), 3)
		from diffusion_geo.resources_hydrothermal_poly r, diffusion_blocks.tract_geoms t
		where st_intersects(r.the_geom_96703, t.the_geom_96703)',
	'diffusion_geo.hydro_poly_lkup',
	'r',
	16);

----------------------------------------
-- B. Create Lookup Table for Hydro Poly
----------------------------------------
-- 1. Add unique integer id (can't use uid because it is text)
alter table diffusion_geo.resources_hydrothermal_pt add row_id serial;
-- 2. Create empty lkup table
drop table if exists diffusion_geo.hydro_pt_lkup;
create table diffusion_geo.hydro_pt_lkup 
	(tract_id_alias integer,
	resource_uid text);
-- 3. Intersect Tracts by Resource and then Resource by Tracts
select parsel_2('dav-gis', 'mmooney', 'mmooney', 
	'diffusion_geo.resources_hydrothermal_pt', --split table
	'row_id', -- splitting unique id
	'select b.tract_id_alias, a.uid
	from diffusion_geo.resources_hydrothermal_pt a
	left join diffusion_blocks.tract_geoms b
	on st_intersects(a.the_geom_96703, b.the_geom_96703)',
	'diffusion_geo.hydro_pt_lkup',
	'a',
	10);


----------------------------------------
-- C. Create Lookup Table for EGS
----------------------------------------
-- resource grid table = dgeo.smu_t35km_2016 
-- 1. Create empty lkup table
drop table if exists diffusion_geo.egs_lkup;
create table diffusion_geo.egs_lkup 
	(tract_id_alias integer,
	cell_gid text,
	area_of_intersection_sqkm numeric);
-- 2. Intersect Tracts by Resource and then Resource by Tracts
select parsel_2('dav-gis', 'mmooney', 'mmooney', 
	'dgeo.smu_t35km_2016', --split table
	'gid', -- splitting unique id
	'select b.tract_id_alias, a.gid,
	round(cast(st_area(st_intersection(a.the_geom_96703, b.the_geom_96703))/1000000 as numeric), 3)
	from dgeo.smu_t35km_2016 a
	left join diffusion_blocks.tract_geoms b
	on st_intersects(a.the_geom_96703, b.the_geom_96703)',
	'diffusion_geo.egs_lkup',
	'a',
	10);

----------------------------------------
-- Part 2 -- Run Checks on the Lookup Tables
----------------------------------------
-- hydro poly
with i as (
select count(distinct(tract_id_alias, resource_uid)) as match_cnt from diffusion_geo.hydro_poly_lkup)
select * from i group by match_cnt;
	-- good to go, they are all distnct matches and there are no duplicates of tract_id and uid

--3132 tracts that intersect the UIDs
--3962 uid-tract combos


-- pts -- look for no duplicates in uid
select resource_uid, count(distnct(uid))


-- EGS:
	-- Look for duplicate matches

