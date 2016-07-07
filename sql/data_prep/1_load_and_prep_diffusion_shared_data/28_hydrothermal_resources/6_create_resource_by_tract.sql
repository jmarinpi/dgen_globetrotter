------------------------------------------------------------
-- A. Create Tract Resource Tables (from Lookup) -- POLYS
------------------------------------------------------------
drop table if exists diffusion_geo.hydro_poly_tracts;
create table diffusion_geo.hydro_poly_tracts as 
	(
		select 
		a.tract_id_alias, 
		a.resource_uid, 
		'hydrothermal'::TEXT as resource_type,
		b.sys_type,
		b.min_depth_m, 
		b.max_depth_m, 
		b.res_temp_deg_c, 
		a.area_of_intersection_sqkm as area_of_res_in_tract_sqkm,
		b.res_thickness_km,
		b.area_per_well_km2 as area_per_well_sqkm,
		(a.area_of_intersection_sqkm/b.area_per_well_km2) as n_wells_in_tract,
		diffusion_geo.extractable_resource_joules_production_plan((a.area_of_intersection_sqkm/b.area_per_well_km2), b.res_temp_deg_c)/3600000000 as extractable_resource_in_tract_mwh,
		b.mean_resource_1e18_joules/3600000000 as extractable_resource_per_well_in_tract_mwh
		from diffusion_geo.hydro_poly_lkup a
		left join diffusion_geo.resources_hydrothermal_poly b
		on a.resource_uid = b.uid
	);

-- ******************-- ******************-- ******************
-- * NOte: I calcualted reservoir thickenss by km (not m) to be consistent with the xlsx
-- ******************-- ******************-- ******************

------------------------------------------------------------
-- B. Create Tract Resource Tables (from Lookup) -- POINTS
------------------------------------------------------------
drop table if exists diffusion_geo.hydro_pt_tracts;
create table diffusion_geo.hydro_pt_tracts as 
	(
		select 
		a.tract_id_alias, 
		a.resource_uid, 
		'hydrothermal'::TEXT as resource_type,
		b.sys_type,
		b.min_depth_m, 
		b.max_depth_m, 
		b.res_temp_deg_c, 
		b.res_vol_km3,
		1::NUMERIC as n_wells_in_tract,
		b.mean_resource_1e18_joules/3600000000 as extractable_resource_in_tract_mwh,
		b.mean_resource_1e18_joules/3600000000 as extractable_resource_per_well_in_tract_mwh
		from diffusion_geo.hydro_pt_lkup a
		left join diffusion_geo.resources_hydrothermal_pt b
		on a.resource_uid = b.uid
	);

-- ******************-- ******************-- ******************
-- ** Note: "reservoir_temp_deg_c" was listed 2x in your notes. is this a typo?"
-- 2. For points, extractable_resource_in_tract_mwh and extractable_resource_per_well_in_tract_mwh
	-- are the same exact value. Is this correct?
-- ******************-- ******************-- ******************


-- ******************-- ******************
-- TODO -- Create lkup for EGS
-- ******************-- ******************