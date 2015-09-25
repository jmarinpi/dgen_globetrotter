-- find all rates within 50 mi of each point with the same utility_type

-- COMMERCIAL
DROP TABLE IF EXISTS diffusion_data_shared.pt_ranked_lidar_city_lkup_com;
CREATE TABLE diffusion_data_shared.pt_ranked_lidar_city_lkup_com
(
	pt_gid integer,
	city_id integer,
	rank integer
);

-- use 96703 geoms even thoguh they aren't equidistant
-- because geography calcs would be too slow
SELECT parsel_2('dav-gis','mgleason','mgleason',
		'diffusion_shared.pt_grid_us_com','gid',
		'with a as
		(
			SELECT a.gid as pt_gid, 
				d.city_id, d.year,
				ST_Distance(a.the_geom_96703, d.rasd_the_geom_96703) as distance_m
			FROM diffusion_shared.pt_grid_us_com a
			LEFT JOIN diffusion_shared.county_geom b
				ON a.county_id = b.county_id
			INNER JOIN pv_rooftop_dsolar_integration.city_ulocale_zone_lkup c
				ON a.ulocale = c.ulocale
				and c.zone = ''com_ind''
			INNER JOIN pv_rooftop_dsolar_integration.solar_gid d
				ON c.city_id = d.city_id
				AND b.census_region = d.census_region
		)
		SELECT pt_gid,  city_id,
			rank() OVER (partition by pt_gid ORDER BY distance_m ASC, year DESC) as rank
		FROM a;',
		'diffusion_data_shared.pt_ranked_lidar_city_lkup_com', 'a', 16);


-- add indices
CREATE INDEX pt_ranked_lidar_city_lkup_com_pt_gid_btree
ON diffusion_data_shared.pt_ranked_lidar_city_lkup_com
using btree(pt_gid);

CREATE INDEX pt_ranked_lidar_city_lkup_com_rank_btree
ON diffusion_data_shared.pt_ranked_lidar_city_lkup_com
using btree(rank);

CREATE INDEX pt_ranked_lidar_city_lkup_com_city_id_alias_btree
ON diffusion_data_shared.pt_ranked_lidar_city_lkup_com
using btree(city_id);

select count(*)
FROM diffusion_data_shared.pt_ranked_lidar_city_lkup_com;
-- 56,567,275
------------------------------------------------------------------------------

-- INDUSTRIAL
DROP TABLE IF EXISTS diffusion_data_shared.pt_ranked_lidar_city_lkup_ind;
CREATE TABLE diffusion_data_shared.pt_ranked_lidar_city_lkup_ind
(
	pt_gid integer,
	city_id integer,
	rank integer
);

-- use 96703 geoms even thoguh they aren't equidistant
-- because geography calcs would be too slow
SELECT parsel_2('dav-gis','mgleason','mgleason',
		'diffusion_shared.pt_grid_us_ind','gid',
		'with a as
		(
			SELECT a.gid as pt_gid, 
				d.city_id, d.year,
				ST_Distance(a.the_geom_96703, d.rasd_the_geom_96703) as distance_m
			FROM diffusion_shared.pt_grid_us_ind a
			LEFT JOIN diffusion_shared.county_geom b
				ON a.county_id = b.county_id
			INNER JOIN pv_rooftop_dsolar_integration.city_ulocale_zone_lkup c
				ON a.ulocale = c.ulocale
				and c.zone = ''com_ind''
			INNER JOIN pv_rooftop_dsolar_integration.solar_gid d
				ON c.city_id = d.city_id
				AND b.census_region = d.census_region
		)
		SELECT pt_gid,  city_id,
			rank() OVER (partition by pt_gid ORDER BY distance_m ASC, year DESC) as rank
		FROM a;',
		'diffusion_data_shared.pt_ranked_lidar_city_lkup_ind', 'a', 16);


-- add indices
CREATE INDEX pt_ranked_lidar_city_lkup_ind_pt_gid_btree
ON diffusion_data_shared.pt_ranked_lidar_city_lkup_ind
using btree(pt_gid);

CREATE INDEX pt_ranked_lidar_city_lkup_ind_rank_btree
ON diffusion_data_shared.pt_ranked_lidar_city_lkup_ind
using btree(rank);

CREATE INDEX pt_ranked_lidar_city_lkup_ind_city_id_alias_btree
ON diffusion_data_shared.pt_ranked_lidar_city_lkup_ind
using btree(city_id);

select count(*)
FROM diffusion_data_shared.pt_ranked_lidar_city_lkup_ind;
-- 56,567,275
------------------------------------------------------------------------------

-- RESIDENTIAL
DROP TABLE IF EXISTS diffusion_data_shared.pt_ranked_lidar_city_lkup_res;
CREATE TABLE diffusion_data_shared.pt_ranked_lidar_city_lkup_res
(
	pt_gid integer,
	city_id integer,
	rank integer
);

-- use 96703 geoms even thoguh they aren't equidistant
-- because geography calcs would be too slow
SELECT parsel_2('dav-gis','mgleason','mgleason',
		'diffusion_shared.pt_grid_us_res','gid',
		'with a as
		(
			SELECT a.gid as pt_gid, 
				d.city_id, d.year,
				ST_Distance(a.the_geom_96703, d.rasd_the_geom_96703) as distance_m
			FROM diffusion_shared.pt_grid_us_res a
			LEFT JOIN diffusion_shared.county_geom b
				ON a.county_id = b.county_id
			INNER JOIN pv_rooftop_dsolar_integration.city_ulocale_zone_lkup c
				ON a.ulocale = c.ulocale
				and c.zone = ''residential''
			INNER JOIN pv_rooftop_dsolar_integration.solar_gid d
				ON c.city_id = d.city_id
				AND b.census_region = d.census_region
		)
		SELECT pt_gid,  city_id,
			rank() OVER (partition by pt_gid ORDER BY distance_m ASC, year DESC) as rank
		FROM a;',
		'diffusion_data_shared.pt_ranked_lidar_city_lkup_res', 'a', 16);


-- add indices
CREATE INDEX pt_ranked_lidar_city_lkup_res_pt_gid_btree
ON diffusion_data_shared.pt_ranked_lidar_city_lkup_res
using btree(pt_gid);

CREATE INDEX pt_ranked_lidar_city_lkup_res_rank_btree
ON diffusion_data_shared.pt_ranked_lidar_city_lkup_res
using btree(rank);

CREATE INDEX pt_ranked_lidar_city_lkup_res_city_id_alias_btree
ON diffusion_data_shared.pt_ranked_lidar_city_lkup_res
using btree(city_id);

select count(*)
FROM diffusion_data_shared.pt_ranked_lidar_city_lkup_res;
-- 56,567,275