-- now calculate distances 
-- use 96703 geom since geog will be too slow
DROP TABLE IF EXISTS diffusion_data_shared.county_to_lidar_city_distances_lkup;
CREATE TABLE diffusion_data_shared.county_to_lidar_city_distances_lkup
(
	county_id integer,
	city_id integer,
	city text,
	state character varying(2),
	year character varying(2),
	basename text,
	dist_m numeric
);

SELECT parsel_2('dav-gis','mgleason','mgleason',
		'diffusion_shared.county_geom','county_id',
		'SELECT a.county_id, b.city_id, b.city, b.state, b.year, b.basename,
			ST_Distance(a.the_geom_96703, b.rasd_the_geom_96703) as dist_m
		FROM diffusion_shared.county_geom a
		LEFT JOIN pv_rooftop_dsolar_integration.solar_gid b
			ON a.census_region = b.census_region',
		'diffusion_data_shared.county_to_lidar_city_distances_lkup', 'a', 16);

-- create indices
CREATE INDEX county_to_lidar_city_distances_lkup_county_id_btree
ON diffusion_data_shared.county_to_lidar_city_distances_lkup
USING BTREE(county_id);

CREATE INDEX county_to_lidar_city_distances_lkup_year_btree
ON diffusion_data_shared.county_to_lidar_city_distances_lkup
USING BTREE(year);

CREATE INDEX county_to_lidar_city_distances_lkup_dist_m_btree
ON diffusion_data_shared.county_to_lidar_city_distances_lkup
USING BTREE(dist_m);
