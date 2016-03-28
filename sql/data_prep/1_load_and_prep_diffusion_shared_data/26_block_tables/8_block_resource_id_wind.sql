

with a as
(
	SELECT a.pgid,
		   ST_ValueCount(ST_Intersection(a.the_geom_4326, b.rast)) as vc
	FROM diffusion_blocks.block_geoms a
	LEFT JOIN aws_2014.iiijjjicf_200m_raster_100x100 b
	ON ST_Intersects(a.the_geom_4326, b.rast)
),
b as
(
	select a.pgid, (vc).


)
select distinct ON (a.pgid) a.pgid, a.solar_re_9809_gid
FROM a
ORDER BY a.pgid asc, a.int_area desc;',
'diffusion_blocks.block_resource_id_solar_big_blocks



				aws_2014.iiijjjicf_200m_raster_100x100 b
		ON ST_Intersects(b.rast,a.the_geom_4326)