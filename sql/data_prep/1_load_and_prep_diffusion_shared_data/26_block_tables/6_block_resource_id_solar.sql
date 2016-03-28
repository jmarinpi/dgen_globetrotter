﻿set role 'diffusion-writers';

------------------------------------------------------------------------------------------
-- create separate tables for each geometry type

-- small blocks
DROP TABLE IF EXISTS diffusion_blocks.block_resource_id_solar_small_blocks;
CREATE TABLE diffusion_blocks.block_resource_id_solar_small_blocks AS
select a.pgid, b.gid as solar_re_9809_gid
FROM diffusion_blocks.block_geoms a
LEFT JOIN solar.solar_re_9809 b
ON ST_Intersects(a.the_point_96703, b.the_geom_96703)
where a.exceeds_10_acres = False;
-- 5968895 rows

DROP TABLE IF EXISTS diffusion_blocks.block_resource_id_solar_big_blocks;
CREATE TABLE diffusion_blocks.block_resource_id_solar_big_blocks 
(
	pgid bigint,
	solar_re_9809_gid integer
);

select parsel_2('dav-gis', 'mgleason', 'mgleason',
				'diffusion_blocks.block_geoms', 'pgid'
				'with a as
				(
					SELECT a.pgid, b.gid as solar_re_9809_gid, 
						   SUM(ST_Area(ST_Intersection(a.the_geom_96703, b.the_geom_96703))) as int_area
					FROM diffusion_blocks.block_geoms a
					LEFT JOIN solar.solar_re_9809 b
					ON ST_Intersects(a.the_geom_96703, b.the_geom_96703)
					GROUP BY a.pgid, b.gid
				)
				select distinct ON (a.pgid) a.pgid, a.solar_re_9809_gid
				FROM a
				ORDER BY a.pgid asc, a.int_area desc;',
				'diffusion_blocks.block_resource_id_solar_big_blocks', 'a', 16
				);


------------------------------------------------------------------------------------------
-- QA/QC

-- points
-- check for nulls
select count(*)
FROM diffusion_blocks.block_resource_id_solar_small_blocks
where solar_re_9809_gid is null;
-- 11430

-- where are these
select a.*, b.state_abbr
FROM diffusion_blocks.block_resource_id_solar_small_blocks a
left join diffusion_blocks.block_geoms b
ON a.pgid = b.pgid
where a.solar_re_9809_gid is null;
-- several states on the boundaries (WA, TX, ND, etc.)
-- plus AK, which won't have results

-- fix using nearest neighbor in the same county with good data
with a as
(
	select a.pgid, b.state_fips, b.county_fips, b.the_point_96703
	from diffusion_blocks.block_resource_id_solar_small_blocks a
	left join diffusion_blocks.block_geoms b
	ON a.pgid = b.pgid
	where a.solar_re_9809_gid is null
),
b as
(
	select a.pgid, a.solar_re_9809_gid, b.state_fips, b.county_fips, b.the_point_96703
	from diffusion_blocks.block_resource_id_solar_small_blocks a
	left join diffusion_blocks.block_geoms b
	ON a.pgid = b.pgid
	where a.solar_re_9809_gid is NOT null
),
c as
(
	select a.pgid, b.solar_re_9809_gid, 
		ST_Distance(a.the_point_96703, b.the_point_96703) as dist_m
	from a
	left join b
	ON a.state_fips = b.state_fips
	and a.county_fips = b.county_fips
),
d AS
(
	select distinct on (c.pgid) c.pgid, c.solar_re_9809_gid
	from c
	ORDER BY c.pgid ASC, c.dist_m asc
)
UPDATE diffusion_blocks.block_resource_id_solar_small_blocks e
set solar_re_9809_gid = d.solar_re_9809_gid
from d
where e.pgid = d.pgid
AND e.solar_re_9809_gid is null;
-- 11430 rows

-- recheck for nulls
select count(*)
FROM diffusion_blocks.block_resource_id_solar_small_blocks
where solar_re_9809_gid is null;
-- 11430

-- where are these
select distinct b.state_abbr
FROM diffusion_blocks.block_resource_id_solar_small_blocks a
left join diffusion_blocks.block_geoms b
ON a.pgid = b.pgid
where a.solar_re_9809_gid is null;
-- all in AK -- all set