------------------------------------------------------------------------------------------------
-- get values from 50 and 80 m rasters to validate to the results calculated from hdfs 

-- ALTER TABLE diffusion_data_wind.us_onshore_windspeed_50m
-- OWNER TO "diffusion-writers";
-- ALTER TABLE diffusion_data_wind.us_onshore_windspeed_80m
-- OWNER TO "diffusion-writers";

SET ROLE 'diffusion-writers';
	
-- res
DROP TABLE IF EXISTS diffusion_shared.pt_grid_us_res_windspeeds_50;
CREATE TABLE diffusion_shared.pt_grid_us_res_windspeeds_50 
(
	gid integer,
	height integer,
	windspeed_avg_ms numeric
);

select parsel_2('dav-gis', 'mgleason', 'mgleason',
		'diffusion_shared.pt_grid_us_res', 'gid',
		'select a.gid, 50::integer as height, ST_Value(b.rast, a.the_geom_4326) as windspeed_avg_ms
		from diffusion_shared.pt_grid_us_res a
		INNER JOIN diffusion_data_wind.us_onshore_windspeed_50m b
		ON ST_Intersects(a.the_geom_4326, b.rast)',
		'diffusion_shared.pt_grid_us_res_windspeeds_50 ',
		'a', 16);

CREATE INDEX pt_grid_us_res_windspeeds_50_btree_gid
ON diffusion_shared.pt_grid_us_res_windspeeds_50
USING BTREE(gid);

CREATE INDEX pt_grid_us_res_windspeeds_50_btree_height
ON diffusion_shared.pt_grid_us_res_windspeeds_50
USING BTREE(height);


DROP TABLE IF EXISTS diffusion_shared.pt_grid_us_res_windspeeds_80;
CREATE TABLE diffusion_shared.pt_grid_us_res_windspeeds_80 
(
	gid integer,
	height integer,
	windspeed_avg_ms numeric
);

select parsel_2('dav-gis', 'mgleason', 'mgleason',
		'diffusion_shared.pt_grid_us_res', 'gid',
		'select a.gid, 80::integer as height, ST_Value(b.rast, a.the_geom_4326) as windspeed_avg_ms
		from diffusion_shared.pt_grid_us_res a
		INNER JOIN diffusion_data_wind.us_onshore_windspeed_80m b
		ON ST_Intersects(a.the_geom_4326, b.rast)',
		'diffusion_shared.pt_grid_us_res_windspeeds_80 ',
		'a', 16);

CREATE INDEX pt_grid_us_res_windspeeds_80_btree_gid
ON diffusion_shared.pt_grid_us_res_windspeeds_80
USING BTREE(gid);

CREATE INDEX pt_grid_us_res_windspeeds_80_btree_height
ON diffusion_shared.pt_grid_us_res_windspeeds_80
USING BTREE(height);

-- com
DROP TABLE IF EXISTS diffusion_shared.pt_grid_us_com_windspeeds_50;
CREATE TABLE diffusion_shared.pt_grid_us_com_windspeeds_50 
(
	gid integer,
	height integer,
	windspeed_avg_ms numeric
);

select parsel_2('dav-gis', 'mgleason', 'mgleason',
		'diffusion_shared.pt_grid_us_com', 'gid',
		'select a.gid, 50::integer as height, ST_Value(b.rast, a.the_geom_4326) as windspeed_avg_ms
		from diffusion_shared.pt_grid_us_com a
		INNER JOIN diffusion_data_wind.us_onshore_windspeed_50m b
		ON ST_Intersects(a.the_geom_4326, b.rast)',
		'diffusion_shared.pt_grid_us_com_windspeeds_50 ',
		'a', 16);

CREATE INDEX pt_grid_us_com_windspeeds_50_btree_gid
ON diffusion_shared.pt_grid_us_com_windspeeds_50
USING BTREE(gid);

CREATE INDEX pt_grid_us_com_windspeeds_50_btree_height
ON diffusion_shared.pt_grid_us_com_windspeeds_50
USING BTREE(height);


DROP TABLE IF EXISTS diffusion_shared.pt_grid_us_com_windspeeds_80;
CREATE TABLE diffusion_shared.pt_grid_us_com_windspeeds_80 
(
	gid integer,
	height integer,
	windspeed_avg_ms numeric
);


select parsel_2('dav-gis', 'mgleason', 'mgleason',
		'diffusion_shared.pt_grid_us_com', 'gid',
		'select a.gid, 80::integer as height, ST_Value(b.rast, a.the_geom_4326) as windspeed_avg_ms
		from diffusion_shared.pt_grid_us_com a
		INNER JOIN diffusion_data_wind.us_onshore_windspeed_80m b
		ON ST_Intersects(a.the_geom_4326, b.rast)',
		'diffusion_shared.pt_grid_us_com_windspeeds_80 ',
		'a', 16);

CREATE INDEX pt_grid_us_com_windspeeds_80_btree_gid
ON diffusion_shared.pt_grid_us_com_windspeeds_80
USING BTREE(gid);

CREATE INDEX pt_grid_us_com_windspeeds_80_btree_height
ON diffusion_shared.pt_grid_us_com_windspeeds_80
USING BTREE(height);


-- ind
DROP TABLE IF EXISTS diffusion_shared.pt_grid_us_ind_windspeeds_50;
CREATE TABLE diffusion_shared.pt_grid_us_ind_windspeeds_50 
(
	gid integer,
	height integer,
	windspeed_avg_ms numeric
);

select parsel_2('dav-gis', 'mgleason', 'mgleason',
		'diffusion_shared.pt_grid_us_ind', 'gid',
		'select a.gid, 50::integer as height, ST_Value(b.rast, a.the_geom_4326) as windspeed_avg_ms
		from diffusion_shared.pt_grid_us_ind a
		INNER JOIN diffusion_data_wind.us_onshore_windspeed_50m b
		ON ST_Intersects(a.the_geom_4326, b.rast)',
		'diffusion_shared.pt_grid_us_ind_windspeeds_50 ',
		'a', 16);

CREATE INDEX pt_grid_us_ind_windspeeds_50_btree_gid
ON diffusion_shared.pt_grid_us_ind_windspeeds_50
USING BTREE(gid);

CREATE INDEX pt_grid_us_ind_windspeeds_50_btree_height
ON diffusion_shared.pt_grid_us_ind_windspeeds_50
USING BTREE(height);


DROP TABLE IF EXISTS diffusion_shared.pt_grid_us_ind_windspeeds_80;
CREATE TABLE diffusion_shared.pt_grid_us_ind_windspeeds_80 
(
	gid integer,
	height integer,
	windspeed_avg_ms numeric
);


select parsel_2('dav-gis', 'mgleason', 'mgleason',
		'diffusion_shared.pt_grid_us_ind', 'gid',
		'select a.gid, 80::integer as height, ST_Value(b.rast, a.the_geom_4326) as windspeed_avg_ms
		from diffusion_shared.pt_grid_us_ind a
		INNER JOIN diffusion_data_wind.us_onshore_windspeed_80m b
		ON ST_Intersects(a.the_geom_4326, b.rast)',
		'diffusion_shared.pt_grid_us_ind_windspeeds_80 ',
		'a', 16);

CREATE INDEX pt_grid_us_ind_windspeeds_80_btree_gid
ON diffusion_shared.pt_grid_us_ind_windspeeds_80
USING BTREE(gid);

CREATE INDEX pt_grid_us_ind_windspeeds_80_btree_height
ON diffusion_shared.pt_grid_us_ind_windspeeds_80
USING BTREE(height);



--------------------------------------------------------------------------------
-- append the hdf results to the points by sector

CREATE INDEX  wind_speed_avg_btree_i_j_cf_bin_btree
ON diffusion_data_wind.wind_speed_avg
USING BTREE(i, j, cf_bin);

-- res
DROP TABLE IF EXISTS diffusion_data_wind.pt_grid_us_res_windspeeds_hdf;
CREATE TABLE diffusion_data_wind.pt_grid_us_res_windspeeds_hdf AS
select a.gid, b.i, b.j, b.cf_bin,
	c.height, c.windspeed_avg_ms
FROM diffusion_shared.pt_grid_us_res a
LEFT JOIN diffusion_wind.ij_cfbin_lookup_res_pts_us b 
	ON a.gid = b.pt_gid
LEFT JOIN diffusion_data_wind.wind_speed_avg c
ON b.i = c.i
and b.j = c.j
and b.cf_bin = c.cf_bin;
-- 17,255,577 rows

create index pt_grid_us_res_windspeeds_hdf_btree_gid
ON diffusion_data_wind.pt_grid_us_res_windspeeds_hdf
using BTREE(gid);

create index pt_grid_us_res_windspeeds_hdf_btree_height
ON diffusion_data_wind.pt_grid_us_res_windspeeds_hdf
using BTREE(height);

-- com
DROP TABLE IF EXISTS diffusion_data_wind.pt_grid_us_com_windspeeds_hdf;
CREATE TABLE diffusion_data_wind.pt_grid_us_com_windspeeds_hdf AS
select a.gid, b.i, b.j, b.cf_bin,
	c.height, c.windspeed_avg_ms
FROM diffusion_shared.pt_grid_us_com a
LEFT JOIN diffusion_wind.ij_cfbin_lookup_com_pts_us b 
	ON a.gid = b.pt_gid
LEFT JOIN diffusion_data_wind.wind_speed_avg c
ON b.i = c.i
and b.j = c.j
and b.cf_bin = c.cf_bin;
-- 4,811,874 rows

create index pt_grid_us_com_windspeeds_hdf_btree_gid
ON diffusion_data_wind.pt_grid_us_com_windspeeds_hdf
using BTREE(gid);

create index pt_grid_us_com_windspeeds_hdf_btree_height
ON diffusion_data_wind.pt_grid_us_com_windspeeds_hdf
using BTREE(height);

-- ind
DROP TABLE IF EXISTS diffusion_data_wind.pt_grid_us_ind_windspeeds_hdf;
CREATE TABLE diffusion_data_wind.pt_grid_us_ind_windspeeds_hdf AS
select a.gid, b.i, b.j, b.cf_bin,
	c.height, c.windspeed_avg_ms
FROM diffusion_shared.pt_grid_us_com a
LEFT JOIN diffusion_wind.ij_cfbin_lookup_ind_pts_us b 
	ON a.gid = b.pt_gid
LEFT JOIN diffusion_data_wind.wind_speed_avg c
ON b.i = c.i
and b.j = c.j
and b.cf_bin = c.cf_bin;
-- 3,894,332 rows

create index pt_grid_us_ind_windspeeds_hdf_btree_gid
ON diffusion_data_wind.pt_grid_us_ind_windspeeds_hdf
using BTREE(gid);

create index pt_grid_us_ind_windspeeds_hdf_btree_height
ON diffusion_data_wind.pt_grid_us_ind_windspeeds_hdf
using BTREE(height);
------------------------------------------------------------------------------------------

-- validate hdf results
-- res
DROP TABLE IF EXISTS diffusion_data_wind.pt_grid_us_res_windspeeds_hdf_validation;
CREATE TABLE diffusion_data_wind.pt_grid_us_res_windspeeds_hdf_validation AS
with b as
(
	select gid, height, windspeed_avg_ms
	from diffusion_shared.pt_grid_us_res_windspeeds_50 
	UNION 
	SELECT gid, height, windspeed_avg_ms
	from diffusion_shared.pt_grid_us_res_windspeeds_80
)
select a.gid, a.height, a.windspeed_avg_ms as hdf_ms, b.windspeed_avg_ms as rast_ms, 
	(b.windspeed_avg_ms-a.windspeed_avg_ms)/b.windspeed_avg_ms as pct_diff
from diffusion_data_wind.pt_grid_us_res_windspeeds_hdf a
INNER join b
ON a.gid = b.gid
and a.height = b.height;
-- 11,503,718

-- com
DROP TABLE IF EXISTS diffusion_data_wind.pt_grid_us_com_windspeeds_hdf_validation;
CREATE TABLE diffusion_data_wind.pt_grid_us_com_windspeeds_hdf_validation AS
with b as
(
	select gid, height, windspeed_avg_ms
	from diffusion_shared.pt_grid_us_com_windspeeds_50 
	UNION 
	SELECT gid, height, windspeed_avg_ms
	from diffusion_shared.pt_grid_us_com_windspeeds_80
)
select a.gid, a.height, a.windspeed_avg_ms as hdf_ms, b.windspeed_avg_ms as rast_ms, 
	(b.windspeed_avg_ms-a.windspeed_avg_ms)/b.windspeed_avg_ms as pct_diff
from diffusion_data_wind.pt_grid_us_com_windspeeds_hdf a
INNER join b
ON a.gid = b.gid
and a.height = b.height;
-- 3,207,916

-- ind
DROP TABLE IF EXISTS diffusion_data_wind.pt_grid_us_ind_windspeeds_hdf_validation;
CREATE TABLE diffusion_data_wind.pt_grid_us_ind_windspeeds_hdf_validation AS
with b as
(
	select gid, height, windspeed_avg_ms
	from diffusion_shared.pt_grid_us_ind_windspeeds_50 
	UNION 
	SELECT gid, height, windspeed_avg_ms
	from diffusion_shared.pt_grid_us_ind_windspeeds_80
)
select a.gid, a.height, a.windspeed_avg_ms as hdf_ms, b.windspeed_avg_ms as rast_ms, 
	(b.windspeed_avg_ms-a.windspeed_avg_ms)/b.windspeed_avg_ms as pct_diff
from diffusion_data_wind.pt_grid_us_ind_windspeeds_hdf a
INNER join b
ON a.gid = b.gid
and a.height = b.height;
-- 2,290,374

select *
from diffusion_data_wind.pt_grid_us_ind_windspeeds_hdf_validation
order by pct_diff desc
limit 100;

select *
from diffusion_data_wind.pt_grid_us_com_windspeeds_hdf_validation
order by pct_diff desc
limit 100;

select *
from diffusion_data_wind.pt_grid_us_res_windspeeds_hdf_validation
order by pct_diff desc
limit 100;

-- determine how many have a significant percent difference
select count(*)
FROM diffusion_data_wind.pt_grid_us_res_windspeeds_hdf_validation
where @(pct_diff) > 0.05;

select 447736/11503718.; -- 4% are off by greater than 5%

select count(*)
FROM diffusion_data_wind.pt_grid_us_com_windspeeds_hdf_validation
where @(pct_diff) > 0.05;

select 129735/3207916.; -- 4% are off by greater than 5%


-- something is wrong with ind...
select count(*)
FROM diffusion_data_wind.pt_grid_us_ind_windspeeds_hdf_validation
where @(pct_diff) > 0.05;

select 86639/2290374.; -- 4% are off by greater than 5%
-- not a big deal, let's just ignore these
------------------------------------------------------------------------------------------------


SELECT 'ind'::TEXT as sector, height, windspeed_avg_ms
       FROM diffusion_data_wind.pt_grid_us_ind_windspeeds_hdf
       where height is null;
