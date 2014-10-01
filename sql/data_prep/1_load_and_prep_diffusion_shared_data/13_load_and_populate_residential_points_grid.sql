-- DROP TABLE IF EXISTS  diffusion_shared.pt_grid_us_res;
CREATE TABLE diffusion_shared.pt_grid_us_res (
	x numeric,
	y numeric,
	temp_col integer);

SET ROLE "server-superusers";

COPY diffusion_shared.pt_grid_us_res FROM '/srv/home/mgleason/data/dg_wind/res_points_200m_us.csv' with csv header;

RESET ROLE;

ALTER TABLE diffusion_shared.pt_grid_us_res ALTER x TYPE INTEGER;
ALTER TABLE diffusion_shared.pt_grid_us_res ALTER y TYPE INTEGER;
-- drop this column -- it means nothing
ALTER TABLE diffusion_shared.pt_grid_us_res DROP COLUMN temp_col;


ALTER TABLE diffusion_shared.pt_grid_us_res ADD COLUMN the_geom_900914 geometry;

UPDATE diffusion_shared.pt_grid_us_res
SET the_geom_900914 = ST_SetSRID(ST_MakePoint(x,y),900914);

CREATE INDEX pt_grid_us_res_the_geom_900914_gist ON diffusion_shared.pt_grid_us_res USING gist(the_geom_900914);
CLUSTER diffusion_shared.pt_grid_us_res USING pt_grid_us_res_the_geom_900914_gist;

VACUUM ANALYZE  diffusion_shared.pt_grid_us_res;

-- add:
-- gid (serial) and pkey
ALTER TABLE diffusion_shared.pt_grid_us_res ADD COLUMN gid serial;
ALTER TABLE diffusion_shared.pt_grid_us_res ADD PRIMARY KEY (gid);
-- the_geom_4326 with index
ALTER TABLE diffusion_shared.pt_grid_us_res ADD COLUMN the_geom_4326 geometry;

UPDATE  diffusion_shared.pt_grid_us_res
SET the_geom_4326 = ST_Transform(the_geom_900914,4326);

CREATE INDEX pt_grid_us_res_the_geom_4326_gist ON diffusion_shared.pt_grid_us_res USING gist(the_geom_4326);
-- county id (from polygons) and index (foreign key)?
ALTER TABLE diffusion_shared.pt_grid_us_res ADD COLUMN county_id integer;

UPDATE diffusion_shared.pt_grid_us_res a
SET county_id = b.county_id
FROM diffusion_shared.county_geom b
WHERE ST_Intersects(a.the_geom_900914, b.the_geom_900914);

CREATE INDEX pt_grid_us_res_county_id_btree ON diffusion_shared.pt_grid_us_res using btree(county_id);
-- check why there are nulls
DROP TABLE IF EXISTS diffusion_wind_data.no_county_pts;
CREATE TABLE  diffusion_wind_data.no_county_pts AS
SELECT gid, the_geom_4326
FROM diffusion_shared.pt_grid_us_res
where county_id is null;
-- inspect in Q -- they are all around the edges of the country -- nothing suprising here

-- get the nearest neighbor for each missing points
DROP TABLE IF EXISTS  diffusion_wind_data.no_county_pts_closest;
CREATE TABLE  diffusion_wind_data.no_county_pts_closest AS
with candidates as (

SELECT a.gid, a.the_geom_900914, 
	unnest((select array(SELECT b.county_id
	 FROM diffusion_shared.county_geom b
	 ORDER BY a.the_geom_900914 <#> b.the_geom_900914 LIMIT 3))) as county_id
FROM diffusion_shared.pt_grid_us_res a
where a.county_id is null
 )

SELECT distinct ON (gid) a.gid, a.the_geom_900914, a.county_id, b.county
FROM candidates a
lEFT JOIN diffusion_shared.county_geom b
ON a.county_id = b.county_id
ORDER BY gid, ST_Distance(a.the_geom_900914,b.the_geom_900914) asc;

UPDATE diffusion_shared.pt_grid_us_res a
SET county_id = b.county_id
FROM  diffusion_wind_data.no_county_pts_closest b
WHERE a.county_id is null
and a.gid = b.gid;

-- check no nulls remain
SELECT count(*)
FROM diffusion_shared.pt_grid_us_res 
where county_id is null;

-- utility type
DROP TABLE IF EXISTS  diffusion_wind_data.pt_grid_us_res_utiltype_lookup;
CREATE TABLE  diffusion_wind_data.pt_grid_us_res_utiltype_lookup (
	gid integer,
	utility_type character varying(9));

	select parsel_2('dav-gis','diffusion_shared.pt_grid_us_res','gid',
	'WITH ut_ranks as (
		SELECT unnest(array[''IOU'',''Muni'',''Coop'',''All Other'']) as utility_type, generate_series(1,4) as rank
	),
	isect as (
		SELECT a.gid, b.company_type_general as utility_type, c.rank
		FROM diffusion_shared.pt_grid_us_res a
		INNER JOIN dg_wind.ventyx_elec_serv_territories_edit_diced b
		ON ST_Intersects(a.the_geom_4326, b.the_geom_4326)
		LEFT JOIN ut_ranks c
		ON b.company_type_general = c.utility_type)
	SELECT DISTINCT ON (a.gid) a.gid, a.utility_type 
	FROM isect a
	ORDER BY a.gid, a.rank ASC;',' diffusion_wind_data.pt_grid_us_res_utiltype_lookup', 'a', 16);
	
	-- join the info back in
	ALTER TABLE diffusion_shared.pt_grid_us_res ADD COLUMN utility_type character varying(9);

	CREATE INDEX pt_grid_us_res_utiltype_lookup_gid_btree ON  diffusion_wind_data.pt_grid_us_res_utiltype_lookup using btree(gid);

	UPDATE diffusion_shared.pt_grid_us_res a
	SET utility_type = b.utility_type
	FROM  diffusion_wind_data.pt_grid_us_res_utiltype_lookup b
	where a.gid = b.gid;
	
	CREATE INDEX pt_grid_us_res_utility_type_btree ON diffusion_shared.pt_grid_us_res USING btree(utility_type);
	
	-- are there any nulls?
	SELECT count(*) 
	FROM diffusion_shared.pt_grid_us_res
	where utility_type is null;

	-- isolate the unjoined points
	-- and fix them by assigning value from their nearest neighbor that is not null
	DROP TABLE IF EXISTS  diffusion_wind_data.pt_grid_us_res_utiltype_missing;
	CREATE TABLE  diffusion_wind_data.pt_grid_us_res_utiltype_missing AS
	with a AS(
		select gid, the_geom_900914
		FROM diffusion_shared.pt_grid_us_res
		where utility_type is null)
	SELECT a.gid, a.the_geom_900914, 
		(SELECT b.utility_type 
		 FROM diffusion_shared.pt_grid_us_res b
		 where b.utility_type is not null
		 ORDER BY a.the_geom_900914 <#> b.the_geom_900914
		 LIMIT 1) as utility_type
	FROM a;

	--update the points table
	UPDATE diffusion_shared.pt_grid_us_res a
	SET utility_type = b.utility_type
	FROM  diffusion_wind_data.pt_grid_us_res_utiltype_missing b
	where a.gid = b.gid
	and a.utility_type is null;

	-- any nulls left?
	SELECT count(*) 
	FROM diffusion_shared.pt_grid_us_res
	where utility_type is null;




-- iii, jjj, icf (from raster)
DROP TABLE IF EXISTS  diffusion_wind_data.pt_grid_us_res_iiijjjicf_id_lookup;
CREATE TABLE  diffusion_wind_data.pt_grid_us_res_iiijjjicf_id_lookup (
	gid integer,
	iiijjjicf_id integer);

	--run in parallel for speed (100x100 tiles are necessary for it ti finishin in about 7 mins -- 1000x1000 tiles would take several hours even in parallel)
	SELECT parsel_2('dav-gis','diffusion_shared.pt_grid_us_res','gid',
	'SELECT a.gid, ST_Value(b.rast,a.the_geom_900914) as iiijjjicf_id
	FROM  diffusion_shared.pt_grid_us_res a
	INNER JOIN  diffusion_wind_data.iiijjjicf_us_100x100 b
	ON ST_Intersects(b.rast,a.the_geom_900914);',
		' diffusion_wind_data.pt_grid_us_res_iiijjjicf_id_lookup', 'a',16);
	
	-- join the info back in
	ALTER TABLE diffusion_shared.pt_grid_us_res ADD COLUMN iiijjjicf_id integer;

	CREATE INDEX pt_grid_us_res_iiijjjicf_id_lookup_gid_btree ON  diffusion_wind_data.pt_grid_us_res_iiijjjicf_id_lookup using btree(gid);

	UPDATE diffusion_shared.pt_grid_us_res a
	SET iiijjjicf_id = b.iiijjjicf_id
	FROM  diffusion_wind_data.pt_grid_us_res_iiijjjicf_id_lookup b
	where a.gid = b.gid;

	CREATE INDEX pt_grid_us_res_iiijjjicf_id_btree ON diffusion_shared.pt_grid_us_res USING btree(iiijjjicf_id);
	
	-- are there any nulls?
	SELECT count(*) 
	FROM diffusion_shared.pt_grid_us_res
	where iiijjjicf_id is null;
	
	-- isolate the unjoined points
	-- and fix them by assigning value from their nearest neighbor that is not null
	DROP TABLE IF EXISTS  diffusion_wind_data.pt_grid_us_res_iiijjjicf_id_lookup_no_id;
	CREATE TABLE  diffusion_wind_data.pt_grid_us_res_iiijjjicf_id_lookup_no_id AS
	with a AS(
		select gid, the_geom_900914
		FROM diffusion_shared.pt_grid_us_res
		where iiijjjicf_id is null)
	SELECT a.gid, a.the_geom_900914, 
		(SELECT b.iiijjjicf_id 
		 FROM diffusion_shared.pt_grid_us_res b
		 where b.iiijjjicf_id is not null
		 ORDER BY a.the_geom_900914 <#> b.the_geom_900914
		 LIMIT 1) as iiijjjicf_id
	FROM a;

	--update the lookup table
	UPDATE  diffusion_wind_data.pt_grid_us_res_iiijjjicf_id_lookup a
	SET iiijjjicf_id = b.iiijjjicf_id
	FROM  diffusion_wind_data.pt_grid_us_res_iiijjjicf_id_lookup_no_id b
	where a.gid = b.gid
	and a.iiijjjicf_id is null;

	--update the points table
	UPDATE diffusion_shared.pt_grid_us_res a
	SET iiijjjicf_id = b.iiijjjicf_id
	FROM  diffusion_wind_data.pt_grid_us_res_iiijjjicf_id_lookup b
	where a.gid = b.gid
	and a.iiijjjicf_id is null;

	-- any nulls left?
	SELECT count(*) 
	FROM diffusion_shared.pt_grid_us_res
	where iiijjjicf_id is null;

-- 3 different versions of exclusions (from rasters)
-- population density only
	DROP TABLE IF EXISTS  diffusion_wind_data.pt_grid_us_res_maxheight_popdens_lookup;
	CREATE TABLE  diffusion_wind_data.pt_grid_us_res_maxheight_popdens_lookup (
		gid integer,
		maxheight_m_popdens integer);

	--run in parallel for speed (100x100 tiles are necessary for it ti finishin in about 7 mins -- 1000x1000 tiles would take several hours even in parallel)
	SELECT parsel_2('dav-gis','diffusion_shared.pt_grid_us_res','gid',
			'SELECT a.gid, ST_Value(b.rast,a.the_geom_900914) as maxheight_m_popdens
			FROM  diffusion_shared.pt_grid_us_res a
			INNER JOIN  diffusion_wind_data.maxheight_popdens_us_100x100 b
			ON ST_Intersects(b.rast,a.the_geom_900914);',
		' diffusion_wind_data.pt_grid_us_res_maxheight_popdens_lookup', 'a',16);

	-- join the info back in
	ALTER TABLE diffusion_shared.pt_grid_us_res ADD COLUMN maxheight_m_popdens integer;

	CREATE INDEX pt_grid_us_res_maxheight_popdens_lookup_gid_btree ON  diffusion_wind_data.pt_grid_us_res_maxheight_popdens_lookup using btree(gid);

	UPDATE diffusion_shared.pt_grid_us_res a
	SET maxheight_m_popdens = b.maxheight_m_popdens
	FROM  diffusion_wind_data.pt_grid_us_res_maxheight_popdens_lookup b
	where a.gid = b.gid;

	-- how many are null?
	CREATE INDEX pt_grid_us_res_maxheight_m_popdens ON diffusion_shared.pt_grid_us_res USING btree(maxheight_m_popdens) where maxheight_m_popdens is null;

	-- any missing?
	select count(*)
	FROM diffusion_shared.pt_grid_us_res 
	where maxheight_m_popdens is null;
	-- nope, so dont need to do the commented section of this code block below
	
	-- isolate the unjoined points
	-- and fix them by assigning value from their nearest neighbor that is not null
	-- DROP TABLE IF EXISTS diffusion_shared.pt_grid_us_res_maxheight_m_popdens_missing_lookup;
-- 	CREATE TABLE diffusion_shared.pt_grid_us_res_maxheight_m_popdens_missing_lookup AS
-- 	with a AS(
-- 		select gid, the_geom_900914
-- 		FROM diffusion_shared.pt_grid_us_res
-- 		where maxheight_m_popdens is null)
-- 	SELECT a.gid, a.the_geom_900914, 
-- 		(SELECT b.maxheight_m_popdens 
-- 		 FROM diffusion_shared.pt_grid_us_res b
-- 		 where b.maxheight_m_popdens is not null
-- 		 ORDER BY a.the_geom_900914 <#> b.the_geom_900914
-- 		 LIMIT 1) as maxheight_m_popdens
-- 	FROM a;
-- 
-- 	--update the lookup table
-- 	UPDATE  diffusion_wind_data.pt_grid_us_res_maxheight_popdens_lookup a
-- 	SET maxheight_m_popdens = b.maxheight_m_popdens
-- 	FROM diffusion_shared.pt_grid_us_res_maxheight_m_popdens_missing_lookup b
-- 	where a.gid = b.gid
-- 	and a.maxheight_m_popdens is null;
-- 
-- 	--update the points table
-- 	UPDATE diffusion_shared.pt_grid_us_res a
-- 	SET maxheight_m_popdens = b.maxheight_m_popdens
-- 	FROM  diffusion_wind_data.pt_grid_us_res_maxheight_popdens_lookup b
-- 	where a.gid = b.gid
-- 	and a.maxheight_m_popdens is null;
--  
-- 	-- check for any remaining nulls?
-- 	select count(*)
-- 	FROM diffusion_shared.pt_grid_us_res 
-- 	where maxheight_m_popdens is null;
-- 
-- 	VACUUM ANALYZE diffusion_shared.pt_grid_us_res;

-- population density and 20 pc canopy cover
	DROP TABLE IF EXISTS  diffusion_wind_data.pt_grid_us_res_maxheight_popdenscancov20pc_lookup;
	CREATE TABLE  diffusion_wind_data.pt_grid_us_res_maxheight_popdenscancov20pc_lookup (
		gid integer,
		maxheight_m_popdenscancov20pc integer);

	--run in parallel for speed (100x100 tiles are necessary for it ti finishin in about 7 mins -- 1000x1000 tiles would take several hours even in parallel)
	SELECT parsel_2('dav-gis','diffusion_shared.pt_grid_us_res','gid',
			'SELECT a.gid, ST_Value(b.rast,a.the_geom_900914) as maxheight_m_popdenscancov20pc
			FROM  diffusion_shared.pt_grid_us_res a
			INNER JOIN  diffusion_wind_data.maxheight_popdenscancov20pc_us_100x100 b
			ON ST_Intersects(b.rast,a.the_geom_900914);',
		' diffusion_wind_data.pt_grid_us_res_maxheight_popdenscancov20pc_lookup', 'a',16);

	-- join the info back in
	ALTER TABLE diffusion_shared.pt_grid_us_res ADD COLUMN maxheight_m_popdenscancov20pc integer;

	CREATE INDEX pt_grid_us_res_maxheight_popdenscancov20pc_lookup_gid_btree ON  diffusion_wind_data.pt_grid_us_res_maxheight_popdenscancov20pc_lookup using btree(gid);

	UPDATE diffusion_shared.pt_grid_us_res a
	SET maxheight_m_popdenscancov20pc = b.maxheight_m_popdenscancov20pc
	FROM  diffusion_wind_data.pt_grid_us_res_maxheight_popdenscancov20pc_lookup b
	where a.gid = b.gid;

	-- how many are null?
	CREATE INDEX pt_grid_us_res_maxheight_m_popdenscancov20pc_btree ON diffusion_shared.pt_grid_us_res USING btree(maxheight_m_popdenscancov20pc) where maxheight_m_popdenscancov20pc is null;

	-- any missing?
	select count(*)
	FROM diffusion_shared.pt_grid_us_res 
	where maxheight_m_popdenscancov20pc is null;
	-- nope, so dont need to do the commented section of this code block below
	
	-- isolate the unjoined points
	-- and fix them by assigning value from their nearest neighbor that is not null
	-- DROP TABLE IF EXISTS diffusion_shared.pt_grid_us_res_maxheight_popdenscancov20pc_missing_lookup;
-- 	CREATE TABLE diffusion_shared.pt_grid_us_res_maxheight_popdenscancov20pc_missing_lookup AS
-- 	with a AS(
-- 		select gid, the_geom_900914
-- 		FROM diffusion_shared.pt_grid_us_res
-- 		where maxheight_m_popdenscancov20pc is null)
-- 	SELECT a.gid, a.the_geom_900914, 
-- 		(SELECT b.maxheight_m_popdenscancov20pc 
-- 		 FROM diffusion_shared.pt_grid_us_res b
-- 		 where b.maxheight_m_popdenscancov20pc is not null
-- 		 ORDER BY a.the_geom_900914 <#> b.the_geom_900914
-- 		 LIMIT 1) as maxheight_m_popdenscancov20pc
-- 	FROM a;
-- 
-- 	--update the lookup table
-- 	UPDATE  diffusion_wind_data.pt_grid_us_res_maxheight_popdenscancov20pc_lookup a
-- 	SET maxheight_m_popdenscancov20pc = b.maxheight_m_popdenscancov20pc
-- 	FROM diffusion_shared.pt_grid_us_res_maxheight_popdenscancov20pc_missing_lookup b
-- 	where a.gid = b.gid
-- 	and a.maxheight_m_popdenscancov20pc is null;
-- 
-- 	--update the points table
-- 	UPDATE diffusion_shared.pt_grid_us_res a
-- 	SET maxheight_m_popdenscancov20pc = b.maxheight_m_popdenscancov20pc
-- 	FROM  diffusion_wind_data.pt_grid_us_res_maxheight_popdenscancov20pc_lookup b
-- 	where a.gid = b.gid
-- 	and a.maxheight_m_popdenscancov20pc is null;
--  
-- 	-- check for any remaining nulls?
-- 	select count(*)
-- 	FROM diffusion_shared.pt_grid_us_res 
-- 	where maxheight_m_popdenscancov20pc is null;
-- 
-- 	VACUUM ANALYZE diffusion_shared.pt_grid_us_res;

-- population density and 40 pc canopy cover
	DROP TABLE IF EXISTS  diffusion_wind_data.pt_grid_us_res_maxheight_popdenscancov40pc_lookup;
	CREATE TABLE  diffusion_wind_data.pt_grid_us_res_maxheight_popdenscancov40pc_lookup (
		gid integer,
		maxheight_m_popdenscancov40pc integer);

	--run in parallel for speed (100x100 tiles are necessary for it ti finishin in about 7 mins -- 1000x1000 tiles would take several hours even in parallel)
	SELECT parsel_2('dav-gis','diffusion_shared.pt_grid_us_res','gid',
			'SELECT a.gid, ST_Value(b.rast,a.the_geom_900914) as maxheight_m_popdenscancov40pc
			FROM  diffusion_shared.pt_grid_us_res a
			INNER JOIN  diffusion_wind_data.maxheight_popdenscancov40pc_us_100x100 b
			ON ST_Intersects(b.rast,a.the_geom_900914);',
		' diffusion_wind_data.pt_grid_us_res_maxheight_popdenscancov40pc_lookup', 'a',16);

	-- join the info back in
	ALTER TABLE diffusion_shared.pt_grid_us_res ADD COLUMN maxheight_m_popdenscancov40pc integer;

	CREATE INDEX pt_grid_us_res_maxheight_popdenscancov40pc_lookup_gid_btree ON  diffusion_wind_data.pt_grid_us_res_maxheight_popdenscancov40pc_lookup using btree(gid);

	UPDATE diffusion_shared.pt_grid_us_res a
	SET maxheight_m_popdenscancov40pc = b.maxheight_m_popdenscancov40pc
	FROM  diffusion_wind_data.pt_grid_us_res_maxheight_popdenscancov40pc_lookup b
	where a.gid = b.gid;

	-- how many are null?
	CREATE INDEX pt_grid_us_res_maxheight_m_popdenscancov40pc_btree ON diffusion_shared.pt_grid_us_res USING btree(maxheight_m_popdenscancov40pc) where maxheight_m_popdenscancov40pc is null;

	-- any missing?
	select count(*)
	FROM diffusion_shared.pt_grid_us_res 
	where maxheight_m_popdenscancov40pc is null;
	-- nope, so dont need to do the commented section of this code block below
	-- 
-- 	-- isolate the unjoined points
-- 	-- and fix them by assigning value from their nearest neighbor that is not null
-- 	DROP TABLE IF EXISTS diffusion_shared.pt_grid_us_res_maxheight_popdenscancov40pc_missing_lookup;
-- 	CREATE TABLE diffusion_shared.pt_grid_us_res_maxheight_popdenscancov40pc_missing_lookup AS
-- 	with a AS(
-- 		select gid, the_geom_900914
-- 		FROM diffusion_shared.pt_grid_us_res
-- 		where maxheight_m_popdenscancov40pc is null)
-- 	SELECT a.gid, a.the_geom_900914, 
-- 		(SELECT b.maxheight_m_popdenscancov40pc 
-- 		 FROM diffusion_shared.pt_grid_us_res b
-- 		 where b.maxheight_m_popdenscancov40pc is not null
-- 		 ORDER BY a.the_geom_900914 <#> b.the_geom_900914
-- 		 LIMIT 1) as maxheight_m_popdenscancov40pc
-- 	FROM a;
-- 
-- 	--update the lookup table
-- 	UPDATE  diffusion_wind_data.pt_grid_us_res_maxheight_popdenscancov40pc_lookup a
-- 	SET maxheight_m_popdenscancov40pc = b.maxheight_m_popdenscancov40pc
-- 	FROM diffusion_shared.pt_grid_us_res_maxheight_popdenscancov40pc_missing_lookup b
-- 	where a.gid = b.gid
-- 	and a.maxheight_m_popdenscancov40pc is null;
-- 
-- 	--update the points table
-- 	UPDATE diffusion_shared.pt_grid_us_res a
-- 	SET maxheight_m_popdenscancov40pc = b.maxheight_m_popdenscancov40pc
-- 	FROM  diffusion_wind_data.pt_grid_us_res_maxheight_popdenscancov40pc_lookup b
-- 	where a.gid = b.gid
-- 	and a.maxheight_m_popdenscancov40pc is null;
--  
-- 	-- check for any remaining nulls?
-- 	select count(*)
-- 	FROM diffusion_shared.pt_grid_us_res 
-- 	where maxheight_m_popdenscancov40pc is null;
-- 
-- 	VACUUM ANALYZE diffusion_shared.pt_grid_us_res;


-- check results are logical
select count(*)
FROM diffusion_shared.pt_grid_us_res
where maxheight_m_popdenscancov20pc > maxheight_m_popdens; -- 0 rows

select count(*)
FROM diffusion_shared.pt_grid_us_res
where maxheight_m_popdenscancov40pc > maxheight_m_popdens; -- 0 rows

select count(*)
FROM diffusion_shared.pt_grid_us_res
where maxheight_m_popdenscancov20pc > maxheight_m_popdenscancov40pc; -- 0 rows

-- if they aren't, it is probably due to areas where i used a NN search to back fill



-- annual average rates (from polygons)
DROP TABLE IF EXISTS  diffusion_wind_data.pt_grid_us_res_annual_rate_gid_lookup;
CREATE TABLE  diffusion_wind_data.pt_grid_us_res_annual_rate_gid_lookup (
	gid integer,
	annual_rate_gid integer);


SELECT parsel_2('dav-gis','diffusion_shared.pt_grid_us_res','gid',
		'SELECT a.gid, b.gid as annual_rate_gid
		FROM  diffusion_shared.pt_grid_us_res a
		INNER JOIN diffusion_shared.annual_ave_elec_rates_2011 b
		ON ST_Intersects(a.the_geom_4326,b.the_geom_4326)
		WHERE b.res_cents_per_kwh IS NOT NULL;',
	' diffusion_wind_data.pt_grid_us_res_annual_rate_gid_lookup', 'a',16);

-- join the info back in
ALTER TABLE diffusion_shared.pt_grid_us_res ADD COLUMN annual_rate_gid integer;

CREATE INDEX pt_grid_us_res_annual_rate_gid_lookup_gid_btree ON  diffusion_wind_data.pt_grid_us_res_annual_rate_gid_lookup using btree(gid);

UPDATE diffusion_shared.pt_grid_us_res a
SET annual_rate_gid = b.annual_rate_gid
FROM  diffusion_wind_data.pt_grid_us_res_annual_rate_gid_lookup b
where a.gid = b.gid;

	-- check for nulls
	DROP TABLE IF EXISTS  diffusion_wind_data.no_res_rate_pts;
	CREATE TABLE  diffusion_wind_data.no_res_rate_pts AS
	SELECT gid, the_geom_900914
	FROM diffusion_shared.pt_grid_us_res
	where annual_rate_gid is null;
	--inspect in Q
	-- these are all in sliver gaps and along the periphery of the country -- just use the nearest polygon's rate
	

	-- get value for nulls from the closest service territory
	DROP TABLE IF EXISTS  diffusion_wind_data.no_res_rate_pts_closest;
	CREATE TABLE  diffusion_wind_data.no_res_rate_pts_closest AS
	with candidates as (

	SELECT a.gid, a.the_geom_900914, 
		unnest((select array(SELECT b.gid
		 FROM diffusion_shared.annual_ave_elec_rates_2011 b
		 WHERE b.res_cents_per_kwh IS NOT NULL
		 ORDER BY a.the_geom_900914 <#> b.the_geom_900914 LIMIT 5))) as rate_gid
	FROM diffusion_shared.pt_grid_us_res a
	where a.annual_rate_gid is null
	 )

	SELECT distinct ON (gid) a.gid, a.the_geom_900914, a.rate_gid
	FROM candidates a
	LEFT JOIN diffusion_shared.annual_ave_elec_rates_2011 b
	ON a.rate_gid = b.gid
	ORDER BY a.gid, ST_Distance(a.the_geom_900914,b.the_geom_900914) asc;

	-- update the missing values in the main table
	UPDATE diffusion_shared.pt_grid_us_res a
	SET annual_rate_gid = b.rate_gid
	FROM  diffusion_wind_data.no_res_rate_pts_closest b
	WHERE a.annual_rate_gid is null
	and a.gid = b.gid;

	-- check for any remaining nulls
	select count(*)
	FROM diffusion_shared.pt_grid_us_res
	WHERE annual_rate_gid is null;

-- create dsire incentives lookup table
	-- FOR BOTH WIND AND SOLAR
	--create a copy of the incentives_geoms table so i can index the geom
	CREATE TABLE dg_wind.incentives_geoms_copy AS
	sELECT gid, the_geom
	FROM geo_incentives.incentives_geoms;

	CREATE INDEX incentives_geoms_the_geom_gist ON dg_wind.incentives_geoms_copy USING gist(the_geom);

	VACUUM ANALYZE dg_wind.incentives_geoms_copy;

	-- dice it up to allow for faster intersects
	CREATE TABLE dg_wind.incentives_geoms_copy_diced AS
	SELECT a.gid, ST_Intersection(a.the_geom, b.the_geom_4326) as the_geom
	FROM dg_wind.incentives_geoms_copy a
	INNER JOIN mgleason.conus_fishnet_2dd b
	ON ST_Intersects(a.the_geom, b.the_geom_4326);

	CREATE INDEX incentives_geoms_copy_diced_the_geom_gist ON dg_wind.incentives_geoms_copy_diced USING gist(the_geom);

	VACUUM ANALYZE dg_wind.incentives_geoms_copy_diced;

	-- WIND
	--create the lookup table		
	DROP TABLE IF EXISTS diffusion_wind.dsire_incentives_lookup_res;
	CREATE TABLE diffusion_wind.dsire_incentives_lookup_res AS

	with a as 
	(
		SELECT b.gid, b.the_geom, d.uid as wind_incentives_uid
		FROM dg_wind.incentives_geoms_copy_diced b
		inner JOIN geo_incentives.incentives c
		ON b.gid = c.geom_id
		INNER JOIN geo_incentives.wind_incentives d
		ON c.gid = d.incentive_id
	)

	SELECT e.gid as pt_gid, a.wind_incentives_uid
	FROM a

	INNER JOIN diffusion_shared.pt_grid_us_res e
	ON ST_Intersects(a.the_geom,e.the_geom_4326);

	CREATE INDEX dsire_incentives_lookup_res_pt_gid_btree ON diffusion_wind.dsire_incentives_lookup_res using btree(pt_gid);

	SELECT pt_gid, count(*)
	FROM diffusion_wind.dsire_incentives_lookup_res
	GROUP BY pt_gid
	ORDER by count desc;

	-- group the incentives into arrays so that there is just one row for each pt_gid
	DROP TABLE IF EXISTS diffusion_wind_data.dsire_incentives_combos_lookup_res;
	CREATE TABLE diffusion_wind_data.dsire_incentives_combos_lookup_res AS
	SELECT pt_gid, array_agg(wind_incentives_uid order by wind_incentives_uid) as wind_incentives_uid_array
	FROM diffusion_wind.dsire_incentives_lookup_res
	group by pt_gid;

	-- find the unique set of incentive arrays
	DROP TABLE IF EXISTS diffusion_wind_data.dsire_incentives_unique_combos_res;
	CREATE TABLE diffusion_wind_data.dsire_incentives_unique_combos_res AS
	SELECT distinct(wind_incentives_uid_array) as wind_incentives_uid_array
	FROM diffusion_wind_data.dsire_incentives_combos_lookup_res;

	-- add a primary key to the table of incentive arrays
	ALTER TABLE diffusion_wind_data.dsire_incentives_unique_combos_res
	ADD column incentive_array_id serial primary key;

	-- join the incentive array primary key back into the combos_lookup_table
	ALTER TABLE diffusion_wind_data.dsire_incentives_combos_lookup_res
	ADD column incentive_array_id integer;

	UPDATE diffusion_wind_data.dsire_incentives_combos_lookup_res a
	SET incentive_array_id = b.incentive_array_id
	FROM diffusion_wind_data.dsire_incentives_unique_combos_res b
	where a.wind_incentives_uid_array = b.wind_incentives_uid_array;

	-- join this info back into the main points table
	ALTER TABLE diffusion_shared.pt_grid_us_res
	ADD COLUMN wind_incentive_array_id integer;

	UPDATE diffusion_shared.pt_grid_us_res a
	SET wind_incentive_array_id = b.incentive_array_id
	FROM diffusion_wind_data.dsire_incentives_combos_lookup_res b
	WHere a.gid = b.pt_gid;
	
	-- add an index
	CREATE INDEX pt_grid_us_res_wind_incentive_btree 
	ON diffusion_shared.pt_grid_us_res
	USING btree(wind_incentive_array_id);

	-- check that we got tem all
	SELECT count(*)
	FROM diffusion_shared.pt_grid_us_res
	where wind_incentive_array_id is not null;
	--6273172

	SELECT count(*)
	FROM diffusion_wind_data.dsire_incentives_combos_lookup_res
	where incentive_array_id is not null;
	--6273172

	--unnest the data from the unique combos table
	DROP TABLE IF EXISTS diffusion_wind.dsire_incentives_simplified_lkup_res;
	CREATE TABLE diffusion_wind.dsire_incentives_simplified_lkup_res AS
	SELECT incentive_array_id as incentive_array_id, 
		unnest(wind_incentives_uid_array) as incentives_uid
	FROM diffusion_wind_data.dsire_incentives_unique_combos_res;

	-- create index
	CREATE INDEX dsire_incentives_simplified_lkup_res_inc_id_btree
	ON diffusion_wind.dsire_incentives_simplified_lkup_res
	USING btree(incentive_array_id);

	-- SOLAR
	--create the lookup table		
	DROP TABLE IF EXISTS diffusion_solar.dsire_incentives_lookup_res;
	CREATE TABLE diffusion_solar.dsire_incentives_lookup_res AS

	with a as 
	(
		SELECT b.gid, b.the_geom, d.uid as solar_incentives_uid
		FROM dg_wind.incentives_geoms_copy_diced b
		inner JOIN geo_incentives.incentives c
		ON b.gid = c.geom_id
		INNER JOIN geo_incentives.pv_incentives d
		ON c.gid = d.incentive_id
	)

	SELECT e.gid as pt_gid, a.solar_incentives_uid
	FROM a

	INNER JOIN diffusion_shared.pt_grid_us_res e
	ON ST_Intersects(a.the_geom,e.the_geom_4326);

	CREATE INDEX dsire_incentives_lookup_res_pt_gid_btree ON diffusion_solar.dsire_incentives_lookup_res using btree(pt_gid);

	SELECT pt_gid, count(*)
	FROM diffusion_solar.dsire_incentives_lookup_res
	GROUP BY pt_gid
	ORDER by count desc;

	-- group the incentives into arrays so that there is just one row for each pt_gid
	DROP TABLE IF EXISTS diffusion_solar_data.dsire_incentives_combos_lookup_res;
	CREATE TABLE diffusion_solar_data.dsire_incentives_combos_lookup_res AS
	SELECT pt_gid, array_agg(solar_incentives_uid order by solar_incentives_uid) as solar_incentives_uid_array
	FROM diffusion_solar.dsire_incentives_lookup_res
	group by pt_gid;

	-- find the unique set of incentive arrays
	DROP TABLE IF EXISTS diffusion_solar_data.dsire_incentives_unique_combos_res;
	CREATE TABLE diffusion_solar_data.dsire_incentives_unique_combos_res AS
	SELECT distinct(solar_incentives_uid_array) as solar_incentives_uid_array
	FROM diffusion_solar_data.dsire_incentives_combos_lookup_res;

	-- add a primary key to the table of incentive arrays
	ALTER TABLE diffusion_solar_data.dsire_incentives_unique_combos_res
	ADD column incentive_array_id serial primary key;

	-- join the incentive array primary key back into the combos_lookup_table
	ALTER TABLE diffusion_solar_data.dsire_incentives_combos_lookup_res
	ADD column incentive_array_id integer;

	UPDATE diffusion_solar_data.dsire_incentives_combos_lookup_res a
	SET incentive_array_id = b.incentive_array_id
	FROM diffusion_solar_data.dsire_incentives_unique_combos_res b
	where a.solar_incentives_uid_array = b.solar_incentives_uid_array;

	-- join this info back into the main points table
	ALTER TABLE diffusion_shared.pt_grid_us_res
	ADD COLUMN solar_incentive_array_id integer;

	UPDATE diffusion_shared.pt_grid_us_res a
	SET solar_incentive_array_id = b.incentive_array_id
	FROM diffusion_solar_data.dsire_incentives_combos_lookup_res b
	WHere a.gid = b.pt_gid;
	
	-- add an index
	CREATE INDEX pt_grid_us_res_solar_incentive_btree 
	ON diffusion_shared.pt_grid_us_res
	USING btree(solar_incentive_array_id);

	-- check that we got tem all
	SELECT count(*)
	FROM diffusion_shared.pt_grid_us_res
	where solar_incentive_array_id is not null;
	--6273172

	SELECT count(*)
	FROM diffusion_solar_data.dsire_incentives_combos_lookup_res
	where incentive_array_id is not null;
	--6273172

	--unnest the data from the unique combos table
	DROP TABLE IF EXISTS diffusion_solar.dsire_incentives_simplified_lkup_res;
	CREATE TABLE diffusion_solar.dsire_incentives_simplified_lkup_res AS
	SELECT incentive_array_id as incentive_array_id, 
		unnest(solar_incentives_uid_array) as incentives_uid
	FROM diffusion_solar_data.dsire_incentives_unique_combos_res;

	-- create index
	CREATE INDEX dsire_incentives_simplified_lkup_res_inc_id_btree
	ON diffusion_solar.dsire_incentives_simplified_lkup_res
	USING btree(incentive_array_id);	

	


-- test county level access
SELECT county_id,sum(annual_rate_gid)
FROM diffusion_shared.pt_grid_us_res
group by county_id; --10,000 - 13000 ms



-- add pca region
DROP TABLE IF EXISTS  diffusion_wind_data.pt_grid_us_res_pca_reg_lookup;
CREATE TABLE  diffusion_wind_data.pt_grid_us_res_pca_reg_lookup (
	gid integer,
	pca_reg integer,
	reeds_reg integer);

SELECT parsel_2('dav-gis','mgleason','mgleason','diffusion_shared.pt_grid_us_res','gid',
		'SELECT a.gid, b.pca_reg, b.demreg as reeds_reg
		FROM  diffusion_shared.pt_grid_us_res a
		INNER JOIN reeds.reeds_regions b
		ON ST_Intersects(a.the_geom_4326,b.the_geom)
		WHERE b.pca_reg NOT IN (135,136);',
	' diffusion_wind_data.pt_grid_us_res_pca_reg_lookup', 'a',16);

-- join the info back in
ALTER TABLE diffusion_shared.pt_grid_us_res 
ADD COLUMN pca_reg integer,
ADD COLUMN reeds_reg integer;

CREATE INDEX pt_grid_us_res_pca_reg_lookup_gid_btree ON  diffusion_wind_data.pt_grid_us_res_pca_reg_lookup using btree(gid);

UPDATE diffusion_shared.pt_grid_us_res a
SET (pca_reg,reeds_reg) = (b.pca_reg,b.reeds_reg)
FROM  diffusion_wind_data.pt_grid_us_res_pca_reg_lookup b
where a.gid = b.gid;

-- how many are null?
CREATE INDEX pt_grid_us_res_pca_reg_btree ON diffusion_shared.pt_grid_us_res USING btree(pca_reg);
CREATE INDEX pt_grid_us_res_reeds_reg_btree ON diffusion_shared.pt_grid_us_res USING btree(reeds_reg);

-- any missing?
select count(*)
FROM diffusion_shared.pt_grid_us_res 
where pca_reg is null or reeds_reg is null;
--6208

select count(*)
FROM diffusion_shared.pt_grid_us_res 
where pca_reg is null;
-- 6208

select count(*)
FROM diffusion_shared.pt_grid_us_res 
where reeds_reg is null;
-- 6208

-- fix the missing based on the closest
DROP TABLE IF EXISTS  diffusion_wind_data.pt_grid_us_res_pca_reg_missing_lookup;
CREATE TABLE  diffusion_wind_data.pt_grid_us_res_pca_reg_missing_lookup AS
with a AS(
	select gid, the_geom_900914
	FROM diffusion_shared.pt_grid_us_res
	where pca_reg is null),
b as (
	SELECT a.gid, a.the_geom_900914, 
		(SELECT b.gid
		 FROM diffusion_shared.pt_grid_us_res b
		 where b.pca_reg is not null
		 ORDER BY a.the_geom_900914 <#> b.the_geom_900914
		 LIMIT 1) as nn_gid
	FROM a)
SELECT b.gid, b.the_geom_900914, b.nn_gid, c.pca_reg, c.reeds_reg
from b
LEFT JOIN diffusion_shared.pt_grid_us_res c
ON b.nn_gid = c.gid;
-- **
  
--update the points table
UPDATE diffusion_shared.pt_grid_us_res a
SET (pca_reg,reeds_reg) = (b.pca_reg,b.reeds_reg)
FROM  diffusion_wind_data.pt_grid_us_res_pca_reg_missing_lookup b
where a.gid = b.gid
and a.pca_reg is null;
 
-- check for any remaining nulls?
select count(*)
FROM diffusion_shared.pt_grid_us_res 
where pca_reg is null or reeds_reg is null;


-- add nsrdb grid gids
DROP TABLE IF EXISTS  diffusion_solar_data.pt_grid_us_res_solar_re_9809_lookup;
CREATE TABLE  diffusion_solar_data.pt_grid_us_res_solar_re_9809_lookup 
(
	gid integer,
	solar_re_9809_gid integer
);


SELECT parsel_2('dav-gis','mgleason','mgleason','diffusion_shared.pt_grid_us_res','gid',
		'SELECT a.gid, b.gid as solar_re_9809_gid
		FROM  diffusion_shared.pt_grid_us_res a
		INNER JOIN solar.solar_re_9809 b
		ON ST_Intersects(a.the_geom_4326,b.the_geom_4326);',
	'diffusion_solar_data.pt_grid_us_res_solar_re_9809_lookup', 'a',16);

-- join the info back in
ALTER TABLE diffusion_shared.pt_grid_us_res 
ADD COLUMN solar_re_9809_gid integer;

CREATE INDEX pt_grid_us_res_solar_re_9809_lookup_git_btree 
ON  diffusion_solar_data.pt_grid_us_res_solar_re_9809_lookup 
using btree(gid);

UPDATE diffusion_shared.pt_grid_us_res a
SET solar_re_9809_gid = b.solar_re_9809_gid
FROM  diffusion_solar_data.pt_grid_us_res_solar_re_9809_lookup  b
where a.gid = b.gid;

-- how many are null?
CREATE INDEX pt_grid_us_res_solar_re_9809_gid_btree ON diffusion_shared.pt_grid_us_res USING btree(solar_re_9809_gid);

-- any missing?
select count(*)
FROM diffusion_shared.pt_grid_us_res 
where solar_re_9809_gid is null;
--

-- fix the missing based on the closest
DROP TABLE IF EXISTS  diffusion_solar_data.pt_grid_us_solar_re_9809_gid_missing_lookup;
CREATE TABLE  diffusion_solar_data.pt_grid_us_solar_re_9809_gid_missing_lookup AS
with a AS(
	select gid, the_geom_900914
	FROM diffusion_shared.pt_grid_us_res
	where solar_re_9809_gid is null)
SELECT a.gid, a.the_geom_900914, 
	(SELECT b.solar_re_9809_gid
	 FROM diffusion_shared.pt_grid_us_res b
	 where b.solar_re_9809_gid is not null
	 ORDER BY a.the_geom_900914 <#> b.the_geom_900914
	 LIMIT 1) as solar_re_9809_gid
	FROM a;
-- 
  
--update the points table
UPDATE diffusion_shared.pt_grid_us_res a
SET solar_re_9809_gid = b.solar_re_9809_gid
FROM  diffusion_solar_data.pt_grid_us_solar_re_9809_gid_missing_lookup b
where a.gid = b.gid
and a.solar_re_9809_gid is null;
 
-- check for any remaining nulls?
select count(*)
FROM diffusion_shared.pt_grid_us_res 
where solar_re_9809_gid is null;



-- add foreign keys
	-- for county_id to county_geom.county id
ALTER TABLE diffusion_shared.pt_grid_us_res ADD CONSTRAINT county_id_fkey FOREIGN KEY (county_id) 
REFERENCES diffusion_shared.county_geom (county_id) MATCH FULL 
ON UPDATE RESTRICT ON DELETE RESTRICT;
	-- for iiijjjicf_id to iiijjjicf_lookup.id
ALTER TABLE diffusion_shared.pt_grid_us_res ADD CONSTRAINT iiijjjicf_id_fkey FOREIGN KEY (iiijjjicf_id) 
REFERENCES diffusion_wind.iiijjjicf_lookup (id) MATCH FULL 
ON UPDATE RESTRICT ON DELETE RESTRICT;
	-- for dsire_incentives_lookup_res.pt_gid to pt_grid_us_res.gid
ALTER TABLE diffusion_wind.dsire_incentives_lookup_res ADD CONSTRAINT pt_gid_fkey FOREIGN KEY (pt_gid) 
REFERENCES diffusion_shared.pt_grid_us_res (gid) MATCH FULL 
ON UPDATE RESTRICT ON DELETE RESTRICT;
	-- for dsire_incentives_lookup_res.wind_incentives_uid to geo_incentives.wind_incentives.uid
ALTER TABLE diffusion_wind.dsire_incentives_lookup_res ADD CONSTRAINT wind_incentives_uid_fkey FOREIGN KEY (wind_incentives_uid) 
REFERENCES geo_incentives.wind_incentives (uid) MATCH FULL 
ON UPDATE RESTRICT ON DELETE RESTRICT;
	-- for annual_rate_gid to annual_ave_elec_rates_2011.gid
-- ALTER TABLE diffusion_shared.pt_grid_us_res DROP CONSTRAINT annual_rate_gid_fkey;
ALTER TABLE diffusion_shared.pt_grid_us_res ADD CONSTRAINT annual_rate_gid_fkey FOREIGN KEY (annual_rate_gid) 
REFERENCES diffusion_shared.annual_ave_elec_rates_2011 (gid) MATCH FULL 
ON UPDATE RESTRICT ON DELETE RESTRICT;




