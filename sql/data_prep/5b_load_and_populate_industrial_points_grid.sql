-- DROP TABLE IF EXISTS wind_ds.pt_grid_us_ind;
CREATE TABLE wind_ds.pt_grid_us_ind (
	x numeric,
	y numeric, 
	temp_col integer);

SET ROLE "server-superusers";

COPY wind_ds.pt_grid_us_ind FROM '/srv/home/mgleason/data/dg_wind/ind_points_200m_us.csv' with csv header;

RESET ROLE;

-- change to integers
ALTER TABLE wind_ds.pt_grid_us_ind ALTER x TYPE INTEGER;
ALTER TABLE wind_ds.pt_grid_us_ind ALTER y TYPE INTEGER;
-- drop this column -- it means nothing
ALTER TABLE wind_ds.pt_grid_us_ind DROP COLUMN temp_col;

ALTER TABLE wind_ds.pt_grid_us_ind ADD COLUMN the_geom_900914 geometry;

UPDATE wind_ds.pt_grid_us_ind
SET the_geom_900914 = ST_SetSRID(ST_MakePoint(x,y),900914);

CREATE INDEX pt_grid_us_ind_the_geom_900914_gist ON wind_ds.pt_grid_us_ind USING gist(the_geom_900914);
CLUSTER wind_ds.pt_grid_us_ind USING pt_grid_us_ind_the_geom_900914_gist;

VACUUM ANALYZE wind_ds.pt_grid_us_ind;

-- add:
-- gid (serial) and pkey
ALTER TABLE wind_ds.pt_grid_us_ind ADD COLUMN gid serial;
ALTER TABLE wind_ds.pt_grid_us_ind ADD PRIMARY KEY (gid);
-- the_geom_4326 with index
ALTER TABLE wind_ds.pt_grid_us_ind ADD COLUMN the_geom_4326 geometry;

UPDATE wind_ds.pt_grid_us_ind
SET the_geom_4326 = ST_Transform(the_geom_900914,4326);

CREATE INDEX pt_grid_us_ind_the_geom_4326_gist ON wind_ds.pt_grid_us_ind USING gist(the_geom_4326);

-- county id (from polygons) and index (foreign key)?
ALTER TABLE wind_ds.pt_grid_us_ind ADD COLUMN county_id integer;

UPDATE wind_ds.pt_grid_us_ind a
SET county_id = b.county_id
FROM wind_ds.county_geom b
WHERE ST_Intersects(a.the_geom_900914, b.the_geom_900914);

CREATE INDEX pt_grid_us_ind_county_id_btree ON wind_ds.pt_grid_us_ind using btree(county_id);

-- check why there are nulls
DROP TABLE IF EXISTS wind_ds_data.no_county_pts_ind;
CREATE TABLE wind_ds_data.no_county_pts_ind AS
SELECT gid, the_geom_4326
FROM wind_ds.pt_grid_us_ind
where county_id is null;
-- 2147  rows
-- inspect in Q -- all around the edges of the country

-- pick county based on nearest
CREATE TABLE wind_ds_data.no_county_pts_ind_closest AS
with candidates as (

SELECT a.gid, a.the_geom_900914, 
	unnest((select array(SELECT b.county_id
	 FROM wind_ds.county_geom b
	 ORDER BY a.the_geom_900914 <#> b.the_geom_900914 LIMIT 5))) as county_id
FROM wind_ds.pt_grid_us_ind a
where a.county_id is null
 )

SELECT distinct ON (gid) a.gid, a.the_geom_900914, a.county_id, b.county
FROM candidates a
lEFT JOIN wind_ds.county_geom b
ON a.county_id = b.county_id
ORDER BY gid, ST_Distance(a.the_geom_900914,b.the_geom_900914) asc;
-- inspect in Q

-- update the main table
UPDATE wind_ds.pt_grid_us_ind a
SET county_id = b.county_id
FROM wind_ds_data.no_county_pts_ind_closest b
WHERE a.county_id is null
and a.gid = b.gid;

-- make sure no more nulls remain
SELECT * 
FROM wind_ds.pt_grid_us_ind
where county_id is null;

-- drop the other tables
-- DROP TABLE IF EXISTS wind_ds_data.no_county_pts_ind;
-- DROP TABLE IF EXISTS wind_ds_data.no_county_pts_ind_closest;

-- utility type
DROP TABLE IF EXISTS wind_ds_data.pt_grid_us_ind_utiltype_lookup;
CREATE TABLE wind_ds_data.pt_grid_us_ind_utiltype_lookup (
	gid integer,
	utility_type character varying(9));

	select parsel_2('dav-gis','wind_ds.pt_grid_us_ind','gid',
	'WITH ut_ranks as (
		SELECT unnest(array[''IOU'',''Muni'',''Coop'',''All Other'']) as utility_type, generate_series(1,4) as rank
	),
	isect as (
		SELECT a.gid, b.company_type_general as utility_type, c.rank
		FROM wind_ds.pt_grid_us_ind a
		INNER JOIN dg_wind.ventyx_elec_serv_territories_edit_diced b
		ON ST_Intersects(a.the_geom_4326, b.the_geom_4326)
		LEFT JOIN ut_ranks c
		ON b.company_type_general = c.utility_type)
	SELECT DISTINCT ON (a.gid) a.gid, a.utility_type 
	FROM isect a
	ORDER BY a.gid, a.rank ASC;','wind_ds_data.pt_grid_us_ind_utiltype_lookup', 'a', 16);

	-- join the info back in
	ALTER TABLE wind_ds.pt_grid_us_ind ADD COLUMN utility_type character varying(9);

	CREATE INDEX pt_grid_us_ind_utiltype_lookup_gid_btree ON wind_ds_data.pt_grid_us_ind_utiltype_lookup using btree(gid);

	UPDATE wind_ds.pt_grid_us_ind a
	SET utility_type = b.utility_type
	FROM wind_ds_data.pt_grid_us_ind_utiltype_lookup b
	where a.gid = b.gid;
	
	CREATE INDEX pt_grid_us_ind_utility_type_btree ON wind_ds.pt_grid_us_ind USING btree(utility_type);
	
	-- are there any nulls?
	SELECT count(*) 
	FROM wind_ds.pt_grid_us_ind
	where utility_type is null;

	-- isolate the unjoined points
	-- and fix them by assigning value from their nearest neighbor that is not null
	DROP TABLE IF EXISTS wind_ds_data.pt_grid_us_ind_utiltype_missing;
	CREATE TABLE wind_ds_data.pt_grid_us_ind_utiltype_missing AS
	with a AS(
		select gid, the_geom_900914
		FROM wind_ds.pt_grid_us_ind
		where utility_type is null)
	SELECT a.gid, a.the_geom_900914, 
		(SELECT b.utility_type 
		 FROM wind_ds.pt_grid_us_ind b
		 where b.utility_type is not null
		 ORDER BY a.the_geom_900914 <#> b.the_geom_900914
		 LIMIT 1) as utility_type
	FROM a;

	--update the points table
	UPDATE wind_ds.pt_grid_us_ind a
	SET utility_type = b.utility_type
	FROM wind_ds_data.pt_grid_us_ind_utiltype_missing b
	where a.gid = b.gid
	and a.utility_type is null;

	-- any nulls left?
	SELECT count(*) 
	FROM wind_ds.pt_grid_us_ind
	where utility_type is null;




-- iii, jjj, icf (from raster)
DROP TABLE IF EXISTS wind_ds_data.pt_grid_us_ind_iiijjjicf_id_lookup;
CREATE TABLE wind_ds_data.pt_grid_us_ind_iiijjjicf_id_lookup (
	gid integer,
	iiijjjicf_id integer);

	--run in parallel for speed (100x100 tiles are necessary for it ti finishin in about 7 mins -- 1000x1000 tiles would take several hours even in parallel)
	SELECT parsel_2('dav-gis','wind_ds.pt_grid_us_ind','gid',
	'SELECT a.gid, ST_Value(b.rast,a.the_geom_900914) as iiijjjicf_id
	FROM  wind_ds.pt_grid_us_ind a
	INNER JOIN wind_ds_data.iiijjjicf_us_100x100 b
	ON ST_Intersects(b.rast,a.the_geom_900914);',
		'wind_ds_data.pt_grid_us_ind_iiijjjicf_id_lookup', 'a',16);

	-- join the info back in
	ALTER TABLE wind_ds.pt_grid_us_ind ADD COLUMN iiijjjicf_id integer;

	CREATE INDEX pt_grid_us_ind_iiijjjicf_id_lookup_gid_btree ON wind_ds_data.pt_grid_us_ind_iiijjjicf_id_lookup using btree(gid);

	UPDATE wind_ds.pt_grid_us_ind a
	SET iiijjjicf_id = b.iiijjjicf_id
	FROM wind_ds_data.pt_grid_us_ind_iiijjjicf_id_lookup b
	where a.gid = b.gid;

	CREATE INDEX pt_grid_us_ind_iiijjjicf_id_btree ON wind_ds.pt_grid_us_ind USING btree(iiijjjicf_id);

	-- check for points with no iiijjjicf
	SELECT count(*)
	FROM wind_ds.pt_grid_us_ind
	where iiijjjicf_id is null;

	-- isolate the unjoined points
	-- and fix them by assigning value from their nearest neighbor that is not null
	DROP TABLE IF EXISTS wind_ds_data.pt_grid_us_ind_iiijjjicf_id_lookup_no_id;
	CREATE TABLE wind_ds_data.pt_grid_us_ind_iiijjjicf_id_lookup_no_id AS
	with a AS(
		select gid, the_geom_900914
		FROM wind_ds.pt_grid_us_ind
		where iiijjjicf_id is null)
	SELECT a.gid, a.the_geom_900914, 
		(SELECT b.iiijjjicf_id 
		 FROM wind_ds.pt_grid_us_ind b
		 where b.iiijjjicf_id is not null
		 ORDER BY a.the_geom_900914 <#> b.the_geom_900914
		 LIMIT 1) as iiijjjicf_id
	FROM a;
	-- inspect in Q

	--update the lookup table
	UPDATE wind_ds_data.pt_grid_us_ind_iiijjjicf_id_lookup a
	SET iiijjjicf_id = b.iiijjjicf_id
	FROM wind_ds_data.pt_grid_us_ind_iiijjjicf_id_lookup_no_id b
	where a.gid = b.gid
	and a.iiijjjicf_id is null;

	--update the points table
	UPDATE wind_ds.pt_grid_us_ind a
	SET iiijjjicf_id = b.iiijjjicf_id
	FROM wind_ds_data.pt_grid_us_ind_iiijjjicf_id_lookup b
	where a.gid = b.gid
	and a.iiijjjicf_id is null;

	-- check no nulls remain
	SELECT *
	FROM wind_ds.pt_grid_us_ind
	where iiijjjicf_id is null; 


-- 3 different versions of exclusions (from rasters)
-- population density only
	DROP TABLE IF EXISTS wind_ds_data.pt_grid_us_ind_maxheight_popdens_lookup;
	CREATE TABLE wind_ds_data.pt_grid_us_ind_maxheight_popdens_lookup (
		gid integer,
		maxheight_m_popdens integer);

	--run in parallel for speed (100x100 tiles are necessary for it ti finishin in about 7 mins -- 1000x1000 tiles would take several hours even in parallel)
	SELECT parsel_2('dav-gis','wind_ds.pt_grid_us_ind','gid',
			'SELECT a.gid, ST_Value(b.rast,a.the_geom_900914) as maxheight_m_popdens
			FROM  wind_ds.pt_grid_us_ind a
			INNER JOIN wind_ds_data.maxheight_popdens_us_100x100 b
			ON ST_Intersects(b.rast,a.the_geom_900914);',
		'wind_ds_data.pt_grid_us_ind_maxheight_popdens_lookup', 'a',16);

	-- join the info back in
	ALTER TABLE wind_ds.pt_grid_us_ind ADD COLUMN maxheight_m_popdens integer;

	CREATE INDEX pt_grid_us_ind_maxheight_popdens_lookup_gid_btree ON wind_ds_data.pt_grid_us_ind_maxheight_popdens_lookup using btree(gid);

	UPDATE wind_ds.pt_grid_us_ind a
	SET maxheight_m_popdens = b.maxheight_m_popdens
	FROM wind_ds_data.pt_grid_us_ind_maxheight_popdens_lookup b
	where a.gid = b.gid;

	-- how many are null?
	CREATE INDEX pt_grid_us_ind_maxheight_m_popdens ON wind_ds.pt_grid_us_ind USING btree(maxheight_m_popdens) where maxheight_m_popdens is null;

	select count(*)
	FROM wind_ds.pt_grid_us_ind 
	where maxheight_m_popdens is null;
	
	-- isolate the unjoined points
	-- and fix them by assigning value from their nearest neighbor that is not null
	DROP TABLE IF EXISTS wind_ds_data.pt_grid_us_ind_maxheight_m_popdens_missing_lookup;
	CREATE TABLE wind_ds_data.pt_grid_us_ind_maxheight_m_popdens_missing_lookup AS
	with a AS(
		select gid, the_geom_900914
		FROM wind_ds.pt_grid_us_ind
		where maxheight_m_popdens is null)
	SELECT a.gid, a.the_geom_900914, 
		(SELECT b.maxheight_m_popdens 
		 FROM wind_ds.pt_grid_us_ind b
		 where b.maxheight_m_popdens is not null
		 ORDER BY a.the_geom_900914 <#> b.the_geom_900914
		 LIMIT 1) as maxheight_m_popdens
	FROM a;

	--update the lookup table
	UPDATE wind_ds_data.pt_grid_us_ind_maxheight_popdens_lookup a
	SET maxheight_m_popdens = b.maxheight_m_popdens
	FROM wind_ds_data.pt_grid_us_ind_maxheight_m_popdens_missing_lookup b
	where a.gid = b.gid
	and a.maxheight_m_popdens is null;

	--update the points table
	UPDATE wind_ds.pt_grid_us_ind a
	SET maxheight_m_popdens = b.maxheight_m_popdens
	FROM wind_ds_data.pt_grid_us_ind_maxheight_popdens_lookup b
	where a.gid = b.gid
	and a.maxheight_m_popdens is null;
 
	-- check for any remaining nulls?
	select count(*)
	FROM wind_ds.pt_grid_us_ind 
	where maxheight_m_popdens is null;

-- population density and 20 pc canopy cover
	DROP TABLE IF EXISTS wind_ds_data.pt_grid_us_ind_maxheight_popdenscancov20pc_lookup;
	CREATE TABLE wind_ds_data.pt_grid_us_ind_maxheight_popdenscancov20pc_lookup (
		gid integer,
		maxheight_m_popdenscancov20pc integer);

	--run in parallel for speed (100x100 tiles are necessary for it ti finishin in about 7 mins -- 1000x1000 tiles would take several hours even in parallel)
	SELECT parsel_2('dav-gis','wind_ds.pt_grid_us_ind','gid',
			'SELECT a.gid, ST_Value(b.rast,a.the_geom_900914) as maxheight_m_popdenscancov20pc
			FROM  wind_ds.pt_grid_us_ind a
			INNER JOIN wind_ds_data.maxheight_popdenscancov20pc_us_100x100 b
			ON ST_Intersects(b.rast,a.the_geom_900914);',
		'wind_ds_data.pt_grid_us_ind_maxheight_popdenscancov20pc_lookup', 'a',16);

	-- join the info back in

	ALTER TABLE wind_ds.pt_grid_us_ind ADD COLUMN maxheight_m_popdenscancov20pc integer;

	CREATE INDEX pt_grid_us_ind_maxheight_popdenscancov20pc_lookup_gid_btree ON wind_ds_data.pt_grid_us_ind_maxheight_popdenscancov20pc_lookup using btree(gid);

	UPDATE wind_ds.pt_grid_us_ind a
	SET maxheight_m_popdenscancov20pc = b.maxheight_m_popdenscancov20pc
	FROM wind_ds_data.pt_grid_us_ind_maxheight_popdenscancov20pc_lookup b
	where a.gid = b.gid;

	-- how many are null?
	CREATE INDEX pt_grid_us_ind_maxheight_m_popdenscancov20pc_btree ON wind_ds.pt_grid_us_ind USING btree(maxheight_m_popdenscancov20pc) where maxheight_m_popdenscancov20pc is null;

	select count(*)
	FROM wind_ds.pt_grid_us_ind 
	where maxheight_m_popdenscancov20pc is null;
	
	-- isolate the unjoined points
	-- and fix them by assigning value from their nearest neighbor that is not null
	DROP TABLE IF EXISTS wind_ds_data.pt_grid_us_ind_maxheight_popdenscancov20pc_missing_lookup;
	CREATE TABLE wind_ds_data.pt_grid_us_ind_maxheight_popdenscancov20pc_missing_lookup AS
	with a AS(
		select gid, the_geom_900914
		FROM wind_ds.pt_grid_us_ind
		where maxheight_m_popdenscancov20pc is null)
	SELECT a.gid, a.the_geom_900914, 
		(SELECT b.maxheight_m_popdenscancov20pc 
		 FROM wind_ds.pt_grid_us_ind b
		 where b.maxheight_m_popdenscancov20pc is not null
		 ORDER BY a.the_geom_900914 <#> b.the_geom_900914
		 LIMIT 1) as maxheight_m_popdenscancov20pc
	FROM a;

	--update the lookup table
	UPDATE wind_ds_data.pt_grid_us_ind_maxheight_popdenscancov20pc_lookup a
	SET maxheight_m_popdenscancov20pc = b.maxheight_m_popdenscancov20pc
	FROM wind_ds_data.pt_grid_us_ind_maxheight_popdenscancov20pc_missing_lookup b
	where a.gid = b.gid
	and a.maxheight_m_popdenscancov20pc is null;

	--update the points table
	UPDATE wind_ds.pt_grid_us_ind a
	SET maxheight_m_popdenscancov20pc = b.maxheight_m_popdenscancov20pc
	FROM wind_ds_data.pt_grid_us_ind_maxheight_popdenscancov20pc_lookup b
	where a.gid = b.gid
	and a.maxheight_m_popdenscancov20pc is null;
 
	-- check for any remaining nulls?
	select count(*)
	FROM wind_ds.pt_grid_us_ind 
	where maxheight_m_popdenscancov20pc is null;

-- population density and 40 pc canopy cover
	DROP TABLE IF EXISTS wind_ds_data.pt_grid_us_ind_maxheight_popdenscancov40pc_lookup;
	CREATE TABLE wind_ds_data.pt_grid_us_ind_maxheight_popdenscancov40pc_lookup (
		gid integer,
		maxheight_m_popdenscancov40pc integer);

	--run in parallel for speed (100x100 tiles are necessary for it ti finishin in about 7 mins -- 1000x1000 tiles would take several hours even in parallel)
	SELECT parsel_2('dav-gis','wind_ds.pt_grid_us_ind','gid',
			'SELECT a.gid, ST_Value(b.rast,a.the_geom_900914) as maxheight_m_popdenscancov40pc
			FROM  wind_ds.pt_grid_us_ind a
			INNER JOIN wind_ds_data.maxheight_popdenscancov40pc_us_100x100 b
			ON ST_Intersects(b.rast,a.the_geom_900914);',
		'wind_ds_data.pt_grid_us_ind_maxheight_popdenscancov40pc_lookup', 'a',16);

	-- join the info back in
	ALTER TABLE wind_ds.pt_grid_us_ind ADD COLUMN maxheight_m_popdenscancov40pc integer;

	CREATE INDEX pt_grid_us_ind_maxheight_popdenscancov40pc_lookup_gid_btree ON wind_ds_data.pt_grid_us_ind_maxheight_popdenscancov40pc_lookup using btree(gid);

	UPDATE wind_ds.pt_grid_us_ind a
	SET maxheight_m_popdenscancov40pc = b.maxheight_m_popdenscancov40pc
	FROM wind_ds_data.pt_grid_us_ind_maxheight_popdenscancov40pc_lookup b
	where a.gid = b.gid;

	-- how many are null?
	CREATE INDEX pt_grid_us_ind_maxheight_m_popdenscancov40pc_btree ON wind_ds.pt_grid_us_ind USING btree(maxheight_m_popdenscancov40pc) where maxheight_m_popdenscancov40pc is null;

	select count(*)
	FROM wind_ds.pt_grid_us_ind 
	where maxheight_m_popdenscancov40pc is null;
	
	-- isolate the unjoined points
	-- and fix them by assigning value from their nearest neighbor that is not null
	DROP TABLE IF EXISTS wind_ds_data.pt_grid_us_ind_maxheight_popdenscancov40pc_missing_lookup;
	CREATE TABLE wind_ds_data.pt_grid_us_ind_maxheight_popdenscancov40pc_missing_lookup AS
	with a AS(
		select gid, the_geom_900914
		FROM wind_ds.pt_grid_us_ind
		where maxheight_m_popdenscancov40pc is null)
	SELECT a.gid, a.the_geom_900914, 
		(SELECT b.maxheight_m_popdenscancov40pc 
		 FROM wind_ds.pt_grid_us_ind b
		 where b.maxheight_m_popdenscancov40pc is not null
		 ORDER BY a.the_geom_900914 <#> b.the_geom_900914
		 LIMIT 1) as maxheight_m_popdenscancov40pc
	FROM a;

	--update the lookup table
	UPDATE wind_ds_data.pt_grid_us_ind_maxheight_popdenscancov40pc_lookup a
	SET maxheight_m_popdenscancov40pc = b.maxheight_m_popdenscancov40pc
	FROM wind_ds_data.pt_grid_us_ind_maxheight_popdenscancov40pc_missing_lookup b
	where a.gid = b.gid
	and a.maxheight_m_popdenscancov40pc is null;

	--update the points table
	UPDATE wind_ds.pt_grid_us_ind a
	SET maxheight_m_popdenscancov40pc = b.maxheight_m_popdenscancov40pc
	FROM wind_ds_data.pt_grid_us_ind_maxheight_popdenscancov40pc_lookup b
	where a.gid = b.gid
	and a.maxheight_m_popdenscancov40pc is null;
 
	-- check for any remaining nulls?
	select count(*)
	FROM wind_ds.pt_grid_us_ind 
	where maxheight_m_popdenscancov40pc is null;

-- check results are logical
select count(*)
FROM wind_ds.pt_grid_us_ind
where maxheight_m_popdenscancov20pc > maxheight_m_popdens;

select count(*)
FROM wind_ds.pt_grid_us_ind
where maxheight_m_popdenscancov40pc > maxheight_m_popdens;

select count(*)
FROM wind_ds.pt_grid_us_ind
where maxheight_m_popdenscancov20pc > maxheight_m_popdenscancov40pc;

-- if they are not, it is probably due to NN backfilling
-- fix as follows
UPDATE wind_ds.pt_grid_us_ind
SET maxheight_m_popdenscancov20pc = maxheight_m_popdens
where maxheight_m_popdenscancov20pc > maxheight_m_popdens;

UPDATE wind_ds.pt_grid_us_ind
SET maxheight_m_popdenscancov40pc = maxheight_m_popdens
where maxheight_m_popdenscancov40pc > maxheight_m_popdens;

UPDATE wind_ds.pt_grid_us_ind
SET maxheight_m_popdenscancov20pc = maxheight_m_popdenscancov40pc
where maxheight_m_popdenscancov20pc > maxheight_m_popdenscancov40pc;

-- annual average rates (from polygons)
DROP TABLE IF EXISTS wind_ds_data.pt_grid_us_ind_annual_rate_gid_lookup;
CREATE TABLE wind_ds_data.pt_grid_us_ind_annual_rate_gid_lookup (
	gid integer,
	annual_rate_gid integer);


SELECT parsel_2('dav-gis','wind_ds.pt_grid_us_ind','gid',
		'SELECT a.gid, b.gid as annual_rate_gid
		FROM  wind_ds.pt_grid_us_ind a
		INNER JOIN wind_ds.annual_ave_elec_rates_2011 b
		ON ST_Intersects(a.the_geom_4326,b.the_geom_4326)
		WHERE b.ind_cents_per_kwh IS NOT NULL;',
	'wind_ds_data.pt_grid_us_ind_annual_rate_gid_lookup', 'a',16);

-- join the info back in
ALTER TABLE wind_ds.pt_grid_us_ind ADD COLUMN annual_rate_gid integer;

CREATE INDEX pt_grid_us_ind_annual_rate_gid_lookup_gid_btree ON wind_ds_data.pt_grid_us_ind_annual_rate_gid_lookup using btree(gid);

UPDATE wind_ds.pt_grid_us_ind a
SET annual_rate_gid = b.annual_rate_gid
FROM wind_ds_data.pt_grid_us_ind_annual_rate_gid_lookup b
where a.gid = b.gid;


	-- check for nulls
	DROP TABLE IF EXISTS wind_ds_data.no_ind_rate_pts;
	CREATE TABLE wind_ds_data.no_ind_rate_pts AS
	SELECT gid, the_geom_900914
	FROM wind_ds.pt_grid_us_ind
	where annual_rate_gid is null;
	-- inspect in Q
	-- these are all in sliver gaps and along the periphery of the country -- just use the nearest polygon's rate

	-- get value for nulls based on average from the same county
	DROP TABLE IF EXISTS wind_ds_data.no_ind_rate_pts_closest;
	CREATE TABLE wind_ds_data.no_ind_rate_pts_closest AS
	with candidates as (

	SELECT a.gid, a.the_geom_900914, 
		unnest((select array(SELECT b.gid
		 FROM wind_ds.annual_ave_elec_rates_2011 b
		 WHERE b.ind_cents_per_kwh IS NOT NULL
		 ORDER BY a.the_geom_900914 <#> b.the_geom_900914 LIMIT 5))) as rate_gid
	FROM wind_ds.pt_grid_us_ind a
	where a.annual_rate_gid is null
	 )

	SELECT distinct ON (gid) a.gid, a.the_geom_900914, a.rate_gid
	FROM candidates a
	LEFT JOIN wind_ds.annual_ave_elec_rates_2011 b
	ON a.rate_gid = b.gid
	ORDER BY a.gid, ST_Distance(a.the_geom_900914,b.the_geom_900914) asc;

	-- update the missing values in the main table
	UPDATE wind_ds.pt_grid_us_ind a
	SET annual_rate_gid = b.rate_gid
	FROM wind_ds_data.no_ind_rate_pts_closest b
	WHERE a.annual_rate_gid is null
	and a.gid = b.gid;

	-- check for any remaining nulls
	select count(*)
	FROM wind_ds.pt_grid_us_ind
	WHERE annual_rate_gid is null;

-- create dsire incentives lookup table
	--create the lookup table	
	DROP TABLE IF EXISTS wind_ds.dsire_incentives_lookup_ind;
	CREATE TABLE wind_ds.dsire_incentives_lookup_ind AS

	with a as (
	SELECT b.gid, b.the_geom, d.uid as wind_incentives_uid

	FROM dg_wind.incentives_geoms_copy_diced b

	inner JOIN geo_incentives.incentives c
	ON b.gid = c.geom_id

	INNER JOIN geo_incentives.wind_incentives d
	ON c.gid = d.incentive_id)

	SELECT e.gid as pt_gid, a.wind_incentives_uid
	FROM a

	INNER JOIN wind_ds.pt_grid_us_ind e
	ON ST_Intersects(a.the_geom,e.the_geom_4326);

	CREATE INDEX dsire_incentives_lookup_ind_pt_gid_btree ON wind_ds.dsire_incentives_lookup_ind using btree(pt_gid);

	SELECT pt_gid, count(*)
	FROM wind_ds.dsire_incentives_lookup_ind
	GROUP BY pt_gid
	ORDER by count desc;	


-- add foreign keys
	-- for county_id to county_geom.county id
ALTER TABLE wind_ds.pt_grid_us_ind ADD CONSTRAINT county_id_fkey FOREIGN KEY (county_id) 
REFERENCES wind_ds.county_geom (county_id) MATCH FULL 
ON UPDATE RESTRICT ON DELETE RESTRICT;
	-- for iiijjjicf_id to iiijjjicf_lookup.id
ALTER TABLE wind_ds.pt_grid_us_ind ADD CONSTRAINT iiijjjicf_id_fkey FOREIGN KEY (iiijjjicf_id) 
REFERENCES wind_ds.iiijjjicf_lookup (id) MATCH FULL 
ON UPDATE RESTRICT ON DELETE RESTRICT;
	-- for dsire_incentives_lookup_res.pt_gid to pt_grid_us_ind.gid
ALTER TABLE wind_ds.dsire_incentives_lookup_ind ADD CONSTRAINT pt_gid_fkey FOREIGN KEY (pt_gid) 
REFERENCES wind_ds.pt_grid_us_ind (gid) MATCH FULL 
ON UPDATE RESTRICT ON DELETE RESTRICT;
	-- for dsire_incentives_lookup_res.wind_incentives_uid to geo_incentives.wind_incentives.uid
ALTER TABLE wind_ds.dsire_incentives_lookup_ind ADD CONSTRAINT wind_incentives_uid_fkey FOREIGN KEY (wind_incentives_uid) 
REFERENCES geo_incentives.wind_incentives (uid) MATCH FULL 
ON UPDATE RESTRICT ON DELETE RESTRICT;
	-- for annual_rate_gid to annual_ave_elec_rates_2011.gid
-- ALTER TABLE wind_ds.pt_grid_us_ind DROP CONSTRAINT annual_rate_gid_fkey;
ALTER TABLE wind_ds.pt_grid_us_ind ADD CONSTRAINT annual_rate_gid_fkey FOREIGN KEY (annual_rate_gid) 
REFERENCES wind_ds.annual_ave_elec_rates_2011 (gid) MATCH FULL 
ON UPDATE RESTRICT ON DELETE RESTRICT;







