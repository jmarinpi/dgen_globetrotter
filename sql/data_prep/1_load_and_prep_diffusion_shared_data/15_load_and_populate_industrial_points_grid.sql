------------------------------------------------------------------------------------------------------------
-- INGEST PTS AND SIMPLE CLEANUP
------------------------------------------------------------------------------------------------------------
set role 'diffusion-writers';
-- DROP TABLE IF EXISTS diffusion_shared.pt_grid_us_ind_new;
CREATE TABLE diffusion_shared.pt_grid_us_ind_new (
	x numeric,
	y numeric, 
	temp_col integer);

SET ROLE "server-superusers";
COPY diffusion_shared.pt_grid_us_ind_new 
FROM '/srv/home/mgleason/data/dg_wind/land_masks_20140205/ind_mask.csv' with csv header;
set role 'diffusion-writers';

-- drop this column -- it means nothing
ALTER TABLE diffusion_shared.pt_grid_us_ind_new 
DROP COLUMN temp_col;
------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------
-- GEOM (4326)
------------------------------------------------------------------------------------------------------------
ALTER TABLE diffusion_shared.pt_grid_us_ind_new 
ADD COLUMN the_geom_4326 geometry;

UPDATE diffusion_shared.pt_grid_us_ind_new
SET the_geom_4326 = ST_SetSRID(ST_MakePoint(x,y),4326);
-- 1,145,187 rows

CREATE INDEX pt_grid_us_ind_new_the_geom_4326_gist 
ON diffusion_shared.pt_grid_us_ind_new 
USING gist(the_geom_4326);

CLUSTER diffusion_shared.pt_grid_us_ind_new 
USING pt_grid_us_ind_new_the_geom_4326_gist;

VACUUM ANALYZE diffusion_shared.pt_grid_us_ind_new;
------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------
-- GID (PRIMARY KEY)
------------------------------------------------------------------------------------------------------------
ALTER TABLE diffusion_shared.pt_grid_us_ind_new 
ADD COLUMN gid serial;

ALTER TABLE diffusion_shared.pt_grid_us_ind_new 
ADD PRIMARY KEY (gid);
------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------
-- COUNTY ID
------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS diffusion_wind_data.pt_grid_us_ind_new_county_lkup;
CREATE TABLE diffusion_wind_data.pt_grid_us_ind_new_county_lkup
(
	gid integer,
	county_id integer
);


select parsel_2('dav-gis','mgleason', 'mgleason',
		'diffusion_shared.pt_grid_us_ind_new','gid',
		'SELECT DISTINCT ON (a.gid) a.gid, b.county_id
		FROM diffusion_shared.pt_grid_us_ind_new a
		INNER JOIN diffusion_shared.county_geom b
		ON ST_Intersects(a.the_geom_4326, b.the_geom_4326)
		ORDER by a.gid, b.county_id;', -- distinct on ensures that there are no dupes along county borders
		'diffusion_wind_data.pt_grid_us_ind_new_county_lkup', 
		'a', 16);

-- add primary key to the lkup table
ALTER TABLE diffusion_wind_data.pt_grid_us_ind_new_county_lkup
ADD PRIMARY KEY (gid);

-- add the results to the main table
ALTER TABLE diffusion_shared.pt_grid_us_ind_new 
ADD COLUMN county_id integer;

UPDATE diffusion_shared.pt_grid_us_ind_new a
SET county_id = b.county_id
FROM diffusion_wind_data.pt_grid_us_ind_new_county_lkup b
WHERE a.gid = b.gid;

-- add an index to the main table
CREATE INDEX pt_grid_us_ind_new_county_id_btree 
ON diffusion_shared.pt_grid_us_ind_new 
using btree(county_id);

-- check why there are nulls
DROP TABLE IF EXISTS diffusion_wind_data.no_county_pts_ind;
CREATE TABLE diffusion_wind_data.no_county_pts_ind AS
SELECT gid, the_geom_4326
FROM diffusion_shared.pt_grid_us_ind_new
where county_id is null;
-- 322  rows
-- inspect in Q - all along edges of the country

-- pick county based on nearest
DROP TABLE IF EXIStS diffusion_wind_data.no_county_pts_ind_closest;
CREATE TABLE diffusion_wind_data.no_county_pts_ind_closest AS
with candidates as (

SELECT a.gid, a.the_geom_4326, 
	unnest((select array(SELECT b.county_id
		 FROM diffusion_shared.county_geom b
		 ORDER BY a.the_geom_4326 <#> b.the_geom_4326 LIMIT 5))) as county_id
		FROM diffusion_shared.pt_grid_us_ind_new a
		where a.county_id is null
 )

SELECT distinct ON (gid) a.gid, a.county_id
FROM candidates a
lEFT JOIN diffusion_shared.county_geom b
ON a.county_id = b.county_id
ORDER BY gid, ST_Distance(a.the_geom_4326,b.the_geom_4326) asc;
-- inspect in Q

-- update the main table
UPDATE diffusion_shared.pt_grid_us_ind_new a
SET county_id = b.county_id
FROM diffusion_wind_data.no_county_pts_ind_closest b
WHERE a.county_id is null
and a.gid = b.gid;

-- make sure no more nulls remain
SELECT count(*)
FROM diffusion_shared.pt_grid_us_ind_new
where county_id is null;
-- 0

-- drop the other tables
DROP TABLE IF EXISTS diffusion_wind_data.no_county_pts_ind;
DROP TABLE IF EXISTS diffusion_wind_data.no_county_pts_ind_closest;
------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------
-- UTILITY TYPE
------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS diffusion_wind_data.pt_grid_us_ind_new_utiltype_lookup;
CREATE TABLE diffusion_wind_data.pt_grid_us_ind_new_utiltype_lookup 
(
	gid integer,
	utility_type character varying(9)
);

select parsel_2('dav-gis','mgleason','mgleason',
		'diffusion_shared.pt_grid_us_ind_new','gid',
		'WITH ut_ranks as (
			SELECT unnest(array[''IOU'',''Muni'',''Coop'',''All Other'']) as utility_type, generate_series(1,4) as rank
		),
		isect as (
			SELECT a.gid, b.company_type_general as utility_type, c.rank
			FROM diffusion_shared.pt_grid_us_ind_new a
			INNER JOIN dg_wind.ventyx_elec_serv_territories_edit_diced b
			ON ST_Intersects(a.the_geom_4326, b.the_geom_4326)
			LEFT JOIN ut_ranks c
			ON b.company_type_general = c.utility_type)
		SELECT DISTINCT ON (a.gid) a.gid, a.utility_type 
		FROM isect a
		ORDER BY a.gid, a.rank ASC;',
		'diffusion_wind_data.pt_grid_us_ind_new_utiltype_lookup', 
		'a', 16);

-- add a primary key to the lkup table
ALTER TABLE diffusion_wind_data.pt_grid_us_ind_new_utiltype_lookup
ADD PRIMARY KEY(gid);

-- join the info back in
ALTER TABLE diffusion_shared.pt_grid_us_ind_new 
ADD COLUMN utility_type character varying(9);

UPDATE diffusion_shared.pt_grid_us_ind_new a
SET utility_type = b.utility_type
FROM diffusion_wind_data.pt_grid_us_ind_new_utiltype_lookup b
where a.gid = b.gid;

CREATE INDEX pt_grid_us_ind_new_utility_type_btree 
ON diffusion_shared.pt_grid_us_ind_new 
USING btree(utility_type);

-- are there any nulls?
SELECT count(*) 
FROM diffusion_shared.pt_grid_us_ind_new
where utility_type is null;
-- 4430 rows

-- isolate the unjoined points
-- and fix them by assigning value from their nearest neighbor that is not null
DROP TABLE IF EXISTS diffusion_wind_data.pt_grid_us_ind_new_utiltype_missing;
CREATE TABLE diffusion_wind_data.pt_grid_us_ind_new_utiltype_missing AS
with a AS(
	select gid, the_geom_4326
	FROM diffusion_shared.pt_grid_us_ind_new
	where utility_type is null)
SELECT a.gid, a.the_geom_4326, 
	(SELECT b.utility_type 
	 FROM diffusion_shared.pt_grid_us_ind_new b
	 where b.utility_type is not null
	 ORDER BY a.the_geom_4326 <#> b.the_geom_4326
	 LIMIT 1) as utility_type
FROM a;

--update the points table
UPDATE diffusion_shared.pt_grid_us_ind_new a
SET utility_type = b.utility_type
FROM diffusion_wind_data.pt_grid_us_ind_new_utiltype_missing b
where a.gid = b.gid
and a.utility_type is null;

-- any nulls left?
SELECT count(*) 
FROM diffusion_shared.pt_grid_us_ind_new
where utility_type is null;
-- 0 rows
------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------
-- WIND RESOURCE IDS (III, JJJ, ICF)
------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS diffusion_wind_data.pt_grid_us_ind_new_iiijjjicf_id_lookup;
CREATE TABLE diffusion_wind_data.pt_grid_us_ind_new_iiijjjicf_id_lookup (
	gid integer,
	iiijjjicf_id integer);

--run in parallel for speed
SELECT parsel_2('dav-gis','mgleason','mgleason','diffusion_shared.pt_grid_us_ind_new','gid',
'SELECT a.gid, ST_Value(b.rast,a.the_geom_4326) as iiijjjicf_id
FROM  diffusion_shared.pt_grid_us_ind_new a
INNER JOIN aws_2014.iiijjjicf_200m_raster_100x100 b
ON ST_Intersects(b.rast,a.the_geom_4326);',
	'diffusion_wind_data.pt_grid_us_ind_new_iiijjjicf_id_lookup', 'a',16);

-- add a primary key on the lookup table
ALTER TABLE diffusion_wind_data.pt_grid_us_ind_new_iiijjjicf_id_lookup
ADD PRIMARY KEY (gid);

-- join the info back in
ALTER TABLE diffusion_shared.pt_grid_us_ind_new 
ADD COLUMN iiijjjicf_id integer;

UPDATE diffusion_shared.pt_grid_us_ind_new a
SET iiijjjicf_id = b.iiijjjicf_id
FROM diffusion_wind_data.pt_grid_us_ind_new_iiijjjicf_id_lookup b
where a.gid = b.gid;

CREATE INDEX pt_grid_us_ind_new_iiijjjicf_id_btree 
ON diffusion_shared.pt_grid_us_ind_new 
USING btree(iiijjjicf_id);

-- check for points with no iiijjjicf -- there shouldnt be any since the land mask is clipped to the raster
SELECT count(*)
FROM diffusion_shared.pt_grid_us_ind_new
where iiijjjicf_id is null;
-- 0
------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------
-- DSIRE INCENTIVES (WIND)
------------------------------------------------------------------------------------------------------------
--create the lookup table	
DROP TABLE IF EXISTS diffusion_wind.dsire_incentives_lookup_ind;
CREATE TABLE diffusion_wind.dsire_incentives_lookup_ind AS
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
INNER JOIN diffusion_shared.pt_grid_us_ind_new e
ON ST_Intersects(a.the_geom,e.the_geom_4326);

CREATE INDEX dsire_incentives_lookup_ind_pt_gid_btree 
ON diffusion_wind.dsire_incentives_lookup_ind 
using btree(pt_gid);

-- group the incentives into arrays so that there is just one row for each pt_gid
DROP TABLE IF EXISTS diffusion_wind_data.dsire_incentives_combos_lookup_ind;
CREATE TABLE diffusion_wind_data.dsire_incentives_combos_lookup_ind AS
SELECT pt_gid, array_agg(wind_incentives_uid order by wind_incentives_uid) as wind_incentives_uid_array
FROM diffusion_wind.dsire_incentives_lookup_ind
group by pt_gid;

-- find the unique set of incentive arrays
DROP TABLE IF EXISTS diffusion_wind_data.dsire_incentives_unique_combos_ind;
CREATE TABLE diffusion_wind_data.dsire_incentives_unique_combos_ind AS
SELECT distinct(wind_incentives_uid_array) as wind_incentives_uid_array
FROM diffusion_wind_data.dsire_incentives_combos_lookup_ind;

-- add a primary key to the table of incentive arrays
ALTER TABLE diffusion_wind_data.dsire_incentives_unique_combos_ind
ADD column incentive_array_id serial primary key;

-- join the incentive array primary key back into the combos_lookup_table
ALTER TABLE diffusion_wind_data.dsire_incentives_combos_lookup_ind
ADD column incentive_array_id integer;

UPDATE diffusion_wind_data.dsire_incentives_combos_lookup_ind a
SET incentive_array_id = b.incentive_array_id
FROM diffusion_wind_data.dsire_incentives_unique_combos_ind b
where a.wind_incentives_uid_array = b.wind_incentives_uid_array;

-- join this info back into the main points table
ALTER TABLE diffusion_shared.pt_grid_us_ind_new
ADD COLUMN wind_incentive_array_id integer;

UPDATE diffusion_shared.pt_grid_us_ind_new a
SET wind_incentive_array_id = b.incentive_array_id
FROM diffusion_wind_data.dsire_incentives_combos_lookup_ind b
WHere a.gid = b.pt_gid;

-- add an index
CREATE INDEX pt_grid_us_ind_new_wind_incentive_btree 
ON diffusion_shared.pt_grid_us_ind_new
USING btree(wind_incentive_array_id);

--unnest the data from the unique combos table
DROP TABLE IF EXISTS diffusion_wind.dsire_incentives_simplified_lkup_ind;
CREATE TABLE diffusion_wind.dsire_incentives_simplified_lkup_ind AS
SELECT incentive_array_id as incentive_array_id, 
	unnest(wind_incentives_uid_array) as incentives_uid
FROM diffusion_wind_data.dsire_incentives_unique_combos_ind;

-- create index
CREATE INDEX dsire_incentives_simplified_lkup_ind_inc_id_btree
ON diffusion_wind.dsire_incentives_simplified_lkup_ind
USING btree(incentive_array_id);
------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------
-- DSIRE INCENTIVES (SOLAR)
------------------------------------------------------------------------------------------------------------
--create the lookup table		
DROP TABLE IF EXISTS diffusion_solar.dsire_incentives_lookup_ind;
CREATE TABLE diffusion_solar.dsire_incentives_lookup_ind AS
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
INNER JOIN diffusion_shared.pt_grid_us_ind_new e
ON ST_Intersects(a.the_geom,e.the_geom_4326);

CREATE INDEX dsire_incentives_lookup_ind_pt_gid_btree 
ON diffusion_solar.dsire_incentives_lookup_ind 
using btree(pt_gid);

-- group the incentives into arrays so that there is just one row for each pt_gid
DROP TABLE IF EXISTS diffusion_solar_data.dsire_incentives_combos_lookup_ind;
CREATE TABLE diffusion_solar_data.dsire_incentives_combos_lookup_ind AS
SELECT pt_gid, array_agg(solar_incentives_uid order by solar_incentives_uid) as solar_incentives_uid_array
FROM diffusion_solar.dsire_incentives_lookup_ind
group by pt_gid;

-- find the unique set of incentive arrays
DROP TABLE IF EXISTS diffusion_solar_data.dsire_incentives_unique_combos_ind;
CREATE TABLE diffusion_solar_data.dsire_incentives_unique_combos_ind AS
SELECT distinct(solar_incentives_uid_array) as solar_incentives_uid_array
FROM diffusion_solar_data.dsire_incentives_combos_lookup_ind;

-- add a primary key to the table of incentive arrays
ALTER TABLE diffusion_solar_data.dsire_incentives_unique_combos_ind
ADD column incentive_array_id serial primary key;

-- join the incentive array primary key back into the combos_lookup_table
ALTER TABLE diffusion_solar_data.dsire_incentives_combos_lookup_ind
ADD column incentive_array_id integer;

UPDATE diffusion_solar_data.dsire_incentives_combos_lookup_ind a
SET incentive_array_id = b.incentive_array_id
FROM diffusion_solar_data.dsire_incentives_unique_combos_ind b
where a.solar_incentives_uid_array = b.solar_incentives_uid_array;

-- join this info back into the main points table
ALTER TABLE diffusion_shared.pt_grid_us_ind_new
ADD COLUMN solar_incentive_array_id integer;

UPDATE diffusion_shared.pt_grid_us_ind_new a
SET solar_incentive_array_id = b.incentive_array_id
FROM diffusion_solar_data.dsire_incentives_combos_lookup_ind b
WHere a.gid = b.pt_gid;

-- add an index
CREATE INDEX pt_grid_us_ind_new_solar_incentive_btree 
ON diffusion_shared.pt_grid_us_ind_new
USING btree(solar_incentive_array_id);

-- check that we got tem all
SELECT count(*)
FROM diffusion_shared.pt_grid_us_ind_new
where solar_incentive_array_id is not null;
--1145181

SELECT count(*)
FROM diffusion_solar_data.dsire_incentives_combos_lookup_ind
where incentive_array_id is not null;
--1145181

--unnest the data from the unique combos table
DROP TABLE IF EXISTS diffusion_solar.dsire_incentives_simplified_lkup_ind;
CREATE TABLE diffusion_solar.dsire_incentives_simplified_lkup_ind AS
SELECT incentive_array_id as incentive_array_id, 
	unnest(solar_incentives_uid_array) as incentives_uid
FROM diffusion_solar_data.dsire_incentives_unique_combos_ind;

-- create index
CREATE INDEX dsire_incentives_simplified_lkup_ind_inc_id_btree
ON diffusion_solar.dsire_incentives_simplified_lkup_ind
USING btree(incentive_array_id);
------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------
-- REEDS REGIONS AND PCAS
------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS diffusion_wind_data.pt_grid_us_ind_new_pca_reg_lookup;
CREATE TABLE diffusion_wind_data.pt_grid_us_ind_new_pca_reg_lookup 
(
	gid integer,
	pca_reg integer,
	reeds_reg integer
);

SELECT parsel_2('dav-gis','mgleason','mgleason','diffusion_shared.pt_grid_us_ind_new','gid',
		'SELECT a.gid, b.pca_reg, b.demreg as reeds_reg
		FROM  diffusion_shared.pt_grid_us_ind_new a
		INNER JOIN reeds.reeds_regions b
		ON ST_Intersects(a.the_geom_4326,b.the_geom)
		WHERE b.pca_reg NOT IN (135,136);',
	'diffusion_wind_data.pt_grid_us_ind_new_pca_reg_lookup', 'a',16);

-- add primary key to the lookup table
ALTER TABLE diffusion_wind_data.pt_grid_us_ind_new_pca_reg_lookup
ADD PRIMARY KEY (gid);

-- join the info back in
ALTER TABLE diffusion_shared.pt_grid_us_ind_new 
ADD COLUMN pca_reg integer,
ADD COLUMN reeds_reg integer;

UPDATE diffusion_shared.pt_grid_us_ind_new a
SET (pca_reg,reeds_reg) = (b.pca_reg,b.reeds_reg)
FROM diffusion_wind_data.pt_grid_us_ind_new_pca_reg_lookup b
where a.gid = b.gid;

-- how many are null?
CREATE INDEX pt_grid_us_ind_new_pca_reg_btree 
ON diffusion_shared.pt_grid_us_ind_new 
USING btree(pca_reg);

CREATE INDEX pt_grid_us_ind_new_reeds_reg_btree 
ON diffusion_shared.pt_grid_us_ind_new 
USING btree(reeds_reg);

-- any missing?
select count(*)
FROM diffusion_shared.pt_grid_us_ind_new 
where pca_reg is null or reeds_reg is null;
--469

select count(*)
FROM diffusion_shared.pt_grid_us_ind_new 
where pca_reg is null;
-- 469

select count(*)
FROM diffusion_shared.pt_grid_us_ind_new 
where reeds_reg is null;
-- 469

-- fix the missing based on the closest
DROP TABLE IF EXISTS diffusion_wind_data.pt_grid_us_ind_new_pca_reg_missing_lookup;
CREATE TABLE diffusion_wind_data.pt_grid_us_ind_new_pca_reg_missing_lookup AS
with a AS
(
	select gid, the_geom_4326
	FROM diffusion_shared.pt_grid_us_ind_new
	where pca_reg is null
),
b as 
(
	SELECT a.gid, a.the_geom_4326, 
		(SELECT b.gid
		 FROM diffusion_shared.pt_grid_us_ind_new b
		 where b.pca_reg is not null
		 ORDER BY a.the_geom_4326 <#> b.the_geom_4326
		 LIMIT 1) as nn_gid
	FROM a
)
SELECT b.gid, b.the_geom_4326, b.nn_gid, c.pca_reg, c.reeds_reg
from b
LEFT JOIN diffusion_shared.pt_grid_us_ind_new c
ON b.nn_gid = c.gid;
  
--update the points table
UPDATE diffusion_shared.pt_grid_us_ind_new a
SET (pca_reg,reeds_reg) = (b.pca_reg,b.reeds_reg)
FROM diffusion_wind_data.pt_grid_us_ind_new_pca_reg_missing_lookup b
where a.gid = b.gid
and a.pca_reg is null;
 
-- check for any remaining nulls?
select count(*)
FROM diffusion_shared.pt_grid_us_ind_new 
where pca_reg is null or reeds_reg is null;
------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------
-- NSRDB GRID GIDs
------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS  diffusion_solar_data.pt_grid_us_ind_new_solar_re_9809_lookup;
CREATE TABLE  diffusion_solar_data.pt_grid_us_ind_new_solar_re_9809_lookup 
(
	gid integer,
	solar_re_9809_gid integer
);


SELECT parsel_2('dav-gis','mgleason','mgleason','diffusion_shared.pt_grid_us_ind_new','gid',
		'SELECT a.gid, b.gid as solar_re_9809_gid
		FROM  diffusion_shared.pt_grid_us_ind_new a
		INNER JOIN solar.solar_re_9809 b
		ON ST_Intersects(a.the_geom_4326,b.the_geom_4326);',
	'diffusion_solar_data.pt_grid_us_ind_new_solar_re_9809_lookup', 'a',16);

-- add primary key to lookup table
ALTER TABLE diffusion_solar_data.pt_grid_us_ind_new_solar_re_9809_lookup 
ADD PRIMARY KEY (gid);

-- join the info back in
ALTER TABLE diffusion_shared.pt_grid_us_ind_new 
ADD COLUMN solar_re_9809_gid integer;

UPDATE diffusion_shared.pt_grid_us_ind_new a
SET solar_re_9809_gid = b.solar_re_9809_gid
FROM  diffusion_solar_data.pt_grid_us_ind_new_solar_re_9809_lookup  b
where a.gid = b.gid;

-- how many are null?
CREATE INDEX pt_grid_us_ind_new_solar_re_9809_gid_btree 
ON diffusion_shared.pt_grid_us_ind_new 
USING btree(solar_re_9809_gid);

-- any missing?
select count(*)
FROM diffusion_shared.pt_grid_us_ind_new 
where solar_re_9809_gid is null;
-- 70

-- fix the missing based on the closest
DROP TABLE IF EXISTS  diffusion_solar_data.pt_grid_us_solar_re_9809_gid_missing_lookup;
CREATE TABLE  diffusion_solar_data.pt_grid_us_solar_re_9809_gid_missing_lookup AS
with a AS
(
	select gid, the_geom_4326
	FROM diffusion_shared.pt_grid_us_ind_new
	where solar_re_9809_gid is null
)
SELECT a.gid, a.the_geom_4326, 
	(SELECT b.solar_re_9809_gid
	 FROM diffusion_shared.pt_grid_us_ind_new b
	 where b.solar_re_9809_gid is not null
	 ORDER BY a.the_geom_4326 <#> b.the_geom_4326
	 LIMIT 1) as solar_re_9809_gid
	FROM a;
  
--update the points table
UPDATE diffusion_shared.pt_grid_us_ind_new a
SET solar_re_9809_gid = b.solar_re_9809_gid
FROM  diffusion_solar_data.pt_grid_us_solar_re_9809_gid_missing_lookup b
where a.gid = b.gid
and a.solar_re_9809_gid is null;
 
-- check for any remaining nulls?
select count(*)
FROM diffusion_shared.pt_grid_us_ind_new 
where solar_re_9809_gid is null;
------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------
-- ENERGY PLUS HDF INDEX
------------------------------------------------------------------------------------------------------------
-- load in the energy plus hdf index associated with each point
-- the energy plus simulations are based on TMY3 stations
-- for each nsrdb grid, we know the "best match" TMY3 station, so we can just link
-- to that off of the nsrdb_gid
-- there will be gaps due to missing stations, which will fix with a nearest neighbor search
ALTER TABLE diffusion_shared.pt_grid_us_ind_new
add column hdf_load_index integer;

UPDATE diffusion_shared.pt_grid_us_ind_new a
SET hdf_load_index = b.hdf_index
FROM diffusion_shared.solar_re_9809_to_eplus_load_com b
where a.solar_re_9809_gid = b.solar_re_9809_gid;

-- create index on the hdf_load_index
CREATE INDEX pt_grid_us_ind_new_hdf_load_index
ON diffusion_shared.pt_grid_us_ind_new
using btree(hdf_load_index);

-- check for nulls
SELECT count(*)
FROM diffusion_shared.pt_grid_us_ind_new
where hdf_load_index is null;
--3461 rows

-- find the value of the nearest neighbor
DROP TABLE IF EXISTS  diffusion_solar_data.pt_grid_us_ind_new_missing_hdf_load_lookup;
CREATE TABLE  diffusion_solar_data.pt_grid_us_ind_new_missing_hdf_load_lookup AS
with a AS
(
	select gid, the_geom_4326
	FROM diffusion_shared.pt_grid_us_ind_new
	where hdf_load_index is null
)
SELECT a.gid, a.the_geom_4326, 
	(
		SELECT b.hdf_load_index
		 FROM diffusion_shared.pt_grid_us_ind_new b
		 where b.hdf_load_index is not null
		 ORDER BY a.the_geom_4326 <#> b.the_geom_4326
		 LIMIT 1
	 ) as hdf_load_index
	FROM a;

UPDATE diffusion_shared.pt_grid_us_ind_new a
SET hdf_load_index = b.hdf_load_index
FROM  diffusion_solar_data.pt_grid_us_ind_new_missing_hdf_load_lookup b
where a.gid = b.gid
and a.hdf_load_index is null;

-- check for nulls again
SELECT count(*)
FROM diffusion_shared.pt_grid_us_ind_new
where hdf_load_index is null;
-- 0 rows
------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------
-- FOREIGN KEYS
------------------------------------------------------------------------------------------------------------
	-- for county_id to county_geom.county id
ALTER TABLE diffusion_shared.pt_grid_us_ind_new ADD CONSTRAINT county_id_fkey FOREIGN KEY (county_id) 
REFERENCES diffusion_shared.county_geom (county_id) MATCH FULL 
ON UPDATE RESTRICT ON DELETE RESTRICT;
	-- for iiijjjicf_id to iiijjjicf_lookup.id
ALTER TABLE diffusion_shared.pt_grid_us_ind_new ADD CONSTRAINT iiijjjicf_id_fkey FOREIGN KEY (iiijjjicf_id) 
REFERENCES diffusion_wind.iiijjjicf_lookup (id) MATCH FULL 
ON UPDATE RESTRICT ON DELETE RESTRICT;
	-- for dsire_incentives_lookup_res.pt_gid to pt_grid_us_ind_new.gid
ALTER TABLE diffusion_wind.dsire_incentives_lookup_ind ADD CONSTRAINT pt_gid_fkey FOREIGN KEY (pt_gid) 
REFERENCES diffusion_shared.pt_grid_us_ind_new (gid) MATCH FULL 
ON UPDATE RESTRICT ON DELETE RESTRICT;
	-- for dsire_incentives_lookup_res.wind_incentives_uid to geo_incentives.wind_incentives.uid
ALTER TABLE diffusion_wind.dsire_incentives_lookup_ind ADD CONSTRAINT wind_incentives_uid_fkey FOREIGN KEY (wind_incentives_uid) 
REFERENCES geo_incentives.wind_incentives (uid) MATCH FULL 
ON UPDATE RESTRICT ON DELETE RESTRICT;
	-- for annual_rate_gid to annual_ave_elec_rates_2011.gid
-- ALTER TABLE diffusion_shared.pt_grid_us_ind_new DROP CONSTRAINT annual_rate_gid_fkey;
ALTER TABLE diffusion_shared.pt_grid_us_ind_new ADD CONSTRAINT annual_rate_gid_fkey FOREIGN KEY (annual_rate_gid) 
REFERENCES diffusion_shared.annual_ave_elec_rates_2011 (gid) MATCH FULL 
ON UPDATE RESTRICT ON DELETE RESTRICT;







