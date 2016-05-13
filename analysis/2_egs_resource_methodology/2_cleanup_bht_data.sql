set role 'dgeo-writers';

-- add a new primary key
ALTER TABLE dgeo.bht_compilation
ADD gid serial primary key;

-- take a look at the data
select *
FROM dgeo.bht_compilation
limit 10;

------------------------------------------------------------------------------------------------
-- add geoms

-- add the columns
ALTER TABLE dgeo.bht_compilation
ADD column the_geom_4326 geometry,
add column the_geom_96703 geometry;

-- check unique SRS for values with weird srs
SELECT distinct srs
from dgeo.bht_compilation
where srs_flag = 1;
-- EPSG:4269
-- NAD83
-- NAD 83
-- these are all the same

-- update the geom field, transforming where the values are in 4269
UPDATE dgeo.bht_compilation
SET the_geom_4326 = CASE 
			WHEN srs_flag = 1 THEN ST_Transform(ST_SetSrid(ST_MakePoint(lon, lat), 4269), 4326)
			 ELSE  ST_SetSrid(ST_MakePoint(lon, lat), 4326)
			 END;
-- 389166 rows

-- check for null geoms?
SELECT count(*)
from dgeo.bht_compilation
where the_geom_4326 is null;
-- 0 -- all set

-- add the 96703 geom
UPDATE dgeo.bht_compilation
set the_geom_96703 = ST_Transform(the_geom_4326, 96703);

-- add indices
CREATE INDEX bht_compilation_gist_the_geom_96703
ON dgeo.bht_compilation
USING GIST(the_geom_96703);

CREATE INDEX bht_compilation_gist_the_geom_4326
ON dgeo.bht_compilation
USING GIST(the_geom_4326);

-- add columns for x and y coordinates in projected space
ALTER TABLE dgeo.bht_compilation
ADD column x_96703 numeric,
add column y_96703 numeric;

UPDATE dgeo.bht_compilation
set x_96703 = ST_X(the_geom_96703);
-- 389166 rows

UPDATE dgeo.bht_compilation
set y_96703 = ST_Y(the_geom_96703);
-- 389166 rows
------------------------------------------------------------------------------------------------

-- tag with the state
ALTER TABLE dgeo.bht_compilation
ADD COLUMN state_abbr varchar(2);

UPDATE dgeo.bht_compilation a
set  state_abbr = b.state_abbr
from diffusion_blocks.county_geoms b
WHERE ST_intersects(a.the_geom_96703, b.the_geom_96703_20m);
-- 372729

-- check for nulls?
select count(*)
FROM dgeo.bht_compilation
where state_abbr is null;
-- 16437
-- what's going on with these?
-- in Q: they all appear to be offshore
------------------------------------------------------------------------------------------------

-- check depth and temp units
-- make sure all temperatures are in Celsius?
select distinct temperatureunits
from dgeo.bht_compilation;
-- Celsius - all set


-- make sure all depths are meters
select distinct lengthunits
from dgeo.bht_compilation;
-- m -- all set

----------------------------------------------------------------

-- remove nulls
-- check for how many have null depth?
select count(*)
FROM dgeo.bht_compilation
where depthfinal is null;
-- 22242 of 389166
select  round(22242/389166., 2); -- 6%

-- how many null temps?
select count(*)
FROM dgeo.bht_compilation
where temperaturefinal is null;
-- 22239 of 389166
select  round(22239/389166., 2); -- 6%

-- what is the overlap?
select count(*)
FROM dgeo.bht_compilation
where temperaturefinal is null and depthfinal is null;
-- 22239

DELETE FROM dgeo.bht_compilation
where temperaturefinal is null and depthfinal is null;
-- 22239 rows deleted
-- to do:
-- delete those
-- backfill the rest of the missing depths

-- how many null depths remain?
select count(*)
FROM dgeo.bht_compilation
where depthfinal is null;
-- 3

-- go ahead and delete these too
delete FROM dgeo.bht_compilation
where depthfinal is null;

-- how many rows remain?
select count(*)
FROM dgeo.bht_compilation;
-- 366924

-- make sure no nulls remain
select count(*)
FROM dgeo.bht_compilation
where the_geom_4326 is null
or depthfinal is null
or temperaturefinal is null;
-- 0 all set

------------------------------------------------------------------------------------------
-- check temp and depth ranges
select min(depthfinal), max(depthfinal), min(temperaturefinal), max(temperaturefinal)
FROM dgeo.bht_compilation;
-- 0,50771.3414634146, -10002.7919817521, 8851.71843242869

select -2.3449e-6 * depthfinal^2 + 0.018268 * depthfinal - 16.512, dt
From dgeo.bht_compilation
where temperaturefinal < 0;
-- calcs are right, 2835 rows

-- just delete these
DELETE FROM dgeo.bht_compilation
where temperaturefinal < 0;

-- re calc the ranges
-- 0,9998.55963128049,0.00349189691240781,8851.71843242869
-- these look more reasonable