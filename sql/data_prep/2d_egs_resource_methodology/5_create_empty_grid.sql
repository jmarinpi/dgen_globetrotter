set role 'dgeo-writers';

-- find the grid cell size (length) in the smu_t35km
select r_median(array_agg(ST_Area(the_geom_4326)::NUMERIC))^.5
from dgeo.smu_t35km;
-- 0.0833333329999988

-- create a grid that covers the entire area
DROP TABLE IF EXISTS dgeo.egs_empty_grid;
CREATE TABLE dgeo.egs_empty_grid AS
with a as
(
	select min(lon) as min_x, 
		min(lat) as min_y, 
		max(lon) as max_x, 
		max(lat) as max_y
	from dgeo.smu_t35km
),
b as 
(
	select unnest(r_seq(min_x, max_x, 0.08333333)) as x,
		unnest(r_seq(min_y, max_y, 0.08333333)) as y
	from a
)
select ST_SetSrid(ST_MakePoint(x, y), 4326) as the_geom_4326
FROM b;
-- 201372 rows

-- how does this compare to the original table?
select count(*)
FROM dgeo.smu_t35km;
-- 117401

-- good, need to drop points outside the land though

-- add gist
cREATE INDEX egs_empty_grid_gist_the_geom_4326
ON dgeo.egs_empty_grid
using gist(the_geom_4326);

-- add primary key
ALTER TABLE dgeo.egs_empty_grid
ADD column gid serial;


-- drop the points outside the original grid
with b as
(
	select a.gid
	from dgeo.egs_empty_grid a
	LEFT JOIN dgeo.smu_t35km b
	ON ST_Intersects(a.the_geom_4326, b.the_geom_4326)
	where b.the_geom_4326 is null
)
DELETE FROM dgeo.egs_empty_grid a
USING b
where a.gid = b.gid;
-- 83870 rows deleted

-- check in Q against the smu_t35km grid -- looks perfect

-- add 96703 geometries
ALTER TABLE dgeo.egs_empty_grid
ADD COLUMN the_geom_96703 geometry;

UPDATE dgeo.egs_empty_grid
SET the_geom_96703 = ST_Transform(the_geom_4326, 96703);
-- 117502 rows

-- add gist
cREATE INDEX egs_empty_grid_gist_the_geom_96703
ON dgeo.egs_empty_grid
using gist(the_geom_96703);

-- get the temperature values at 3.5 km
ALTER TABLE dgeo.egs_empty_grid
ADD COLUMN t35km numeric;

UPDATE dgeo.egs_empty_grid a
SET t35km = b.temp_c
FROM dgeo.smu_t35km b
WHERE ST_Intersects(a.the_geom_4326, b.the_geom_4326);
-- 117502

-- check for nulls
select count(*)
FROM  dgeo.egs_empty_grid
where t35km is null;
-- 0 -- all set


ALTER TABLE dgeo.egs_empty_grid
ADD column x_96703 numeric,
add column y_96703 numeric;

UPDATE dgeo.egs_empty_grid
set x_96703 = ST_X(the_geom_96703);

UPDATE dgeo.egs_empty_grid
set y_96703 = ST_Y(the_geom_96703);

