-- change ownership
ALTER TABLE dgeo.smu_t35km_2016
OWNER TO "dgeo-writers";

set role 'dgeo-writers';

-- fix column names
ALTER TABLE dgeo.smu_t35km_2016
RENAME COLUMN t35 to temp_c;

-- add projected geom column
ALTER TABLE dgeo.smu_t35km_2016
ADD COLUMN the_geom_96703 geometry;

UPDATE dgeo.smu_t35km_2016
set the_geom_96703 = ST_Transform(the_geom_4326, 96703);

-- add indices
CREATE INDEX smu_t35km_2016_gist_the_geom_96703
ON dgeo.smu_t35km_2016
USING GIST(the_geom_96703);

CREATE INDEX smu_t35km_2016_gist_the_geom_4326
ON dgeo.smu_t35km_2016
USING GIST(the_geom_4326);

-- calculate the x and y of the centroids (in 96703 and 4326)
ALTER TABLE dgeo.smu_t35km_2016
ADD COLUMN x_96703 numeric,
add column y_96703 numeric;

UPDATE dgeo.smu_t35km_2016
set x_96703 = ST_X(ST_Centroid(the_geom_96703));

UPDATE dgeo.smu_t35km_2016
set y_96703 = ST_y(ST_Centroid(the_geom_96703));


ALTER TABLE dgeo.smu_t35km_2016
ADD COLUMN lon numeric,
add column lat numeric;

UPDATE dgeo.smu_t35km_2016
set lon = ST_X(ST_Centroid(the_geom_4326));

UPDATE dgeo.smu_t35km_2016
set lat = ST_y(ST_Centroid(the_geom_4326));


-- calculate area of each cell
ALTER TABLE dgeo.smu_t35km_2016
ADD COLUMN area_sqkm numeric;

UPDATE dgeo.smu_t35km_2016
set area_sqkm = ST_Area(the_geom_96703)/1000/1000;

-- check values
SELECT min(area_sqkm), avg(area_sqkm), max(area_sqkm)
from dgeo.smu_t35km_2016;
-- 13.594770949727, 16.3883701309914646, 19.6939917326383
-- so, resolution is nominally 4 km x 4 km
-- and we are unlikely to have any merged cells