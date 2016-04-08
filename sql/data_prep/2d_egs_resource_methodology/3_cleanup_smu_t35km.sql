-- change ownership
ALTER TABLE dgeo.smu_t35km
OWNER TO "dgeo-writers";

set role 'dgeo-writers';

-- fix column names
ALTER TABLE dgeo.smu_t35km
RENAME COLUMN "TEMP" to temp_c;

ALTER TABLE dgeo.smu_t35km
RENAME COLUMN "INTERVAL" to temp_interval_50c;

ALTER TABLE dgeo.smu_t35km
RENAME COLUMN "INTERVL2" to temp_interval_25c;

-- add new geom columns
ALTER TABLE dgeo.smu_t35km
ADD COLUMN the_geom_4326 geometry,
ADD COLUMN the_geom_96703 geometry;

UPDATE dgeo.smu_t35km
set the_geom_96703 = ST_Transform(the_geom_4267, 96703);

UPDATE dgeo.smu_t35km
set the_geom_4326 = ST_Transform(the_geom_4267, 4326);

-- add indices
CREATE INDEX smu_t35km_gist_the_geom_96703
ON dgeo.smu_t35km
USING GIST(the_geom_96703);

CREATE INDEX smu_t35km_gist_the_geom_4326
ON dgeo.smu_t35km
USING GIST(the_geom_4326);

-- drop the original geom column
ALTER TABLE dgeo.smu_t35km
DROP COLUMN IF EXISTS the_geom_4267;

-- calculate the x and y of the centroids (in 96703 and 4326)
ALTER TABLE dgeo.smu_t35km
ADD COLUMN x_96703 numeric,
add column y_96703 numeric;

UPDATE dgeo.smu_t35km
set x_96703 = ST_X(ST_Centroid(the_geom_96703));

UPDATE dgeo.smu_t35km
set y_96703 = ST_y(ST_Centroid(the_geom_96703));


ALTER TABLE dgeo.smu_t35km
ADD COLUMN lon numeric,
add column lat numeric;

UPDATE dgeo.smu_t35km
set lon = ST_X(ST_Centroid(the_geom_4326));

UPDATE dgeo.smu_t35km
set lat = ST_y(ST_Centroid(the_geom_4326));
