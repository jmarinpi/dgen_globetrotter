set role 'dgeo-writers';

-- add a new primary key
ALTER TABLE dgeo.bht_smu
ADD gid serial primary key;

-- add geoms
ALTER TABLE dgeo.bht_smu
ADD column the_geom_4326 geometry,
add column the_geom_96703 geometry;

UPDATE dgeo.bht_smu
set the_geom_4326 = ST_SetSrid(ST_MakePoint(lon, lat), 4326);

UPDATE dgeo.bht_smu
set the_geom_96703 = ST_Transform(the_geom_4326, 96703);

-- add indices
CREATE INDEX bht_smu_gist_the_geom_96703
ON dgeo.bht_smu
USING GIST(the_geom_96703);

CREATE INDEX bht_smu_gist_the_geom_4326
ON dgeo.bht_smu
USING GIST(the_geom_4326);

-- add columns for x and y coordinates in projectedd space
ALTER TABLE dgeo.bht_smu
ADD column x_96703 numeric,
add column y_96703 numeric;

UPDATE dgeo.bht_smu
set x_96703 = ST_X(the_geom_96703);

UPDATE dgeo.bht_smu
set y_96703 = ST_Y(the_geom_96703);