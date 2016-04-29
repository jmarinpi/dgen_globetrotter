set role 'dgeo-writers';

-- add a new field
ALTER TABLE dgeo.bht_compilation
ADD t35km numeric;

UPDATE dgeo.bht_compilation a
set t35km = temp_c
FROM dgeo.smu_t35km_2016 b
WHERE ST_Intersects(a.the_geom_96703, b.the_geom_96703);
-- 346846 rows affected

-- also in the values from the old smu map
ALTER TABLE dgeo.bht_compilation
ADD t35km_old numeric;

UPDATE dgeo.bht_compilation a
set t35km_old = temp_c
FROM dgeo.smu_t35km b
WHERE ST_Intersects(a.the_geom_96703, b.the_geom_96703);
-- 346251 rows