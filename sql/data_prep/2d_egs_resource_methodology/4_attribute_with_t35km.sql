set role 'dgeo-writers';

-- add a new primary key
ALTER TABLE dgeo.bht_smu
ADD t35km numeric;

UPDATE dgeo.bht_smu a
set t35km = temp_c
FROM dgeo.smu_t35km b
WHERE ST_Intersects(a.the_geom_96703, b.the_geom_96703);
