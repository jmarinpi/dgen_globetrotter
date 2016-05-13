set role 'dgeo-writers';

-- add a new field
ALTER TABLE dgeo.bht_compilation
ADD grid_gid integer;

UPDATE dgeo.bht_compilation a
set grid_gid = b.gid
from dgeo.smu_t35km_2016 b
where ST_Intersects(a.the_geom_96703, b.the_geom_96703);
-- 346846 ROWS