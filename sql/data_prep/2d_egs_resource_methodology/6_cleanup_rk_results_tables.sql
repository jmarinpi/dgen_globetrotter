set role 'dgeo-writers';

drop table if exists dgeo.egs_temp_at_depth_all_update;
CREATE TABLE dgeo.egs_temp_at_depth_all_update AS
SELECT a.x, a.y, 
	ST_SetSRID(ST_MakePoint(a.x, a.y), 96703) as the_geom_96703,
	a.est as t_500,
	a.ci95 as ci95_500,
	b.est as t_1000,
	b.ci95 as ci95_1000,
	c.est as t_1500,
	c.ci95 as ci95_1500,
	d.est as t_2000,
	d.ci95 as ci95_2000,
	e.est as t_2500,
	e.ci95 as ci95_2500,
	f.est as t_3000,
	a.ci95 as ci95_3000	
FROM dgeo.egs_temp_at_depth_update_500 a
LEFT JOIN dgeo.egs_temp_at_depth_update_1000 b
ON a.x = b.x
and a.y = b.y
LEFT JOIN dgeo.egs_temp_at_depth_update_1500 c
ON a.x = c.x
and a.y = c.y
LEFT JOIN dgeo.egs_temp_at_depth_update_2000 d
ON a.x = d.x
and a.y = d.y
LEFT JOIN dgeo.egs_temp_at_depth_update_2500 e
ON a.x = e.x
and a.y = e.y
LEFT JOIN dgeo.egs_temp_at_depth_update_3000 f
ON a.x = f.x
and a.y = f.y;
-- 117502

-- add primary key
ALTER TABLE dgeo.egs_temp_at_depth_all_update
ADD column gid integer;

UPDATE dgeo.egs_temp_at_depth_all_update a
SET gid = b.gid
from dgeo.smu_t35km_2016 b
where a.x = b.x_96703
and a.y = b.y_96703;

-- make sure no nulls
select count(*)
FROM dgeo.egs_temp_at_depth_all_update
where gid is null;
-- 0 -- all set


-- bring in the grid cell boundaries too
ALTER TABLE dgeo.egs_temp_at_depth_all_update
ADD COLUMN the_geom_96703 geometry;

UPDATE dgeo.egs_temp_at_depth_all_update a
SET the_geom_96703 = b.the_geom_96703
from dgeo.smu_t35km_2016 b
where a.gid = b.gid;

-- add a 4326 geom
ALTER TABLE dgeo.egs_temp_at_depth_all_update
ADD column the_geom_4326 geometry;

UPDATE dgeo.egs_temp_at_depth_all_update
set the_geom_4326 = ST_Transform(the_geom_96703, 4326);

-- add index on geoms
CREATE INDEX egs_temp_at_depth_all_update_gist_the_geom_96703
ON dgeo.egs_temp_at_depth_all_update
USING GIST(the_geom_96703);

CREATE INDEX egs_temp_at_depth_all_update_gist_the_geom_4326
ON dgeo.egs_temp_at_depth_all_update
USING GIST(the_geom_4326);


-- investigate the data in Q
