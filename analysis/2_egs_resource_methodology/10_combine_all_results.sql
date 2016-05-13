set role 'dgeo-writers';


-- combine all results into a single table
DROP TABLE IF EXISTS dgeo.egs_resource_shallow_lowt;
CREATE TABLE dgeo.egs_resource_shallow_lowt AS
SELECT  a.gid,
	a.the_geom_4326,
	a.the_geom_96703,
	a.x,
	a.y,
	a.area_sqm,
	a.ci95_500::NUMERIC as t_ci95,
	a.t_500::NUMERIC as t500est,
	a.t_500::NUMERIC - ci95_500::NUMERIC as t500min,
	a.t_500::NUMERIC + ci95_500::NUMERIC as t500max,
	a.t_1000::NUMERIC as t1000est,
	a.t_1000::NUMERIC - ci95_1000::NUMERIC as t1000min,
	a.t_1000::NUMERIC + ci95_1000::NUMERIC as t1000max,
	a.t_1500::NUMERIC as t1500est,
	a.t_1500::NUMERIC - ci95_1500::NUMERIC as t1500min,
	a.t_1500::NUMERIC + ci95_1500::NUMERIC as t1500max,
	a.t_2000::NUMERIC as t2000est,
	a.t_2000::NUMERIC - ci95_2000::NUMERIC as t2000min,
	a.t_2000::NUMERIC + ci95_2000::NUMERIC as t2000max,
	a.t_2500::NUMERIC as t2500est,
	a.t_2500::NUMERIC - ci95_2500::NUMERIC as t2500min,
	a.t_2500::NUMERIC + ci95_2500::NUMERIC as t2500max,
	a.t_3000::NUMERIC as t3000est,
	a.t_3000::NUMERIC - ci95_3000::NUMERIC as t3000min,
	a.t_3000::NUMERIC + ci95_3000::NUMERIC as t3000max,
	b.res_500_est::NUMERIC AS res500est,
	b.res_500_max::NUMERIC AS res500max,
	b.res_500_min::NUMERIC AS res500min,
	b.res_1000_est::NUMERIC AS res1000est,
	b.res_1000_max::NUMERIC AS res1000max,
	b.res_1000_min::NUMERIC AS res1000min,
	b.res_1500_est::NUMERIC AS res1500est,
	b.res_1500_max::NUMERIC AS res1500max,
	b.res_1500_min::NUMERIC AS res1500min,
	b.res_2000_est::NUMERIC AS res2000est,
	b.res_2000_max::NUMERIC AS res2000max,
	b.res_2000_min::NUMERIC AS res2000min,
	b.res_2500_est::NUMERIC AS res2500est,
	b.res_2500_max::NUMERIC AS res2500max,
	b.res_2500_min::NUMERIC AS res2500min,
	b.res_3000_est::NUMERIC AS res3000est,
	b.res_3000_max::NUMERIC AS res3000max,
	b.res_3000_min::NUMERIC AS res3000min,
	c.res_tot_est::NUMERIC AS restotest,
	c.res_tot_max::NUMERIC AS restotmax,
	c.res_tot_min::NUMERIC AS restotmin
FROM dgeo.egs_temp_at_depth_all_update a
LEFT JOIN dgeo.egs_accessible_resource_by_depth b
ON a.gid = b.gid
LEFT JOIN dgeo.egs_accessible_resource_total c
ON a.gid = c.gid;
-- 473992 rows

-- add primary key
ALTER TABLE dgeo.egs_resource_shallow_lowt
ADD PRIMARY KEY (gid);

-- add index
CREATE INDEX egs_resource_shallow_lowt_the_geom_4326_gist
ON dgeo.egs_resource_shallow_lowt
USING GIST(the_geom_4326);

CREATE INDEX egs_resource_shallow_lowt_the_geom_96703_gist
ON dgeo.egs_resource_shallow_lowt
USING GIST(the_geom_96703);

-- check results
select 	round(sum(restotmin)/(1000 * 1e6),0) as min,
	round(sum(restotest)/(1000 * 1e6),0) as est,
	round(sum(restotmax)/(1000 * 1e6),0) as max
from dgeo.egs_resource_shallow_lowt; -- units will be in million twh
-- 514,796,1095


-- select 13.3*10^24/3.6e+12;
-- 3,694,444,444,444.44

-- add centroid
ALTER TABLE dgeo.egs_resource_shallow_lowt
ADD column the_pos_96703 geometry;

UPDATE dgeo.egs_resource_shallow_lowt
set the_pos_96703 = ST_PointOnSurface(the_geom_96703);

CREATE INDEX egs_resource_shallow_lowt_the_pos_96703_gist
ON dgeo.egs_resource_shallow_lowt
USING GIST(the_pos_96703);

-- add state_abbr
ALTER TABLE dgeo.egs_resource_shallow_lowt
ADD column state_abbr varchar(2);

UPDATE dgeo.egs_resource_shallow_lowt a
set state_abbr = b.state_abbr
from diffusion_blocks.county_geoms b
where ST_Intersects(a.the_pos_96703, b.the_geom_96703_20m);
-- 473399 rows

-- check for nulls
select count(*)
FROM dgeo.egs_resource_shallow_lowt 
where state_abbr is null;
-- 593

-- for these, use the full geom
UPDATE dgeo.egs_resource_shallow_lowt a
set state_abbr = b.state_abbr
from diffusion_blocks.county_geoms b
where ST_Intersects(a.the_geom_96703, b.the_geom_96703_20m)
and a.state_abbr is null;
-- 559

-- for the remainder, inspect in Q
-- they are all offshore, so assume they are not within any state