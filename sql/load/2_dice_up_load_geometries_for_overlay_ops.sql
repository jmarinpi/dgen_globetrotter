-- create fishnet (2 decimal degrees)
DROP TABLE IF EXISTS dg_wind.us_fishnet_p5dd;
CREATE TABLE dg_wind.us_fishnet_p5dd AS
SELECT ST_SetSrid(ST_Fishnet('dg_wind.ventyx_elec_serv_territories_w_2011_sales_data_backfilled_clip','the_geom_4326',0.5),4326) as the_geom_4326;

CREATE INDEX us_fishnet_p5dd_the_geom_4326_gist ON dg_wind.us_fishnet_p5dd using gist(the_geom_4326);

-- dice up the backfilled geoms
DROP TABLE IF EXISTS dg_wind.ventyx_backfilled_ests_diced;
CREATE TABLE dg_wind.ventyx_backfilled_ests_diced AS
SELECT a.gid as est_gid, a.state_abbr, ST_Intersection(a.the_geom_4326, b.the_geom_4326) as the_geom_4326
FROM dg_wind.ventyx_elec_serv_territories_w_2011_sales_data_backfilled_clip a
INNER JOIN dg_wind.us_fishnet_p5dd b
ON ST_Intersects(a.the_geom_4326, b.the_geom_4326);

-- rename the table -- only have to do this once
ALTER TABLE dg_wind.ventyx_backfilled_ests_diced2 RENAME TO ventyx_backfilled_ests_diced;

-- add a new id, called the state_id, which will allow me to parsel the data up more efficiently
ALTER TABLE dg_wind.ventyx_backfilled_ests_diced ADD COLUMN state_id integer;

with b as (
	SELECT distinct(state_abbr)
	FROM dg_wind.ventyx_backfilled_ests_diced),
c as (
	 SELECT state_abbr, row_number() over () as state_id 
	 from b)
UPDATE dg_wind.ventyx_backfilled_ests_diced a
SET state_id = c.state_id
FROM c
WHERe a.state_abbr = c.state_abbr;

CREATE INDEX  ventyx_backfilled_ests_diced_state_id_btree 
ON dg_wind.ventyx_backfilled_ests_diced
USING btree(state_id);

CREATE INDEX ventyx_backfilled_ests_diced_the_geom_4326_gist ON dg_wind.ventyx_backfilled_ests_diced USING gist(the_geom_4326);

CLUSTER dg_wind.ventyx_backfilled_ests_diced USING ventyx_backfilled_ests_diced_state_id_btree;

CREATE INDEX ventyx_backfilled_ests_diced2_est_gid_btree ON dg_wind.ventyx_backfilled_ests_diced USING btree(est_gid);

VACUUM ANALYZE dg_wind.ventyx_backfilled_ests_diced;

-- make sure that each gid is only tied to a single state_id
select est_gid, count(distinct(state_id))
FROM dg_wind.ventyx_backfilled_ests_diced
group by est_gid
order by count desc;