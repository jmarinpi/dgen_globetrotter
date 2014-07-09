--------------------------------------------------------------------------------------------------------------------
-- us
--------------------------------------------------------------------------------------------------------------------

-- sum the total population in each electric service territory
-- create output table
DROP TABLE IF EXISTS dg_wind.ventyx_res_pop_sums_us CASCADE;
CREATE TABLE dg_wind.ventyx_res_pop_sums_us
(gid integer,
pop numeric,
cell_count numeric);

-- use dg_wind.ventyx_ests_backfilled_geoms_clipped
-- run parsel

-- if its still too slow, need to try to split up on something other than gid

select parsel_2('dav-gis','dg_wind.ventyx_backfilled_ests_diced','state_id',
'WITH tile_stats as (
	select a.est_gid as gid,
		ST_SummaryStats(ST_Clip(b.rast, 1, a.the_geom_4326, true)) as stats
	FROM dg_wind.ventyx_backfilled_ests_diced as a
	INNER JOIN dg_wind.ls2012_ntime_res_pop_us_100x100 b
	ON ST_Intersects(a.the_geom_4326,b.rast)
)
	--aggregate the results from each tile
SELECT gid, sum((stats).sum) as pop, sum((stats).count) as cell_count
FROM tile_stats
GROUP by gid;'
,'dg_wind.ventyx_res_pop_sums_us','a',16);
-- run time = ~10 minutes

-- check for service territories that do not have population
DROP TABLE IF EXISTS dg_wind.ests_w_no_pop;
CREATE TABLE dg_wind.ests_w_no_pop AS
	SELECT a.gid, a.state_abbr,
		a.the_geom_4326, 
		a.total_residential_sales_mwh, c.pop, c.cell_count
	FROM dg_wind.ventyx_elec_serv_territories_w_2011_sales_data_backfilled_clip a
	left join dg_wind.ventyx_res_pop_sums_us c
	ON a.gid = c.gid
	where (c.pop is null or c.pop = 0)
	and a.state_abbr not in ('AK','HI');
-- review in Q
-- these are all just small territories with no ntime population
-- in these cases, just spread the data around evenly


-- perform map algebra to estimate the percent of each raster
DROP TABLE IF EXISTS dg_wind.disaggregated_load_residential_us;
CREATE TABLE dg_wind.disaggregated_load_residential_us (
	tile_id integer,
	rast raster);

-- need to add something in here to use the diced geoms with state_id

--- run parsel
select parsel_2('dav-gis',' dg_wind.ventyx_backfilled_ests_diced','state_id',
'WITH clip as (
select a.gid, c.pop, c.cell_count, b.rid,
	ST_Clip(b.rast, 1, x.the_geom_4326, true) as rast,
	CASE WHEN c.pop is null or c.pop = 0 THEN ''([rast]+1.)/'' || c.cell_count || ''*'' || a.total_residential_sales_mwh
	ELSE ''[rast]/'' || c.pop || ''*'' || a.total_residential_sales_mwh 
	END as map_alg_expr

FROM dg_wind.ventyx_backfilled_ests_diced x

LEFT JOIN dg_wind.ventyx_elec_serv_territories_w_2011_sales_data_backfilled_clip as a
ON x.est_gid = a.gid

INNER JOIN dg_wind.ls2012_ntime_res_pop_us_100x100 b
ON ST_Intersects(x.the_geom_4326,b.rast)

LEFT JOIN dg_wind.ventyx_res_pop_sums_us c
ON x.est_gid = c.gid

where c.cell_count > 0 and a.total_residential_sales_mwh >= 0) 
SELECT rid as tile_id, ST_MapAlgebraExpr(rast, ''32BF'', map_alg_expr) as rast
FROM clip;','dg_wind.disaggregated_load_residential_us','x',16);
--  runtime = ~74 minutes

-- add rid primary key column
ALTER TABLE dg_wind.disaggregated_load_residential_us
ADD COLUMN rid serial;

ALTER TABLE dg_wind.disaggregated_load_residential_us
ADD PRIMARY KEY (rid);

-- aggregate the results into tiles
DROP TABLE IF EXISTS dg_wind.mosaic_load_residential_us;
CREATE TABLE dg_wind.mosaic_load_residential_us 
	(rid integer,
	rast raster);

select parsel_2('dav-gis','dg_wind.disaggregated_load_residential_us','tile_id',
'SELECT a.tile_id as rid, ST_Union(a.rast,''SUM'') as rast
FROM dg_wind.disaggregated_load_residential_us a
GROUP BY a.tile_id;','dg_wind.mosaic_load_residential_us','a',16);
-- run time = 830647.564 ms (14 mins)

-- create spatial index on this file
CREATE INDEX mosaic_load_residential_us_rast_gist
  ON dg_wind.mosaic_load_residential_us
  USING gist
  (st_convexhull(rast));

-- then sum to counties
-- create output table
DROP TABLE IF EXISTS dg_wind.res_load_by_county_us;
CREATE TABLE dg_wind.res_load_by_county_us
(county_id integer,
total_load_mwh_2011_residential numeric);

-- run parsel
select parsel_2('dav-gis','diffusion_shared.county_geom','county_id',
'WITH tile_stats as (
	select a.county_id,
		ST_SummaryStats(ST_Clip(b.rast, 1, a.the_geom_4326, true)) as stats
	FROM diffusion_shared.county_geom as a
	INNER JOIN dg_wind.mosaic_load_residential_us b
	ON ST_Intersects(a.the_geom_4326,b.rast)
)
	--aggregate the results from each tile
SELECT county_id, sum((stats).sum) as total_load_mwh_2011_residential
FROM tile_stats
GROUP by county_id;'
,'dg_wind.res_load_by_county_us','a',16);

-- do some additional verification
SELECT sum(total_load_mwh_2011_residential)
FROM dg_wind.res_load_by_county_us; -- 1,417,737,929.8571711127352

select sum(total_residential_sales_mwh)
FROM dg_wind.ventyx_elec_serv_territories_w_2011_sales_data_backfilled_clip
where state_abbr not in ('AK','HI'); -- 1,417,737,928

select 1417737928 - 1417737929.8571711127352; -- -1.8571711127352 (difference likely due to rounding)
select (1417737928 - 1417737929.8571711127352)/1417737928  * 100; -- -0.0000001309953748190335499000 % load is missing nationally

-- cehck on state level
with a as (
select state_abbr, sum(total_residential_sales_mwh)
FROM dg_wind.ventyx_elec_serv_territories_w_2011_sales_data_backfilled_clip
where state_abbr not in ('AK','HI')
GROUP BY state_abbr),

b as (

SELECT k.state_abbr, sum(total_load_mwh_2011_residential)
FROM dg_wind.res_load_by_county_us j
LEFT join diffusion_shared.county_geom k
ON j.county_id = k.county_id
GROUP BY k.state_abbr)

SELECT a.state_abbr, a.sum as est_total, b.sum as county_total, b.sum-a.sum as diff, (b.sum-a.sum)/a.sum * 100 as perc_diff
FROM a
LEFT JOIN b
on a.state_abbr = b.state_abbr
order by a.state_abbr; --
-- looks good -- these differcnces are probably due to incongruencies between county_geoms and the ventyx state boundaries

select *
FROM dg_wind.res_load_by_county_us
where total_load_mwh_2011_residential = 0;

select *
FROM diffusion_shared.county_geom
where county_id = 2988;

----------------------------------------------------------------------------------------------------
-- repeat for number of customers
----------------------------------------------------------------------------------------------------
-- check that there aren't any territories with residential customers but zero load
SELECT count(*)
FROM dg_wind.ventyx_elec_serv_territories_w_2011_sales_data_backfilled_clip
where total_residential_customers > 0 and (total_residential_sales_mwh <= 0 or total_residential_sales_mwh is null)

-- 1 - create the disag raster table
DROP TABLE IF EXISTS dg_wind.disaggregated_customers_residential_us;
CREATE TABLE dg_wind.disaggregated_customers_residential_us (
	tile_id integer,
	rast raster);

--- run parsel
select parsel_2('dav-gis',' dg_wind.ventyx_backfilled_ests_diced','state_id',
'WITH clip as (
select a.gid, c.pop, c.cell_count, b.rid,
	ST_Clip(b.rast, 1, x.the_geom_4326, true) as rast,
	CASE WHEN c.pop is null or c.pop = 0 THEN ''([rast]+1.)/'' || c.cell_count || ''*'' || a.total_residential_customers
	ELSE ''[rast]/'' || c.pop || ''*'' || a.total_residential_customers 
	END as map_alg_expr

FROM dg_wind.ventyx_backfilled_ests_diced x

LEFT JOIN dg_wind.ventyx_elec_serv_territories_w_2011_sales_data_backfilled_clip as a
ON x.est_gid = a.gid

INNER JOIN dg_wind.ls2012_ntime_res_pop_us_100x100 b
ON ST_Intersects(x.the_geom_4326,b.rast)

LEFT JOIN dg_wind.ventyx_res_pop_sums_us c
ON x.est_gid = c.gid

where c.cell_count > 0 and a.total_residential_customers >= 0) 
SELECT rid as tile_id, ST_MapAlgebraExpr(rast, ''32BF'', map_alg_expr) as rast
FROM clip;','dg_wind.disaggregated_customers_residential_us','x',16);
--  runtime = ~74 minutes (3950668.635 ms)

-- add rid primary key column
ALTER TABLE dg_wind.disaggregated_customers_residential_us
ADD COLUMN rid serial;

ALTER TABLE dg_wind.disaggregated_customers_residential_us
ADD PRIMARY KEY (rid);

-- 2 - aggregate the results into tiles
DROP TABLE IF EXISTS dg_wind.mosaic_customers_residential_us;
CREATE TABLE dg_wind.mosaic_customers_residential_us 
	(rid integer,
	rast raster);

select parsel_2('dav-gis','dg_wind.disaggregated_customers_residential_us','tile_id',
'SELECT a.tile_id as rid, ST_Union(a.rast,''SUM'') as rast
FROM dg_wind.disaggregated_customers_residential_us a
GROUP BY a.tile_id;','dg_wind.mosaic_customers_residential_us','a',16);
-- run time = 830647.564 ms (14 mins)

-- create spatial index on this file
CREATE INDEX mosaic_customers_residential_us_rast_gist
  ON dg_wind.mosaic_customers_residential_us
  USING gist
  (st_convexhull(rast));

-- 3- then sum to counties
-- create output table
DROP TABLE IF EXISTS dg_wind.res_customers_by_county_us;
CREATE TABLE dg_wind.res_customers_by_county_us
(county_id integer,
total_customers_2011_residential numeric);

-- run parsel
select parsel_2('dav-gis','diffusion_shared.county_geom','county_id',
'WITH tile_stats as (
	select a.county_id,
		ST_SummaryStats(ST_Clip(b.rast, 1, a.the_geom_4326, true)) as stats
	FROM diffusion_shared.county_geom as a
	INNER JOIN dg_wind.mosaic_customers_residential_us b
	ON ST_Intersects(a.the_geom_4326,b.rast)
)
	--aggregate the results from each tile
SELECT county_id, sum((stats).sum) as total_customers_2011_residential
FROM tile_stats
GROUP by county_id;'
,'dg_wind.res_customers_by_county_us','a',16);

-- 4 - do some verification
SELECT sum(total_customers_2011_residential)
FROM dg_wind.res_customers_by_county_us; -- 125,451,686.038416417944474

select sum(total_residential_customers)
FROM dg_wind.ventyx_elec_serv_territories_w_2011_sales_data_backfilled_clip
where state_abbr not in ('AK','HI'); -- 125,451,686

select 125451686 - 125451686.038416417944474; -- -0.038416417944474 (difference likely due to rounding)
select (125451686 - 125451686.038416417944474)/125451686  * 100; -- -0.0000000306224803901591246800 % load is missing nationally

-- cehck on state level
with a as (
select state_abbr, sum(total_residential_customers)
FROM dg_wind.ventyx_elec_serv_territories_w_2011_sales_data_backfilled_clip
where state_abbr not in ('AK','HI')
GROUP BY state_abbr),

b as (

SELECT k.state_abbr, sum(total_customers_2011_residential)
FROM dg_wind.res_customers_by_county_us j
LEFT join diffusion_shared.county_geom k
ON j.county_id = k.county_id
GROUP BY k.state_abbr)

SELECT a.state_abbr, a.sum as est_total, b.sum as county_total, b.sum-a.sum as diff, (b.sum-a.sum)/a.sum * 100 as perc_diff
FROM a
LEFT JOIN b
on a.state_abbr = b.state_abbr
order by a.state_abbr; --

select *
FROM dg_wind.res_customers_by_county_us
where total_customers_2011_residential = 0;
-- looks good -- these differcnces are probably due to incongruencies between county_geoms and the ventyx state boundaries

