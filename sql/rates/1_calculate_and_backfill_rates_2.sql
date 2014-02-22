-- calculate rates
-- use the ventyx/eia service territory data, where available
DROP TABLE IF EXISTS dg_wind.electricity_rates_2011_backfilled;
CREATE TABLE dg_wind.electricity_rates_2011_backfilled AS
SELECT company_id, state_name, the_geom_4326,
	CASE WHEN total_residential_revenue_000s = 0 and total_residential_sales_mwh = 0 THEN NULL
	   ElSE (total_residential_revenue_000s*1000*100)/(total_residential_sales_mwh*1000)
	 end as res_cents_per_kwh,

	CASE WHEN total_commercial_revenue_000s = 0 and total_commercial_sales_mwh = 0 THEN NULL
	   ElSE (total_commercial_revenue_000s*1000*100)/(total_commercial_sales_mwh*1000)
	 end as comm_cents_per_kwh,

	CASE WHEN total_industrial_revenue_000s = 0 and total_industrial_sales_mwh = 0 THEN NULL
	   ElSE (total_industrial_revenue_000s*1000*100)/(total_industrial_sales_mwh*1000)
	 end as ind_cents_per_kwh,
	 data_year,
	 source
FROM dg_wind.ventyx_ests_2011_sales_data_backfilled
where source = 'Ventyx Sales Data Join'  -- only for directly joined ventyx data -- not for backfilled state level remainders	 

UNION
-- backfill all other areas with state averages
SELECT -99999 as company_id, a.state_name, a.the_geom_4326,
	b.res as res_cents_per_kwh,
	b.com as com_cents_per_kwh,
	b.ind as ind_cents_per_kwh,
       2011 as data_year,
       'EIA Table 4 Rates by State 2011'::text as source
FROM dg_wind.ventyx_ests_2011_sales_data_backfilled a
LEFT JOIN eia.table_4_rates_by_state_2011 b
ON a.state_name = b.state
where a.source <> 'Ventyx Sales Data Join'; -- back fill the rest with 2011 state level averages from eia table 4

CREATE INDEX electricity_rates_2011_backfilled_the_geom_4326_gist ON dg_wind.electricity_rates_2011_backfilled using gist(the_geom_4326);

-- add geom for 900914 and index
ALTER TABLE dg_wind.electricity_rates_2011_backfilled ADD COLUMN the_geom_900914 geometry;
UPDATE dg_wind.electricity_rates_2011_backfilled  
SET the_geom_900914 = ST_Transform(the_geom_4326,900914);

CREATE INDEX electricity_rates_2011_backfilled_the_geom_900914_gist ON dg_wind.electricity_rates_2011_backfilled using gist(the_geom_900914);

ALTER TABLE dg_wind.electricity_rates_2011_backfilled ADD COLUMN gid serial;

-- export to shapefile
-- in ArcGIS, convert to a coverage, then back to a shapefile
-- delete all columns from the shapefile -- we only care about the geometry
-- reload the shapefile as dg_wind.est_rate_geoms_no_overlaps
-- repair the geometry

-- add spatial indices on st centroid and st point on surface
-- DROP spatial index
DROP INDEX dg_wind.est_rate_geoms_no_overlaps_the_geom_900914_gist;
ALTER TABLE dg_wind.est_rate_geoms_no_overlaps ALTER the_geom_900914 type geometry;
-- fix broken geometries
UPDATE dg_wind.est_rate_geoms_no_overlaps
SET the_geom_900914 = ST_Buffer(the_geom_900914,0)
where ST_Isvalid(the_geom_900914) = false;
-- make sure they were all dealt with
SELECT st_isvalidreason(the_geom_900914)
FROM dg_wind.est_rate_geoms_no_overlaps
where ST_Isvalid(the_geom_900914) = false;
-- re-create spatial index
CREATE INDEX est_rate_geoms_no_overlaps_the_geom_900914_gist
  ON dg_wind.est_rate_geoms_no_overlaps
  USING gist
  (the_geom_900914);
-- add the geom_4326 as a column
ALTER TABLE dg_wind.est_rate_geoms_no_overlaps ADD COLUMN the_geom_4326 geometry;
UPDATE dg_wind.est_rate_geoms_no_overlaps
SET the_geom_4326 = ST_Transform(the_geom_900914,4326);
-- create index for it
CREATE INDEX est_rate_geoms_no_overlaps_the_geom_4326_gist
ON dg_wind.est_rate_geoms_no_overlaps
USING gist
(the_geom_4326);
 
-- add column for point on surface
ALTER TABLE dg_wind.est_rate_geoms_no_overlaps ADD COLUMN the_point_on_surface_4326 geometry;
-- populate that column
UPDATE dg_wind.est_rate_geoms_no_overlaps
SET the_point_on_surface_4326 = ST_Transform(ST_PointOnSurface(the_geom_900914),4326); -- need to do it this way because the_geom_4326 has invalid geoms and if i try to fix them, polygons disappear
-- create spatial index
CREATE INDEX est_rate_geoms_no_overlaps_the_geom_4326_pointonsurface_gist ON dg_wind.est_rate_geoms_no_overlaps USING gist(the_point_on_surface_4326);
VACUUM ANALYZE dg_wind.est_rate_geoms_no_overlaps;

-- then intersect the centroid/point on surface against the original data, 
-- and populate the rates based on averages of all intersections
DROP TABLE IF EXISTS dg_wind.electricity_rates_2011_backfilled_no_overlaps;
CREATE TABLE dg_wind.electricity_rates_2011_backfilled_no_overlaps AS
WITH ix AS (
SELECT a.gid, a.the_geom_4326, 
	b.company_id, b.state_name, b.res_cents_per_kwh, b.comm_cents_per_kwh, 
       b.ind_cents_per_kwh, b.data_year, b.source
FROM dg_wind.est_rate_geoms_no_overlaps a
INNER JOIN dg_wind.electricity_rates_2011_backfilled b
ON ST_Intersects(a.the_point_on_surface_4326,b.the_geom_4326))

SELECT gid, the_geom_4326, array_agg(company_id) as company_id, first(state_name) as state_name,
	avg(res_cents_per_kwh) as res_cents_per_kwh,
	avg(comm_cents_per_kwh) as comm_cents_per_kwh,
	avg(ind_cents_per_kwh) as ind_cents_per_kwh,
	array_agg(data_year) as data_year, array_agg(source) as source
FROM ix
GROUP BY gid, the_geom_4326;
-- 

-- inspect results in Q -- looks good

-- **
-- move results to wind_ds
-- ALTER TABLE wind_ds.annual_ave_elec_rates_2011 RENAME TO annual_ave_elec_rates_2011_old;
-- ALTER TABLE wind_ds.annual_ave_elec_rates_2011_old SET SCHEMA wind_ds_data;
dROP TABLE IF EXISTS wind_ds.annual_ave_elec_rates_2011;
CREATE TABLE wind_ds.annual_ave_elec_rates_2011 AS
SELECT *
FROM dg_wind.electricity_rates_2011_backfilled_no_overlaps;

CREATE INDEX annual_ave_elec_rates_2011_the_geom_4326_gist ON wind_ds.annual_ave_elec_rates_2011 USING gist(the_geom_4326);

ALTER TABLE wind_ds.annual_ave_elec_rates_2011 
ADD COLUMN the_geom_900914 geometry,
ADD COLUMN the_geom_900915 geometry,
ADD COLUMN the_geom_900916 geometry;

UPDATE wind_ds.annual_ave_elec_rates_2011
SET (the_geom_900914, the_geom_900915, the_geom_900916) = (ST_Transform(the_geom_4326,900914),ST_Transform(the_geom_4326,900915),ST_Transform(the_geom_4326,900916));

CREATE INDEX annual_ave_elec_rates_2011_the_geom_900914_gist ON wind_ds.annual_ave_elec_rates_2011 USING gist(the_geom_900914);
CREATE INDEX annual_ave_elec_rates_2011_the_geom_900915_gist ON wind_ds.annual_ave_elec_rates_2011 USING gist(the_geom_900915);
CREATE INDEX annual_ave_elec_rates_2011_the_geom_900916_gist ON wind_ds.annual_ave_elec_rates_2011 USING gist(the_geom_900916);

ALTER TABLE wind_ds.annual_ave_elec_rates_2011 ADD PRIMARY KEY (gid);


VACUUM ANALYZE wind_ds.annual_ave_elec_rates_2011;