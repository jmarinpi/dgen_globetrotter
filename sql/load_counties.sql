INSERT INTO wind_ds.county_geom (county_id,county,state,state_abbr, the_geom_4326)
SELECT a.gid as county_id, a.NAME as county, a.state_name as state, b.state_abbr, a.the_geom_4326
FROM esri.dtl_cnty_all_multi a
LEFT JOIN esri.dtl_state b
ON a.state_name = b.state_name

ALTER TABLE wind_ds.county_geom ADD COLUMN census_division_abbr character varying(3);

UPDATE wind_ds.county_geom a
SET census_division_abbr = b.division_abbr
FROM eia.census_regions b
WHERE a.state = b.state_name;

-- add other srid geometries and indices
ALTER TABLE  wind_ds.county_geom 
ADD COLUMN the_geom_900914 geometry;

UPDATE wind_ds.county_geom SET the_geom_900914 = ST_Transform(the_geom_4326,900914);

ALTER TABLE  wind_ds.county_geom 
ADD COLUMN the_geom_900915 geometry;

UPDATE wind_ds.county_geom SET the_geom_900915 = ST_Transform(the_geom_4326,900915);

ALTER TABLE  wind_ds.county_geom 
ADD COLUMN the_geom_900916 geometry;

UPDATE wind_ds.county_geom SET the_geom_900916 = ST_Transform(the_geom_4326,900916);

CREATE INDEX county_geom_the_geom_900914_gist ON wind_ds.county_geom USING gist(the_geom_900914);
CREATE INDEX county_geom_the_geom_900915_gist ON wind_ds.county_geom USING gist(the_geom_900915);
CREATE INDEX county_geom_the_geom_900916_gist ON wind_ds.county_geom USING gist(the_geom_900916);

CLUSTER wind_ds.county_geom USING county_geom_the_geom_4326_gist;

VACUUM ANALYZE wind_ds.county_geom;
