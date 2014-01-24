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

