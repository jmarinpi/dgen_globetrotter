-- create table by aggregating the resload from individual points to county level
CREATE TABLE wind_ds.load_by_county_us AS
SELECT county_id, sum(res_load) as total_load_mwh_2011_residential
FROM wind_ds.pt_grid_us_res
GROUP BY county_id;
-- 3109 rows (does not include ak and hi)

-- create index on the county id
CREATE INDEX load_by_county_us_county_id_btree ON wind_ds.load_by_county_us USING btree(county_id);

-- -- the residential load data can now be removed from the individual residential points, but back it up first
-- CREATE TABLE wind_ds_data.res_load_for_each_point AS
-- SELECT gid, county_id, res_load
-- FROM wind_ds.pt_grid_us_res;
-- 
-- -- remove it from the points
-- ALTER TABLE wind_ds.pt_grid_us_res
-- DROP COLUMN res_load;

-- join in commercial totals


-- join in industrial/mnfg totals