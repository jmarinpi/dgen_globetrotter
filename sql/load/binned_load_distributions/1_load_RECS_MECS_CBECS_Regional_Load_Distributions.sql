DROP TABLE IF EXISTS wind_ds.binned_annual_load_kwh_10_bins;
CREATE TABLE wind_ds.binned_annual_load_kwh_10_bins (
	row_id integer,
	ann_cons_kwh numeric,
	prob numeric,
	census_region_code integer,
	sector text,
	weight numeric);

SET ROLE 'server-superusers';
COPY wind_ds.binned_annual_load_kwh_10_bins FROM '/srv/home/mgleason/data/dg_wind/binned_load_distributions/annual_load_kwh_10_bins.csv' with csv header;
RESET ROLE;

DROP TABLE IF EXISTS wind_ds.binned_annual_load_kwh_50_bins;
CREATE TABLE wind_ds.binned_annual_load_kwh_50_bins (
	row_id integer,
	ann_cons_kwh numeric,
	prob numeric,
	census_region_code integer,
	sector text,
	weight numeric);

SET ROLE 'server-superusers';
COPY wind_ds.binned_annual_load_kwh_50_bins FROM '/srv/home/mgleason/data/dg_wind/binned_load_distributions/annual_load_kwh_50_bins.csv' with csv header;
RESET ROLE;

DROP TABLE IF EXISTS wind_ds.binned_annual_load_kwh_100_bins;
CREATE TABLE wind_ds.binned_annual_load_kwh_100_bins (
	row_id integer,
	ann_cons_kwh numeric,
	prob numeric,
	census_region_code integer,
	sector text,
	weight numeric);

SET ROLE 'server-superusers';
COPY wind_ds.binned_annual_load_kwh_100_bins FROM '/srv/home/mgleason/data/dg_wind/binned_load_distributions/annual_load_kwh_100_bins.csv' with csv header;
RESET ROLE;

DROP TABLE IF EXISTS wind_ds.binned_annual_load_kwh_500_bins;
CREATE TABLE wind_ds.binned_annual_load_kwh_500_bins (
	row_id integer,
	ann_cons_kwh numeric,
	prob numeric,
	census_region_code integer,
	sector text,
	weight numeric);

SET ROLE 'server-superusers';
COPY wind_ds.binned_annual_load_kwh_500_bins FROM '/srv/home/mgleason/data/dg_wind/binned_load_distributions/annual_load_kwh_500_bins.csv' with csv header;
RESET ROLE;

-- drop the row_id column (it is meaningless)
ALTER TABLE wind_ds.binned_annual_load_kwh_10_bins DROP COLUMN row_id;
ALTER TABLE wind_ds.binned_annual_load_kwh_50_bins DROP COLUMN row_id;
ALTER TABLE wind_ds.binned_annual_load_kwh_100_bins DROP COLUMN row_id;
ALTER TABLE wind_ds.binned_annual_load_kwh_500_bins DROP COLUMN row_id;

-- create index for each table on the sector column
CREATE INDEX binned_annual_load_kwh_10_bins_sector_btree ON wind_ds.binned_annual_load_kwh_10_bins using btree(sector);
CREATE INDEX binned_annual_load_kwh_50_bins_sector_btree ON wind_ds.binned_annual_load_kwh_50_bins using btree(sector);
CREATE INDEX binned_annual_load_kwh_100_bins_sector_btree ON wind_ds.binned_annual_load_kwh_100_bins using btree(sector);
CREATE INDEX binned_annual_load_kwh_500_bins_sector_btree ON wind_ds.binned_annual_load_kwh_500_bins using btree(sector);

-- add census_region (text) column
ALTER TABLE wind_ds.binned_annual_load_kwh_10_bins ADD COLUMN census_region text;
ALTER TABLE wind_ds.binned_annual_load_kwh_50_bins ADD COLUMN census_region text;
ALTER TABLE wind_ds.binned_annual_load_kwh_100_bins ADD COLUMN census_region text;
ALTER TABLE wind_ds.binned_annual_load_kwh_500_bins ADD COLUMN census_region text;

-- update the census region column
UPDATE wind_ds.binned_annual_load_kwh_10_bins
SET census_region = Case when census_region_code = 1 then 'Northeast'
			 when census_region_code = 2 then 'Midwest'
			 when census_region_code = 3 then 'South'
			 when census_region_code = 4 then 'West'
		    end;

UPDATE wind_ds.binned_annual_load_kwh_50_bins
SET census_region = Case when census_region_code = 1 then 'Northeast'
			 when census_region_code = 2 then 'Midwest'
			 when census_region_code = 3 then 'South'
			 when census_region_code = 4 then 'West'
		    end;

UPDATE wind_ds.binned_annual_load_kwh_100_bins
SET census_region = Case when census_region_code = 1 then 'Northeast'
			 when census_region_code = 2 then 'Midwest'
			 when census_region_code = 3 then 'South'
			 when census_region_code = 4 then 'West'
		    end;

UPDATE wind_ds.binned_annual_load_kwh_500_bins
SET census_region = Case when census_region_code = 1 then 'Northeast'
			 when census_region_code = 2 then 'Midwest'
			 when census_region_code = 3 then 'South'
			 when census_region_code = 4 then 'West'
		    end;


-- fix the sector column (industry --> industrial)
UPDATE wind_ds.binned_annual_load_kwh_10_bins
set sector = 'industrial'
where sector = 'industry';

UPDATE wind_ds.binned_annual_load_kwh_50_bins
set sector = 'industrial'
where sector = 'industry';

UPDATE wind_ds.binned_annual_load_kwh_100_bins
set sector = 'industrial'
where sector = 'industry';

UPDATE wind_ds.binned_annual_load_kwh_500_bins
set sector = 'industrial'
where sector = 'industry';


-- add index to the census region column
CREATE INDEX binned_annual_load_kwh_10_bins_census_region_btree ON wind_ds.binned_annual_load_kwh_10_bins using btree(census_region);
CREATE INDEX binned_annual_load_kwh_50_bins_census_region_btree ON wind_ds.binned_annual_load_kwh_50_bins using btree(census_region);
CREATE INDEX binned_annual_load_kwh_100_bins_census_region_btree ON wind_ds.binned_annual_load_kwh_100_bins using btree(census_region);
CREATE INDEX binned_annual_load_kwh_500_bins_census_region_btree ON wind_ds.binned_annual_load_kwh_500_bins using btree(census_region);

-- vacuum/analyze
VACUUM ANALYZE wind_ds.binned_annual_load_kwh_10_bins;

VACUUM ANALYZE wind_ds.binned_annual_load_kwh_50_bins;

VACUUM ANALYZE wind_ds.binned_annual_load_kwh_100_bins;

VACUUM ANALYZE wind_ds.binned_annual_load_kwh_500_bins;