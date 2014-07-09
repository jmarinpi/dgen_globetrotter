-- load the dg_wind temporarily to remove dupes for each state
DROP TABLE IF EXISTS dg_wind.rate_escalations;
CREATE TABLE dg_wind.rate_escalations (
	census_division text,
	sector text,
	year integer,
	escalation_factor numeric);

-- load the data
SET ROLE 'server-superusers';
COPY dg_wind.rate_escalations FROM '/srv/home/mgleason/data/dg_wind/rate_esc_by_state_and_sector.csv' with csv header;
RESET ROLE;

-- add census division abbr column
ALTER TABLE dg_wind.rate_escalations
ADD COLUMN census_division_abbr text;

UPDATE  dg_wind.rate_escalations a
SET census_division_abbr = b.division_abbr
FROM eia.census_regions_20140123 b
where a.census_division = b.division;

-- remove the duplicates for each state, and move to wind_ds (prior to doing this, make sure that this query's row count matches the row count if you remove escalation_factor from the query
CREATE TABLE diffusion_shared.rate_escalations AS
SELECT census_division_abbr, sector, year, escalation_factor
FROM dg_wind.rate_escalations
GROUP BY census_division_abbr, sector, year, escalation_factor;

-- set a check constraint for sector
ALTER TABLE diffusion_shared.rate_escalations 
ADD constraint sector_check check (sector in ('Residential','Commercial','Industrial'));

-- add source column
ALTER TABLE diffusion_shared.rate_escalations 
ADD COLUMN source text;

UPDATE  diffusion_shared.rate_escalations
set source = 'AEO2014';

-- add indices
CREATE INDEX rate_escalations_btree_year ON diffusion_shared.rate_escalations using btree(year);
CREATE INDEX rate_escalations_btree_census_division_abbr ON diffusion_shared.rate_escalations using btree(census_division_abbr);
CREATE INDEX rate_escalations_btree_sector ON diffusion_shared.rate_escalations using btree(sector);
CREATE INDEX rate_escalations_btree_source ON diffusion_shared.rate_escalations using btree(source);