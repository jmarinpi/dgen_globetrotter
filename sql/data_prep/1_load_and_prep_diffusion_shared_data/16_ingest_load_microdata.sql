-- load commercial building microdata
SET ROLE 'diffusion_shared-writers';
DROP TABLE IF EXISTS diffusion_shared.eia_microdata_cbecs_2003;
CREATE TABLE diffusion_shared.eia_microdata_cbecs_2003 
(
	pubid8	integer primary key,	--building identifier
	region8	integer,	--Census region 
	cendiv8	integer,	--Census division
	sqft8	integer,	--Square footage
	sqftc8	integer,	--Square footage category
	yrconc8	integer,	--year of construction category
	pba8	integer,	--principal building activity
	elused8	integer,	--electricity used
	ngused8	integer,	--natural gas used
	fkused8	integer,	--Fuel oil/diesel/kerosene used 
	prused8	integer,	--Bottled gas/LPG/propane used 
	stused8	integer,	--District steam used
	hwused8	integer,	--District hot water used
	adjwt8	numeric,	--Final full sample building weight
	stratum8 integer,	--Variance stratum
	pair8	integer,	--Variance unit 
	hdd658	integer,	--Heating degree days (base 65)
	cdd658	integer,	--Cooling degree days (base 65)
	mfused8	integer,	--Any major fuel used 
	mfbtu8	integer,	--Annual maj fuel consumption (thous Btu)
	mfexp8	integer,	--Annual major fuel expenditures ($)
	elcns8	integer,	--Annual electricity consumption (kWh)
	elbtu8	integer,	--Annual elec consumption (thous Btu)
	elexp8	integer,	--Annual electricity expenditures ($)
	zelcns8	integer,	--Imputed electricity consumption
	zelexp8	integer		--Imputed electricity expenditures 		
);


COMMENT ON TABLE diffusion_shared.eia_microdata_cbecs_2003 IS
'Data dictionary available at: http://www.eia.gov/consumption/commercial/data/2003/pdf/layouts&formats.pdf';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.pubid8 IS 'building identifier';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.region8 IS 'Census region ';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.cendiv8 IS 'Census division';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.sqft8 IS 'Square footage';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.sqftc8 IS 'Square footage category';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.yrconc8 IS 'year of construction category';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.pba8 IS 'principal building activity';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.elused8 IS 'electricity used';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.ngused8 IS 'natural gas used';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.fkused8 IS 'Fuel oil/diesel/kerosene used ';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.prused8 IS 'Bottled gas/LPG/propane used ';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.stused8 IS 'District steam used';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.hwused8 IS 'District hot water used';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.adjwt8 IS 'Final full sample building weight';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.stratum8 IS 'Variance stratum';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.pair8 IS 'Variance unit ';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.hdd658 IS 'Heating degree days (base 65)';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.cdd658 IS 'Cooling degree days (base 65)';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.mfused8 IS 'Any major fuel used ';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.mfbtu8 IS 'Annual maj fuel consumption (thous Btu)';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.mfexp8 IS 'Annual major fuel expenditures ($)';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.elcns8 IS 'Annual electricity consumption (kWh)';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.elbtu8 IS 'Annual elec consumption (thous Btu)';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.elexp8 IS 'Annual electricity expenditures ($)';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.zelcns8 IS 'Imputed electricity consumption';
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.zelexp8 IS 'Imputed electricity expenditures ';

SET ROLE 'server-superusers';
COPY diffusion_shared.eia_microdata_cbecs_2003	 FROM '/srv/home/mgleason/data/dg_wind/cbecs_file15.csv' with csv header;
SET ROLE 'diffusion_shared-writers';

ALTER TABLE diffusion_shared.eia_microdata_cbecs_2003
ADD COLUMN census_region text;

UPDATE diffusion_shared.eia_microdata_cbecs_2003
SET census_region = 
	CASE WHEN region8 = 1 THEN 'Northeast'
	     WHEN region8 = 2 THEN 'Midwest'
	     WHEN region8 = 3 then 'South'
	     WHEN region8 = 4 then 'West'
	END;


ALTER TABLE diffusion_shared.eia_microdata_cbecs_2003
ADD COLUMN census_division_abbr text;

UPDATE diffusion_shared.eia_microdata_cbecs_2003
SET census_division_abbr = 
	CASE WHEN cendiv8 = 1 THEN 'NE'
		WHEN cendiv8 = 2 THEN 'MA'
		WHEN cendiv8 = 3 THEN 'ENC'
		WHEN cendiv8 = 4 THEN 'WNC'
		WHEN cendiv8 = 5 THEN 'SA'
		WHEN cendiv8 = 6 THEN 'ESC'
		WHEN cendiv8 = 7 THEN 'WSC'
		WHEN cendiv8 = 8 THEN 'MTN'
		WHEN cendiv8 = 9 THEN 'PAC'
	END;


-- add index
CREATE INDEX eia_microdata_cbecs_2003_census_region_btree 
ON diffusion_shared.eia_microdata_cbecs_2003
USING btree(census_region);

CREATE INDEX eia_microdata_cbecs_2003_census_division_abbr_btree 
ON diffusion_shared.eia_microdata_cbecs_2003
USING btree(census_division_abbr);

-----------------------------------------------------------------
-- Residential Energy Consumption Survey
DROP TABLE IF EXISTS diffusion_shared.eia_microdata_recs_2009;
CREATE TABLE diffusion_shared.eia_microdata_recs_2009 
(
	doeid			integer primary key,	--Unique identifier for each respondent
	regionc			integer,	--Census Region
	division		integer,	--Census Division
	reportable_domain	integer,	--Reportable states and groups of states
	typehuq			integer,	--Type of housing unit
	nweight			numeric,	--Final sample weight
	kwh			numeric 	--Total Site Electricity usage, in kilowatt-hours, 2009	
);

COMMENT ON TABLE diffusion_shared.eia_microdata_recs_2009 iS 'Note: This is just a subset of the available columns. Data Dictionary available at: http://www.eia.gov/consumption/residential/data/2009/csv/public_layout.csv';
COMMENT ON COLUMN diffusion_shared.eia_microdata_recs_2009.doeid IS 'Unique identifier for each respondent';
COMMENT ON COLUMN diffusion_shared.eia_microdata_recs_2009.regionc IS 'Census Region';
COMMENT ON COLUMN diffusion_shared.eia_microdata_recs_2009.division IS 'Census Division';
COMMENT ON COLUMN diffusion_shared.eia_microdata_recs_2009.reportable_domain IS 'Reportable states and groups of states';
COMMENT ON COLUMN diffusion_shared.eia_microdata_recs_2009.typehuq IS 'Type of housing unit';
COMMENT ON COLUMN diffusion_shared.eia_microdata_recs_2009.nweight IS 'Final sample weight';
COMMENT ON COLUMN diffusion_shared.eia_microdata_recs_2009.kwh IS 'Total Site Electricity usage, in kilowatt-hours, 2009';


SET ROLE 'server-superusers';
COPY diffusion_shared.eia_microdata_recs_2009	 FROM '/srv/home/mgleason/data/dg_wind/recs2009_selected_columns.csv' with csv header;
SET ROLE 'diffusion_shared-writers';

ALTER TABLE diffusion_shared.eia_microdata_recs_2009
ADD COLUMN census_region text;

UPDATE diffusion_shared.eia_microdata_recs_2009
SET census_region = 
	CASE WHEN regionc = 1 THEN 'Northeast'
	     WHEN regionc = 2 THEN 'Midwest'
	     WHEN regionc = 3 then 'South'
	     WHEN regionc = 4 then 'West'
	END;


ALTER TABLE diffusion_shared.eia_microdata_recs_2009
ADD COLUMN census_division_abbr text;

UPDATE diffusion_shared.eia_microdata_recs_2009
SET census_division_abbr = 
	CASE WHEN division = 1 THEN 'NE'
		WHEN division = 2 THEN 'MA'
		WHEN division = 3 THEN 'ENC'
		WHEN division = 4 THEN 'WNC'
		WHEN division = 5 THEN 'SA'
		WHEN division = 6 THEN 'ESC'
		WHEN division = 7 THEN 'WSC'
		WHEN division = 8 THEN 'MTN' -- NOTE: RECS breaks the MTN into N and S subdivision, but for consistency w CbECS, we will stick with one
		WHEN division = 9 THEN 'MTN' -- (see above)
		WHEN division = 10 THEN 'PAC'
	END;


-- add index
CREATE INDEX eia_microdata_recs_2009_census_region_btree 
ON diffusion_shared.eia_microdata_recs_2009
USING btree(census_region);

CREATE INDEX eia_microdata_recs_2009_census_division_abbr_btree 
ON diffusion_shared.eia_microdata_recs_2009
USING btree(census_division_abbr);

SELECT distinct(census_region)
fROM diffusion_shared.eia_microdata_recs_2009;

SELECT distinct(census_division_abbr)
fROM diffusion_shared.eia_microdata_recs_2009;

-- ingest lookup table to translate recs reportable domain to states
set role 'diffusion-writers';
DrOP TABLE IF EXISTS diffusion_shared.eia_reportable_domain_to_state_recs_2009;
CREATE TABLE diffusion_shared.eia_reportable_domain_to_state_recs_2009
(
	reportable_domain integer,
	state_name text primary key
);

SET ROLE 'server-superusers';
COPY  diffusion_shared.eia_reportable_domain_to_state_recs_2009
FROM '/srv/home/mgleason/data/dg_wind/recs_reportable_dominain_to_state.csv' with csv header;
set role 'diffusion-writers';

-- create index for reportable domai column in this table and the recs table
CREATE INDEX eia_reportable_domain_to_state_recs_2009_reportable_domain_btree 
ON diffusion_shared.eia_reportable_domain_to_state_recs_2009
USING btree(reportable_domain);

CREATE INDEX eia_microdata_recs_2009_reportable_domain_btree 
ON diffusion_shared.eia_microdata_recs_2009
USING btree(reportable_domain);
