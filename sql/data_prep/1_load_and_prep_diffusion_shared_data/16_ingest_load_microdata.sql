-- load commercial building microdata
SET ROLE 'diffusion-writers';
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
SET ROLE 'diffusion-writers';

-- add in the ownocc8 data (detailed building activity info)
DROP TABLE IF EXISTS diffusion_shared.eia_microdata_cbecs_ownocc8;
CREATE TABLE  diffusion_shared.eia_microdata_cbecs_ownocc8 (
	pubid8	integer primary key,	--building identifier
	ownocc8 text
);
SET ROLE 'server-superusers';
COPY diffusion_shared.eia_microdata_cbecs_ownocc8 
FROM '/srv/home/mgleason/data/dg_wind/cbecs_ownocc8.csv' with csv header;
SET ROLE 'diffusion-writers';

-- replace ' ' with NULL and cast to integer
UPDATE diffusion_shared.eia_microdata_cbecs_ownocc8
set ownocc8 = null
where ownocc8 = ' ';

alter table diffusion_shared.eia_microdata_cbecs_ownocc8
alter column ownocc8 type integer using ownocc8::integer;

-- make sure there is data for every building in the main cbecs table
SELECT *
FROM diffusion_shared.eia_microdata_cbecs_2003 a
lEFT JOIN diffusion_shared.eia_microdata_cbecs_ownocc8  b
ON a.pubid8 = b.pubid8
where b.pubid8 is null;
-- 0 rows, all is good

-- add this information into the main cbecs table
ALTER TABLE diffusion_shared.eia_microdata_cbecs_2003
add column ownocc8 integer;

UPDATE diffusion_shared.eia_microdata_cbecs_2003 a
SET ownocc8 = b.ownocc8
from diffusion_shared.eia_microdata_cbecs_ownocc8  b
where a.pubid8 = b.pubid8;

-- check that all values were transferred
select count(*)
FROM diffusion_shared.eia_microdata_cbecs_2003 
where ownocc8 is null;

select count(*)
FROM diffusion_shared.eia_microdata_cbecs_ownocc8 
where ownocc8 is null;
-- 395 null in both -- all set

-- drop the pbaplus 8 table -- no longer needed
DROP TABLE diffusion_shared.eia_microdata_cbecs_ownocc8;

-- add a comment to describe this column
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.ownocc8 
IS 'Owner occupies space';

-- add indices on pba8 and pbaplus8
CREATE INDEX eia_microdata_cbecs_2003_ownocc8_btree
ON diffusion_shared.eia_microdata_cbecs_2003
using btree(ownocc8);

select distinct(ownocc8)
FROM diffusion_shared.eia_microdata_cbecs_2003

-- add in the pbaplus 8 data (detailed building activity info)
DROP TABLE IF EXISTS diffusion_shared.eia_microdata_cbecs_pbaplus8;
CREATE TABLE  diffusion_shared.eia_microdata_cbecs_pbaplus8 (
	pubid8	integer primary key,	--building identifier
	pbaplus8 integer
);
SET ROLE 'server-superusers';
COPY diffusion_shared.eia_microdata_cbecs_pbaplus8 
FROM '/srv/home/mgleason/data/dg_wind/cbecs_pba_plus8.csv' with csv header;
SET ROLE 'diffusion-writers';

-- make sure there is data for every building in the main cbecs table
SELECT *
FROM diffusion_shared.eia_microdata_cbecs_2003 a
lEFT JOIN diffusion_shared.eia_microdata_cbecs_pbaplus8  b
ON a.pubid8 = b.pubid8
where b.pubid8 is null;
-- 0 rows, all is good

-- add this information into the main cbecs table
ALTER TABLE diffusion_shared.eia_microdata_cbecs_2003
add column pbaplus8 integer;

UPDATE diffusion_shared.eia_microdata_cbecs_2003 a
SET pbaplus8 = b.pbaplus8
from diffusion_shared.eia_microdata_cbecs_pbaplus8  b
where a.pubid8 = b.pubid8;

-- check that all values were transferred
select *
FROM diffusion_shared.eia_microdata_cbecs_2003 
where pbaplus8 is null;
-- all set

-- drop the pbaplus 8 table -- no longer needed
DROP TABLE diffusion_shared.eia_microdata_cbecs_pbaplus8;

-- add a comment to describe this column
COMMENT ON COLUMN diffusion_shared.eia_microdata_cbecs_2003.pbaplus8 
IS 'More specific building activity';

-- add indices on pba8 and pbaplus8
CREATE INDEX eia_microdata_cbecs_2003_pba8_btree
ON diffusion_shared.eia_microdata_cbecs_2003
using btree(pba8);

CREATE INDEX eia_microdata_cbecs_2003_pbaplus8_btree
ON diffusion_shared.eia_microdata_cbecs_2003
using btree(pbaplus8);

-- add lookup table for pba8
DROP TABLE IF EXISTS diffusion_shared.eia_microdata_cbecs_2003_pba_lookup;
CREATE TABLE  diffusion_shared.eia_microdata_cbecs_2003_pba_lookup (
	pba8	integer primary key,
	description text
);
SET ROLE 'server-superusers';
COPY diffusion_shared.eia_microdata_cbecs_2003_pba_lookup 
FROM '/srv/home/mgleason/data/dg_wind/pba8_lookup.csv' with csv header QUOTE '''';
SET ROLE 'diffusion-writers';

-- add lookup table for pbaplus8
DROP TABLE IF EXISTS diffusion_shared.eia_microdata_cbecs_2003_pbaplus8_lookup;
CREATE TABLE  diffusion_shared.eia_microdata_cbecs_2003_pbaplus8_lookup (
	pbaplus8	integer primary key,
	description text
);
SET ROLE 'server-superusers';
COPY diffusion_shared.eia_microdata_cbecs_2003_pbaplus8_lookup 
FROM '/srv/home/mgleason/data/dg_wind/pbaplus8_lookup.csv' with csv header QUOTE '''';
SET ROLE 'diffusion-writers';

-- extract all of the disctinct pba/pbaplus8 building uses
SET ROLE 'server-superusers';
COPY 
(
	with a as
	(
		SELECT pba8, pbaplus8
		from diffusion_shared.eia_microdata_cbecs_2003
		group by pba8, pbaplus8
	)
	SELECT a.pba8, b.description as pba8_desc,
	       a.pbaplus8, c.description as pbaplus8_desc
	FROM a
	left join diffusion_shared.eia_microdata_cbecs_2003_pba_lookup b
	ON a.pba8 = b.pba8
	LEFT JOIN diffusion_shared.eia_microdata_cbecs_2003_pbaplus8_lookup c
	on a.pbaplus8 = c.pbaplus8
	order by a.pba8, a.pbaplus8
) TO '/srv/home/mgleason/data/dg_wind/cbecs_to_eplus_commercial_building_types.csv' with csv header;
SET ROLE 'diffusion-writers';

-- manually edit this table to identify the DOE Commercial Building Type (there 16)
-- associated with each pba8/pbaplus8 combination
-- use http://www.nrel.gov/docs/fy11osti/46861.pdf as a starting point
-- then reload the resulting lookup table to diffusion_shared.cbecs_pba8_pbaplus8_to_eplus_bldg_types

-- add descriptions for census region
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

-- load cbecs pba/pbaplus to Energy Plus Commercial Reference Buildings
-- lookup table
SET role 'diffusion-writers';
DROP TABLE IF EXISTS diffusion_shared.cbecs_2003_pba_to_eplus_crbs;
CREATE TABLE diffusion_shared.cbecs_2003_pba_to_eplus_crbs
(
	pba8 integer,
	pba8_desc text,
	pbaplus8 integer,
	pbaplus8_desc text,
	sqft_min numeric,
	sqft_max numeric,
	crb_model text,
	defined_by text,
	notes text
);

SET ROLE 'server-superusers';
COPY diffusion_shared.cbecs_2003_pba_to_eplus_crbs 
FROM '/srv/home/mgleason/data/dg_wind/cbecs_to_eplus_commercial_building_types.csv' 
with csv header;
SET ROLE 'diffusion-writers';

-- create indices on pba8 and pbaplus 8
CREATE INDEX cbecs_2003_pba_to_eplus_crbs_pba8_btree
ON diffusion_shared.cbecs_2003_pba_to_eplus_crbs 
using btree(pba8);

CREATE INDEX cbecs_2003_pba_to_eplus_crbs_pbaplus8_btree
ON diffusion_shared.cbecs_2003_pba_to_eplus_crbs 
using btree(pbaplus8);

-- create a simple lookup table for all non-vacant
-- cbecs buildings that gives the commercial reference building model
DROP TABLE IF EXISTS diffusion_shared.cbecs_2003_crb_lookup;
CREATE TABLE diffusion_shared.cbecs_2003_crb_lookup AS
with a AS
(
	SELECT a.pubid8, a.sqft8, b.*
	FROM diffusion_shared.eia_microdata_cbecs_2003 a
	LEFT JOIN diffusion_shared.cbecs_2003_pba_to_eplus_crbs b
	ON a.pba8 = b.pba8
	and a.pbaplus8 = b.pbaplus8
	where a.pba8 <> 1 -- ignore vacant buildings
)
select pubid8, crb_model
FROM a
where (sqft_min is null and sqft_max is null)
or (sqft8 >= sqft_min and sqft8 < sqft_max);
-- 5019 rows

-- does that match the count of nonvacant buldings?
select count(*)
FROM diffusion_shared.eia_microdata_cbecs_2003
where pba8 <> 1;
-- yes-- 5019

-- do all buildings have a crb?
SELECT count(*)
FROM diffusion_shared.cbecs_2003_crb_lookup
where crb_model is null;
-- yes

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
	kownrent		integer,        --Housing unit is owned, rented, or occupied without payment of rent
	kwh			numeric 	--Total Site Electricity usage, in kilowatt-hours, 2009	
);

COMMENT ON TABLE diffusion_shared.eia_microdata_recs_2009 iS 'Note: This is just a subset of the available columns. Data Dictionary available at: http://www.eia.gov/consumption/residential/data/2009/csv/public_layout.csv';
COMMENT ON COLUMN diffusion_shared.eia_microdata_recs_2009.doeid IS 'Unique identifier for each respondent';
COMMENT ON COLUMN diffusion_shared.eia_microdata_recs_2009.regionc IS 'Census Region';
COMMENT ON COLUMN diffusion_shared.eia_microdata_recs_2009.division IS 'Census Division';
COMMENT ON COLUMN diffusion_shared.eia_microdata_recs_2009.reportable_domain IS 'Reportable states and groups of states';
COMMENT ON COLUMN diffusion_shared.eia_microdata_recs_2009.typehuq IS 'Type of housing unit';
COMMENT ON COLUMN diffusion_shared.eia_microdata_recs_2009.nweight IS 'Final sample weight';
COMMENT ON COLUMN diffusion_shared.eia_microdata_recs_2009.kownrent IS 'Housing unit is owned, rented, or occupied without payment of rent';
COMMENT ON COLUMN diffusion_shared.eia_microdata_recs_2009.kwh IS 'Total Site Electricity usage, in kilowatt-hours, 2009';


SET ROLE 'server-superusers';
COPY diffusion_shared.eia_microdata_recs_2009	 FROM '/srv/home/mgleason/data/dg_wind/recs2009_selected_columns.csv' with csv header;
SET ROLE 'diffusion-writers';


-- add indices on kownrent and typehuq
CREATE INDEX eia_microdata_recs_2009_typehuq_btree
ON diffusion_shared.eia_microdata_recs_2009
using btree(typehuq)
where typehuq in (1,2);

CREATE INDEX eia_microdata_recs_2009_kownrent_btree
ON diffusion_shared.eia_microdata_recs_2009
using btree(kownrent)
where kownrent = 1;

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
