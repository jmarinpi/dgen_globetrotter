SET ROLE 'dg_wind-writers';
DROP TABLE IF EXISTS dg_wind.acs_2011_tenure_by_housing_units_in_structure;
CREATE TABLE dg_wind.acs_2011_tenure_by_housing_units_in_structure
(
	gisjoin text,
	year character varying(9),
	state text,
	statea integer,
	county text,
	countya integer,
	full_county_name text,
	tot_occu_h integer,
	own_occu_h integer,
	own_occu_1str_detached_h integer,
	own_occu_1str_attached_h integer,
	renter_occu_h integer,
	renter_occu_1str_detached integer,
	renter_occu_1str_attached integer
);

SET ROLE 'server-superusers';
SET client_encoding to "LATIN1";
COPY dg_wind.acs_2011_tenure_by_housing_units_in_structure FROM '/srv/home/mgleason/data/dg_wind/nhgis0006_ds184_20115_2011_county_simplified.csv'
WITH CSV HEADER;
SET client_encoding TO 'UNICODE';
SET ROLE 'dg_wind-writers';

-- have to join on fips codes, which requires some prejoins
DROP TABLE IF EXISTS dg_wind.county_housing_units;
CREATE TABLE dg_wind.county_housing_units AS
WITH a as (
	SELECT a.*, b.name as county_name, b.statefp, b.countyfp
	FROM dg_wind.acs_2011_tenure_by_housing_units_in_structure a
	INNER JOIN dg_wind.acs_2011_us_county b
	ON a.gisjoin = b.gisjoin
),
b AS (
SELECT a.county_id, a.state, a.county, b.state_fips, b.cnty_fips
FROM wind_shared.county_geom a
inner JOIN esri.dtl_cnty_all_multi b
ON a.county_id = b.gid)
SELECT b.county_id, b.state, b.county, b.state_fips, b.cnty_fips, 
	 a.tot_occu_h, a.own_occu_h, a.own_occu_1str_detached_h, a.own_occu_1str_attached_h, 
	-- calculate total 1 structure own occu
	 a.own_occu_1str_detached_h + a.own_occu_1str_attached_h as own_occu_1str_all, 
       a.renter_occu_h, a.renter_occu_1str_detached, a.renter_occu_1str_attached
FROM b
LEFT JOIN a
ON b.state_fips = a.statefp
and b.cnty_fips = a.countyfp;

-- look for unjoined data
SELECT *
FROM dg_wind.county_housing_units
where tot_occu_h is null;
-- 3 counties in alaska
-- 3;'Prince of Wales-Outer Ketchikan'
-- 9;'Skagway-Hoonah-Angoon'
-- 4;'Wrangell-Petersburg'

-- look for these in the acs table
SELECT *
FROM dg_wind.acs_2011_tenure_by_housing_units_in_structure
where state = 'Alaska'
ORDER BY county;
-- these counties exist -- they are just separate in ACS


-- sum them to fix
WITH b AS (
SELECT sum(tot_occu_h) as tot_occu_h, 
       sum(own_occu_h) as own_occu_h, 
       sum(own_occu_1str_detached_h) as own_occu_1str_detached_h, 
       sum(own_occu_1str_attached_h) AS own_occu_1str_attached_h, 
       sum(own_occu_1str_detached_h+own_occu_1str_attached_h) AS own_occu_1str_all,
       sum(renter_occu_h) as renter_occu_h, 
       sum(renter_occu_1str_detached) as renter_occu_1str_detached, 
       sum(renter_occu_1str_attached) as renter_occu_1str_attached
FROM dg_wind.acs_2011_tenure_by_housing_units_in_structure
WHERE full_county_name in ('Skagway Municipality, Alaska','Hoonah-Angoon Census Area, Alaska'))
UPDATE dg_wind.county_housing_units a
SET (tot_occu_h, 
       own_occu_h, own_occu_1str_detached_h, own_occu_1str_attached_h, 
       own_occu_1str_all, renter_occu_h, renter_occu_1str_detached, 
       renter_occu_1str_attached) =
	(b.tot_occu_h, 
       b.own_occu_h, b.own_occu_1str_detached_h, b.own_occu_1str_attached_h, 
       b.own_occu_1str_all, b.renter_occu_h, b.renter_occu_1str_detached, 
       b.renter_occu_1str_attached)
FROM b
where a.county_id = 9;


WITH b AS (
SELECT sum(tot_occu_h) as tot_occu_h, 
       sum(own_occu_h) as own_occu_h, 
       sum(own_occu_1str_detached_h) as own_occu_1str_detached_h, 
       sum(own_occu_1str_attached_h) AS own_occu_1str_attached_h, 
       sum(own_occu_1str_detached_h+own_occu_1str_attached_h) AS own_occu_1str_all,
       sum(renter_occu_h) as renter_occu_h, 
       sum(renter_occu_1str_detached) as renter_occu_1str_detached, 
       sum(renter_occu_1str_attached) as renter_occu_1str_attached
FROM dg_wind.acs_2011_tenure_by_housing_units_in_structure
WHERE full_county_name in ('Wrangell City and Borough, Alaska','Petersburg Census Area, Alaska'))
UPDATE dg_wind.county_housing_units a
SET (tot_occu_h, 
       own_occu_h, own_occu_1str_detached_h, own_occu_1str_attached_h, 
       own_occu_1str_all, renter_occu_h, renter_occu_1str_detached, 
       renter_occu_1str_attached) =
	(b.tot_occu_h, 
       b.own_occu_h, b.own_occu_1str_detached_h, b.own_occu_1str_attached_h, 
       b.own_occu_1str_all, b.renter_occu_h, b.renter_occu_1str_detached, 
       b.renter_occu_1str_attached)
FROM b
where a.county_id = 4;

-- this one may not be quite right, but it's likely correct
WITH b AS (
SELECT sum(tot_occu_h) as tot_occu_h, 
       sum(own_occu_h) as own_occu_h, 
       sum(own_occu_1str_detached_h) as own_occu_1str_detached_h, 
       sum(own_occu_1str_attached_h) AS own_occu_1str_attached_h, 
       sum(own_occu_1str_detached_h+own_occu_1str_attached_h) AS own_occu_1str_all,
       sum(renter_occu_h) as renter_occu_h, 
       sum(renter_occu_1str_detached) as renter_occu_1str_detached, 
       sum(renter_occu_1str_attached) as renter_occu_1str_attached
FROM dg_wind.acs_2011_tenure_by_housing_units_in_structure
WHERE full_county_name in ('Prince of Wales-Hyder Census Area, Alaska'))
UPDATE dg_wind.county_housing_units a
SET (tot_occu_h, 
       own_occu_h, own_occu_1str_detached_h, own_occu_1str_attached_h, 
       own_occu_1str_all, renter_occu_h, renter_occu_1str_detached, 
       renter_occu_1str_attached) =
	(b.tot_occu_h, 
       b.own_occu_h, b.own_occu_1str_detached_h, b.own_occu_1str_attached_h, 
       b.own_occu_1str_all, b.renter_occu_h, b.renter_occu_1str_detached, 
       b.renter_occu_1str_attached)
FROM b
where a.county_id = 3;


-- confirm nothing is missing now
-- look for unjoined data
SELECT *
FROM dg_wind.county_housing_units
where tot_occu_h is null;


ALTER TABLE dg_wind.county_housing_units
ADD COLUMN perc_own_occu_1str_housing numeric;

UPDATE dg_wind.county_housing_units
SET perc_own_occu_1str_housing = own_occu_1str_all::NUMERIC/tot_occu_h::NUMERIC;

-- check results against previously calculate owner occupied %
SELECT a.county_id, a.state, a.county, a.perc_own_occu_1str_housing, b.perc_own_occupied_housing
FROM dg_wind.county_housing_units a
LEFT JOIN dg_wind.county_own_occ_housing b
ON a.county_id = b.county_id
-- where a.state = 'New York'
-- where a.own_occu_h <> b.own_occ_housing_units
ORDER bY b.perc_own_occupied_housing-a.perc_own_occu_1str_housing;



-- copy data to diffusion_shared schema
SET ROLE 'wind_ds-writers';
DROP TABLE IF EXISTS diffusion_shared.county_housing_units;
CREATE TABLE diffusion_shared.county_housing_units AS
SELECT county_id, perc_own_occu_1str_housing 
FROM  dg_wind.county_housing_units;

ALTER TABLE diffusion_shared.county_housing_units
ADD primary key (county_id);

ALTER TABLE diffusion_shared.county_housing_units
ADD CONSTRAINT county_id_fkey FOREIGN KEY (county_id)
      REFERENCES diffusion_shared.county_geom (county_id) MATCH FULL
      ON UPDATE RESTRICT ON DELETE RESTRICT;
