--------------------------------------------------------------------------------
-- LOAD OPENPV DATA (FROM 7/11/2014 export)
SET ROLE 'diffusion-writers';
DROP tABLE IF EXISTS diffusion_solar_data.existing_installs_open_pv_2014_07_11;
CREATE TABLE diffusion_solar_data.existing_installs_open_pv_2014_07_11
(
	cost numeric,
	size_kw		numeric,
	date_installed	numeric, -- stored as epoch -- needs to be cast to date
	zipcode		character varying(5),
	electricity_purchaser	text,
	county		text,
	technology text,
	developer	text,
	city		text,
	land_type	text,
	state_abbr	text,
	net_metered	text, -- needs to be cast to boolean
	project_name	text,
	install_type	text,	
	cost_per_watt	text,  -- needs to be cast to numeric (replace "" with null)
	is_zip_level	boolean,	
	lat		numeric,
	lon		numeric,
	address1		text,
	street_address		text,
	third_party_owned	text, -- needs to be cast to boolean
	incentive_amount	text, --stored as text w dollar sign and commas -- needs to be cast to numeric
	installer	text,
	utility		text,
	utility_name	text
);

SET ROLE 'server-superusers';
COPY diffusion_solar_data.existing_installs_open_pv_2014_07_11
FROM '/srv/home/mgleason/data/dg_solar/gleason_export_7_11_14_all_fields.csv' WITH CSV HEADER;
SET ROLE 'diffusion-writers';

------------------------------------------
-- CLEAN UP COLUMN TYPES

-- date_installed -- stored as epoch -- needs to be cast to date
ALTER TABLE diffusion_solar_data.existing_installs_open_pv_2014_07_11
ADD COLUMN date_installed2 timestamp without time zone;

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET date_installed2 = timestamp 'epoch' + date_installed * interval '1 second';

ALTER TABLE diffusion_solar_data.existing_installs_open_pv_2014_07_11
Drop COLUMN date_installed;

ALTER TABLE diffusion_solar_data.existing_installs_open_pv_2014_07_11
RENAME date_installed2 to date_installed;

-- net_metered	-- needs to be cast to boolean
SELECT distinct(net_metered)
FROM diffusion_solar_data.existing_installs_open_pv_2014_07_11
order by net_metered;

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET net_metered = 'True'
where net_metered = 'X';

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET net_metered = Null
where net_metered = '';

ALTER TABLE diffusion_solar_data.existing_installs_open_pv_2014_07_11
ALTER net_metered TYPE boolean using net_metered::boolean;

-- cost_per_watt -- needs to be cast to numeric (replce '' with null)
UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET cost_per_watt = Null
where cost_per_watt = '';

ALTER TABLE diffusion_solar_data.existing_installs_open_pv_2014_07_11
ALTER cost_per_watt TYPE numeric using cost_per_watt::numeric;

-- incentive_amount	text, --stored as text w dollar sign and commas -- needs to be cast to numeric
SELECT distinct(incentive_amount)
FROM diffusion_solar_data.existing_installs_open_pv_2014_07_11
order by incentive_amount;

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET incentive_amount = replace(incentive_amount, ',', '');

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET incentive_amount = replace(incentive_amount, '$', '');

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET incentive_amount = NULL
where incentive_amount in ('No Data','-9999', '3.25/watt', 'none','NA','n/a','n/a''','F',
	'db.installs.find({"_id":{$in:[ObjectId("4f7a240d45538529b1007eeb"),ObjectId("4f5fa86b45538529b1000509")]}})',
	 '9999',
	 'db.installs.find({"_id":{in:[ObjectId("4f7a240d45538529b1007eeb")ObjectId("4f5fa86b45538529b1000509")]}})',
	 '', '30% fed', '52 412','15000 possibley more','30% Federal Tax Credit','4920.00 
	4920.00 ','80%');

ALTER TABLE diffusion_solar_data.existing_installs_open_pv_2014_07_11
ALTER incentive_amount TYPE numeric using incentive_amount::numeric;

-- to do: 3rd party owned, install type, others?
-- third_party_owned	text, -- needs to be cast to boolean
SELECT distinct(third_party_owned)
FROM diffusion_solar_data.existing_installs_open_pv_2014_07_11;

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET third_party_owned = 'True'
where third_party_owned in ('yes','Yes');

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET third_party_owned = 'False'
where third_party_owned in ('No','no','N');

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET third_party_owned = NULL
where third_party_owned in ('No Data','Barnstable','');

ALTER TABLE  diffusion_solar_data.existing_installs_open_pv_2014_07_11
ALTER third_party_owned TYPE boolean using third_party_owned::boolean;

-- ADD GEOMETRY
ALTER TABLE diffusion_solar_data.existing_installs_open_pv_2014_07_11
ADD COLUMN the_geom_4326 geometry;

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET the_geom_4326 = ST_SetSRID(ST_MakePoint(lon,lat),4326);

CREATE INDEX existing_installs_open_pv_the_geom_4326_gist 
ON diffusion_solar_data.existing_installs_open_pv_2014_07_11 USING gist(the_geom_4326);

-- add second geom (96703)
ALTER TABLE diffusion_solar_data.existing_installs_open_pv_2014_07_11
ADD COLUMN the_geom_96703 geometry;

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET the_geom_96703 = ST_Transform(the_geom_4326,96703);

CREATE INDEX existing_installs_open_pv_the_geom_96703_gist 
ON diffusion_solar_data.existing_installs_open_pv_2014_07_11 USING gist(the_geom_96703);


-- FIX install_type column
ALTER TABLE diffusion_solar_data.existing_installs_open_pv_2014_07_11
RENAME install_type TO sector;

ALTER TABLE diffusion_solar_data.existing_installs_open_pv_2014_07_11
add column industry text;

SELECT distinct(sector)
FROM diffusion_solar_data.existing_installs_open_pv_2014_07_11
order by sector;

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET sector = Null
where sector in ('Unknown','unknown','Not Stated','Customer');

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET (sector,industry) = ('commercial','agriculture')
where sector in ('agricultural','Agricultural','agriculture','Commercial - Agriculture');

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET (sector,industry) = ('public','government')
where sector in ('government','Government','Gov''t/NP','Municipal','Institutional');

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET (sector,industry) = ('commercial','nonprofit')
where sector in ('nonprofit','Nonprofit');

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET sector = 'residential'
where sector in ('residential','Residential','Residential/SF');

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET (sector,industry) = ('public','education')
where sector in ('education','educational','Educational');

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET (sector,industry) = ('public',NULL)
where sector in ('public','Public');

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET sector = 'commercial'
where sector in ('commercial','Small Business','Commerical','Commercial','Commercial - Builders',
	'Commercial - Other','Commercial - Small Business','Nonresidential');

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET sector = 'utility'
where sector in ('utility','Utility');

-- deal with sector = NULL (assign to res if possible)
SELECT *
FROM diffusion_solar_data.existing_installs_open_pv_2014_07_11
where sector is null;
-- 20k out of 244k (about 8%)

-- Plot system size distributions by sector to investigate whether 10kw is an appropriate threshold for residential systems
-- see R script: /Volumes/Staff/mgleason/SEEDS/R/system_size_distributions_by_state_and_sector.R
-- results generally support using 10kw to threshold residential from the nulls
UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET sector = 'residential'
where sector is null
and size_kw <= 10;

UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET sector = 'commercial'
where sector is null
and size_kw > 10;

DELETE FROM diffusion_solar_data.existing_installs_open_pv_2014_07_11
where sector is null and size_kw is null;

-- fix zipcodes
UPDATE diffusion_solar_data.existing_installs_open_pv_2014_07_11
SET  zipcode = lpad(zipcode,5,'0');

-- fix states
SELECT distinct(state_abbr)
FROM diffusion_solar_data.existing_installs_open_pv_2014_07_11
order by state_abbr;

DELETE FROM diffusion_solar_data.existing_installs_open_pv_2014_07_11
where state_abbr = 'Κεντρική Μακεδονία'
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- load state by state data
-- use seia.cumulative_pv_capacity_by_state_2014_q4 (loaded for the SEEDS analysis)

--------------------------------------------------------------------------------
-- combine/meld the two sources
with a AS 
(
	SELECT state_abbr, sum(size_kw) as open_pv_capacity
	FROM diffusion_solar_data.existing_installs_open_pv_2014_07_11
	where sector <> 'utility'
	GROUP BY state_abbr
)

SELECT a.state_abbr, a.open_pv_capacity/1000 as open_pv_mw, b.res_cap_mw+b.nonres_cap_mw as seia_mw
FROM a
LEFT JOIN seia.cumulative_pv_capacity_by_state_2014_q4 b
ON a.state_abbr = b.state_abbr
order by a.state_abbr;

