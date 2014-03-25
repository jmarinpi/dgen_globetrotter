-- load iiijjjicf lookup table

-- load temporary table with all columsn
DROP TABLE IF EXISTS dg_wind.iiijjjicf_temp;
CREATE TABLE dg_wind.iiijjjicf_temp (
	row_id integer,
	value integer,
	count integer,
	ijid integer,
	icf80 character varying(3),
	iiijjj character varying(6),
	iiijjjicf character varying(9));
SET ROLE "server-superusers";
COPY dg_wind.iiijjjicf_temp FROM '/srv/home/mgleason/data/dg_wind/gridvalue_to_iiijjjicf_lkup.csv' with csv header;
RESET ROLE;	

-- copy the data over to the new table
DROP TABLE IF EXISTS wind_ds.iiijjjicf_lookup;
CREATE TABLE wind_ds.iiijjjicf_lookup AS
sELECT value as id, iiijjjicf,
	substring(iiijjjicf from 1 for 3) as iii,
	substring(iiijjjicf from 4 for 3)  as jjj,
	substring(iiijjjicf from 7 for 3)  as icf
FROM dg_wind.iiijjjicf_temp;

ALTER TABLE wind_ds.iiijjjicf_lookup ADD PRIMARY KEY (id);

VACUUM ANALYZE  wind_ds.iiijjjicf_lookup 

-- drop the temp table
DROP TABLE dg_wind.iiijjjicf_temp;