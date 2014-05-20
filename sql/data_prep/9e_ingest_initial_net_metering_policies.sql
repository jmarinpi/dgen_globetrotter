﻿DROP TABLE IF EXISTS wind_ds.net_metering_availability_2013;
CREATE TABLE wind_ds.net_metering_availability_2013 (
	state text,
	nem_grade character varying(2),
	sector character varying(3),
	utility_type character varying(9),
	nem_system_limit_kw float
);

SET ROLE 'server-superusers';
COPY wind_ds.net_metering_availability_2013 FROM '/home/mgleason/data/dg_wind/NetMeterAvail.csv' with csv header;
RESET ROLE;

ALTER TABLE wind_ds.net_metering_availability_2013 
DROP COLUMN nem_grade,
ADD COLUMN state_abbr character varying(2);

UPDATE wind_ds.net_metering_availability_2013 a
SET state_abbr = b.state_abbr
FROM esri.dtl_state_20110101 b
WHERE a.state = b.state_name;

-- add in values for utility_type = 'All Other'
INSERT INTO wind_ds.net_metering_availability_2013 (state_abbr, sector, utility_type, nem_system_limit_kw) 
SELECT state_abbr, sector, 'All Other'::text as utility_type, 0:: float as nem_system_limit_kw
FROM wind_ds.net_metering_availability_2013
GROUP BY state_abbr, sector;

CREATE INDEX net_metering_availability_2013_state_abbr_btree ON wind_ds.net_metering_availability_2013
USING btree(state_abbr);

CREATE INDEX net_metering_availability_2013_utility_type_btree ON wind_ds.net_metering_availability_2013
USING btree(utility_type);

CREATE INDEX net_metering_availability_2013_sector_btree ON wind_ds.net_metering_availability_2013
USING btree(sector);

ALTER TABLE wind_ds.net_metering_availability_2013 ADD PRIMARY KEY (state_abbr, sector, utility_type);

ALTER TABLE wind_ds.net_metering_availability_2013 
DROP COLUMN state;

-----------------------------------------------------------------
--- create manual nem table too
DROP TABLE IF EXIStS wind_ds.manual_net_metering_availability;
CREATE TABLE wind_ds.manual_net_metering_availability
(
  sector character varying(3) NOT NULL,
  utility_type character varying(9) NOT NULL,
  nem_system_limit_kw double precision,
  state_abbr character varying(2) NOT NULL,
  CONSTRAINT manual_net_metering_availability_pkey PRIMARY KEY (state_abbr, sector, utility_type)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wind_ds.manual_net_metering_availability
  OWNER TO "wind_ds-writers";
GRANT ALL ON TABLE wind_ds.manual_net_metering_availability TO mgleason;
GRANT ALL ON TABLE wind_ds.manual_net_metering_availability TO "wind_ds-writers";

-- Index: wind_ds.net_metering_availability_2013_sector_btree

-- DROP INDEX wind_ds.net_metering_availability_2013_sector_btree;

CREATE INDEX manual_net_metering_availability_sector_btree
  ON wind_ds.manual_net_metering_availability
  USING btree
  (sector COLLATE pg_catalog."default");

-- Index: wind_ds.manual_net_metering_availability_state_abbr_btree

-- DROP INDEX wind_ds.manual_net_metering_availability_state_abbr_btree;

CREATE INDEX manual_net_metering_availability_state_abbr_btree
  ON wind_ds.manual_net_metering_availability
  USING btree
  (state_abbr COLLATE pg_catalog."default");

-- Index: wind_ds.manual_net_metering_availability_utility_type_btree

-- DROP INDEX wind_ds.manual_net_metering_availability_utility_type_btree;

CREATE INDEX manual_net_metering_availability_utility_type_btree
  ON wind_ds.manual_net_metering_availability
  USING btree
  (utility_type COLLATE pg_catalog."default");