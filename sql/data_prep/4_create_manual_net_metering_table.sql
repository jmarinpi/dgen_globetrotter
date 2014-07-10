--- create manual nem table too
DROP TABLE IF EXIStS diffusion_wind.manual_net_metering_availability;
CREATE TABLE diffusion_wind.manual_net_metering_availability
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
ALTER TABLE diffusion_wind.manual_net_metering_availability
  OWNER TO "wind_ds-writers";
GRANT ALL ON TABLE diffusion_wind.manual_net_metering_availability TO mgleason;
GRANT ALL ON TABLE diffusion_wind.manual_net_metering_availability TO "wind_ds-writers";

-- Index: diffusion_shared.net_metering_availability_2013_sector_btree

-- DROP INDEX diffusion_shared.net_metering_availability_2013_sector_btree;

CREATE INDEX manual_net_metering_availability_sector_btree
  ON  diffusion_wind.manual_net_metering_availability
  USING btree
  (sector COLLATE pg_catalog."default");

-- Index:  diffusion_wind.manual_net_metering_availability_state_abbr_btree

-- DROP INDEX  diffusion_wind.manual_net_metering_availability_state_abbr_btree;

CREATE INDEX manual_net_metering_availability_state_abbr_btree
  ON  diffusion_wind.manual_net_metering_availability
  USING btree
  (state_abbr COLLATE pg_catalog."default");

-- Index:  diffusion_wind.manual_net_metering_availability_utility_type_btree

-- DROP INDEX  diffusion_wind.manual_net_metering_availability_utility_type_btree;

CREATE INDEX manual_net_metering_availability_utility_type_btree
  ON  diffusion_wind.manual_net_metering_availability
  USING btree
  (utility_type COLLATE pg_catalog."default");