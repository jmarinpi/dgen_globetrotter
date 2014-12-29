CREATE TABLE diffusion_solar.nem_manual_policies
(
  state_abbr character varying(2),
  year numeric,
  nem_availability boolean,
  sellback_rate_dol_kwh numeric
)
WITH (
  OIDS=FALSE
);
ALTER TABLE diffusion_solar.nem_manual_policies
  OWNER TO "diffusion-writers";