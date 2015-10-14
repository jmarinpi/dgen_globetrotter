set role 'diffusion-writers';
DROP TABLE IF EXISTS diffusion_solar.bass_pq_calibrated_params_solar;
CREATE TABLE diffusion_solar.bass_pq_calibrated_params_solar
(
  state_abbr character varying(2),
  p numeric,
  q numeric,
  sector_abbr text
);


\COPY diffusion_solar.bass_pq_calibrated_params_solar FROM '/Volumes/Staff/mgleason/DG_Solar/Data/Source_Data/bass_parameters/bass_pq_calibrated_params_solar.csv' with csv header;

select *
FROM diffusion_solar.bass_pq_calibrated_params_solar;

CREATE INDEX bass_pq_calibrated_params_solar_sector_abbr_btree
ON diffusion_solar.bass_pq_calibrated_params_solar
USING BTREE(sector_abbr);

CREATE INDEX bass_pq_calibrated_params_solar_state_abbr_btree
ON diffusion_solar.bass_pq_calibrated_params_solar
USING BTREE(state_abbr);
