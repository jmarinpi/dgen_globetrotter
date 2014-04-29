DROP TABLE IF EXISTS wind_ds.perc_own_occ_housing_by_county;
CREATE TABLE wind_ds.perc_own_occ_housing_by_county AS
SELECT county_id, perc_own_occupied_housing as perc_ooh
FROM  dg_wind.county_own_occ_housing;

ALTER TABLE wind_ds.perc_own_occ_housing_by_county
ADD primary key (county_id);

ALTER TABLE wind_ds.perc_own_occ_housing_by_county
ADD CONSTRAINT county_id_fkey FOREIGN KEY (county_id)
      REFERENCES wind_ds.county_geom (county_id) MATCH FULL
      ON UPDATE RESTRICT ON DELETE RESTRICT;
