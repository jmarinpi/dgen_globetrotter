-- move results over to wind_ds
DROP TABLE IF EXISTS wind_ds.load_and_customers_by_county;
CREATE TABLE wind_ds.load_and_customers_by_county AS
SELECT a.county_id, b.total_customers_2011_residential, c.total_customers_2011_commercial,
	d.total_load_mwh_2011_residential, e.total_load_mwh_2011_commercial
FROM wind_ds.county_geom a
LEFT JOIN dg_wind.res_customers_by_county_us b
ON a.county_id = b.county_id
LEFT JOIN dg_wind.com_customers_by_county_us c
ON a.county_id = c.county_id
LEFT JOIN dg_wind.res_load_by_county_us d
ON a.county_id = d.county_id
LEFT JOIN dg_wind.com_load_by_county_us e
ON a.county_id = e.county_id;

ALTER TABLE wind_ds.load_and_customers_by_county ADD primary key(county_id);

ALTER TABLE wind_ds.load_and_customers_by_county ADD primary key(county_id);


-- add foreign keys to pt tables
ALTER TABLE wind_ds.pt_grid_us_res ADD CONSTRAINT county_id FOREIGN KEY (county_id) 
REFERENCES wind_ds.load_and_customers_by_county (county_id) MATCH FULL 
ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE wind_ds.pt_grid_us_com ADD CONSTRAINT county_id FOREIGN KEY (county_id) 
REFERENCES wind_ds.load_and_customers_by_county (county_id) MATCH FULL 
ON UPDATE RESTRICT ON DELETE RESTRICT;

-- add foreign key to wind_ds.load_and_customers_by_county
ALTER TABLE wind_ds.load_and_customers_by_county ADD CONSTRAINT county_id FOREIGN KEY (county_id) 
REFERENCES wind_ds.county_geom (county_id) MATCH FULL 
ON UPDATE RESTRICT ON DELETE RESTRICT;