-- to create wind resource AEP and CF AVG data, run: windspeed2power (windpy repo) on each of the turbines of interest
-- e.g., create windows batch file (.bat) as follows:
	-- call C:\VIRTUAL_ENV\mgleason\py27_64bit_main\Scripts\activate
	-- call python D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\windspeed2power.py -mode each_turbine -turbines DG_Wind_Current_Small -hts 30 40 50 80 100 -o D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\outputs\hdf\DG_Wind -annual-only > D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\outputs\hdf\DG_Wind\DG_Wind_Current_Small.txt
	-- call python D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\windspeed2power.py -mode each_turbine -turbines DG_Wind_Current_Mid -hts 30 40 50 80 100 -o D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\outputs\hdf\DG_Wind -annual-only > D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\outputs\hdf\DG_Wind\DG_Wind_Current_Mid.txt
	-- call python D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\windspeed2power.py -mode each_turbine -turbines DG_Wind_Current_Large -hts 30 40 50 80 100 -o D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\outputs\hdf\DG_Wind -annual-only > D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\outputs\hdf\DG_Wind\DG_Wind_Current_Large.txt
	-- call python D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\windspeed2power.py -mode each_turbine -turbines DG_Wind_NearFuture_Small -hts 30 40 50 80 100 -o D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\outputs\hdf\DG_Wind -annual-only > D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\outputs\hdf\DG_Wind\DG_Wind_NearFuture_Small.txt
	-- call python D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\windspeed2power.py -mode each_turbine -turbines DG_Wind_NearFuture_Mid_and_Large -hts 30 40 50 80 100 -o D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\outputs\hdf\DG_Wind -annual-only > D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\outputs\hdf\DG_Wind\DG_Wind_NearFuture_Mid_and_Large.txt
	-- call python D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\windspeed2power.py -mode each_turbine -turbines DG_Wind_Future_Small -hts 30 40 50 80 100 -o D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\outputs\hdf\DG_Wind -annual-only > D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\outputs\hdf\DG_Wind\DG_Wind_Future_Small.txt
	-- call python D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\windspeed2power.py -mode each_turbine -turbines DG_Wind_Future_Mid_and_Large -hts 30 40 50 80 100 -o D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\outputs\hdf\DG_Wind -annual-only > D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\windpy\outputs\hdf\DG_Wind\DG_Wind_Future_Mid_and_Large.txt
-- then run the bat fiele from from command line

-- to load the data into postgres as separate tables, run: hdf_results_to_pg.py (windpy repo)
-- in this case, data were loaded to 7 separate tables:
	-- wind_ds_data.wind_resource_current_small_turbine
	-- wind_ds_data.wind_resource_current_mid_turbine
	-- wind_ds_data.wind_resource_current_large_turbine
	-- wind_ds_data.wind_resource_nearfuture_small_turbine
	-- wind_ds_data.wind_resource_nearfuture_mid_and_large_turbine
	-- wind_ds_data.wind_resource_future_small_turbine
	-- wind_ds_data.wind_resource_future_mid_and_large_turbine

-- add turbine id to each of these tables
ALTER TABLE wind_ds_data.wind_resource_current_small_turbine ADD COLUMN turbine_id integer;
UPDATE wind_ds_data.wind_resource_current_small_turbine SET turbine_id = 1;

ALTER TABLE wind_ds_data.wind_resource_current_mid_turbine ADD COLUMN turbine_id integer;
UPDATE wind_ds_data.wind_resource_current_mid_turbine SET turbine_id = 2;

ALTER TABLE wind_ds_data.wind_resource_current_large_turbine ADD COLUMN turbine_id integer;
UPDATE wind_ds_data.wind_resource_current_large_turbine SET turbine_id = 3;

ALTER TABLE wind_ds_data.wind_resource_nearfuture_small_turbine ADD COLUMN turbine_id integer;
UPDATE wind_ds_data.wind_resource_nearfuture_small_turbine SET turbine_id = 4;

ALTER TABLE wind_ds_data.wind_resource_nearfuture_mid_and_large_turbine ADD COLUMN turbine_id integer;
UPDATE wind_ds_data.wind_resource_nearfuture_mid_and_large_turbine SET turbine_id = 5;

ALTER TABLE wind_ds_data.wind_resource_future_small_turbine ADD COLUMN turbine_id integer;
UPDATE wind_ds_data.wind_resource_future_small_turbine SET turbine_id = 6;

ALTER TABLE wind_ds_data.wind_resource_future_mid_and_large_turbine ADD COLUMN turbine_id integer;
UPDATE wind_ds_data.wind_resource_future_mid_and_large_turbine SET turbine_id = 7;


--I mistakenly included 100 m profiles, which we don't need -- so delete those records to improve query performance
DELETE FROM wind_ds_data.wind_resource_current_small_turbine where height = 100;
DELETE FROM wind_ds_data.wind_resource_current_mid_turbine where height = 100;
DELETE FROM wind_ds_data.wind_resource_current_large_turbine where height = 100;
DELETE FROM wind_ds_data.wind_resource_nearfuture_small_turbine where height = 100;
DELETE FROM wind_ds_data.wind_resource_nearfuture_mid_and_large_turbine where height = 100;
DELETE FROM wind_ds_data.wind_resource_future_small_turbine where height = 100;
DELETE FROM wind_ds_data.wind_resource_future_mid_and_large_turbine where height = 100;


-- create parent table
DROP TABLE IF EXISTS wind_ds.wind_resource_annual;
CREATE TABLE wind_ds.wind_resource_annual (
        i integer,
        j integer,
        cf_bin integer,
        height integer,
        aep numeric,
        cf_avg numeric,
        turbine_id integer);





-- inherit individual turbine tables to the parent tables
-- add check constraint on turbine_id
-- add foreign key constraint to wind_ds.turbines table
-- add primary keys (use combos of i, j, icf, and height for now, 
-- add indices on height and turbine_id
	-- but maybe it would be better to link each of these to an iiijjjicf index from iiijjjicf_lookup?)

	--current small
	ALTER TABLE wind_ds_data.wind_resource_current_small_turbine INHERIT wind_ds.wind_resource_annual;

	ALTER TABLE wind_ds_data.wind_resource_current_small_turbine
		ADD CONSTRAINT wind_resource_current_small_turbine_turbine_id_check CHECK (turbine_id = 1);

	ALTER TABLE wind_ds_data.wind_resource_current_small_turbine
		ADD CONSTRAINT wind_resource_current_small_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES wind_ds.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE wind_ds_data.wind_resource_current_small_turbine
		ADD CONSTRAINT wind_resource_current_small_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

	--current mid
	ALTER TABLE wind_ds_data.wind_resource_current_mid_turbine INHERIT wind_ds.wind_resource_annual;

	ALTER TABLE wind_ds_data.wind_resource_current_mid_turbine
		ADD CONSTRAINT wind_resource_current_mid_turbine_turbine_id_check CHECK (turbine_id = 2);

	ALTER TABLE wind_ds_data.wind_resource_current_mid_turbine
		ADD CONSTRAINT wind_resource_current_mid_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES wind_ds.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE wind_ds_data.wind_resource_current_mid_turbine
		ADD CONSTRAINT wind_resource_current_mid_turbine_pkey PRIMARY KEY(i, j, cf_bin, height, turbine_id);


	--current large
	ALTER TABLE wind_ds_data.wind_resource_current_large_turbine INHERIT wind_ds.wind_resource_annual;

	ALTER TABLE wind_ds_data.wind_resource_current_large_turbine
		ADD CONSTRAINT wind_resource_current_large_turbine_turbine_id_check CHECK (turbine_id = 3);

	ALTER TABLE wind_ds_data.wind_resource_current_large_turbine
		ADD CONSTRAINT wind_resource_current_large_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES wind_ds.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE wind_ds_data.wind_resource_current_large_turbine
		ADD CONSTRAINT wind_resource_current_large_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);


	--near future small
	ALTER TABLE wind_ds_data.wind_resource_nearfuture_small_turbine INHERIT wind_ds.wind_resource_annual;

	ALTER TABLE wind_ds_data.wind_resource_nearfuture_small_turbine
		ADD CONSTRAINT wind_resource_nearfuture_small_turbine_turbine_id_check CHECK (turbine_id = 4);

	ALTER TABLE wind_ds_data.wind_resource_nearfuture_small_turbine
		ADD CONSTRAINT wind_resource_nearfuture_small_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES wind_ds.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE wind_ds_data.wind_resource_nearfuture_small_turbine
		ADD CONSTRAINT wind_resource_nearfuture_small_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

	--near future mid and large
	ALTER TABLE wind_ds_data.wind_resource_nearfuture_mid_and_large_turbine INHERIT wind_ds.wind_resource_annual;

	ALTER TABLE wind_ds_data.wind_resource_nearfuture_mid_and_large_turbine
		ADD CONSTRAINT wind_resource_nearfuture_mid_and_large_turbine_turbine_id_check CHECK (turbine_id = 5);

	ALTER TABLE wind_ds_data.wind_resource_nearfuture_mid_and_large_turbine
		ADD CONSTRAINT wind_resource_nearfuture_mid_and_large_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES wind_ds.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE wind_ds_data.wind_resource_nearfuture_mid_and_large_turbine
		ADD CONSTRAINT wind_resource_nearfuture_mid_and_large_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

	--future small
	ALTER TABLE wind_ds_data.wind_resource_future_small_turbine INHERIT wind_ds.wind_resource_annual;

	ALTER TABLE wind_ds_data.wind_resource_future_small_turbine
		ADD CONSTRAINT wind_resource_future_small_turbine_turbine_id_check CHECK (turbine_id = 6);

	ALTER TABLE wind_ds_data.wind_resource_future_small_turbine
		ADD CONSTRAINT wind_resource_future_small_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES wind_ds.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE wind_ds_data.wind_resource_future_small_turbine
		ADD CONSTRAINT wind_resource_future_small_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);


	--future mid and large
	ALTER TABLE wind_ds_data.wind_resource_future_mid_and_large_turbine INHERIT wind_ds.wind_resource_annual;

	ALTER TABLE wind_ds_data.wind_resource_future_mid_and_large_turbine
		ADD CONSTRAINT wind_resource_future_mid_and_large_turbine_turbine_id_check CHECK (turbine_id = 7);

	ALTER TABLE wind_ds_data.wind_resource_future_mid_and_large_turbine
		ADD CONSTRAINT wind_resource_future_mid_and_large_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES wind_ds.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE wind_ds_data.wind_resource_future_mid_and_large_turbine
		ADD CONSTRAINT wind_resource_future_mid_and_large_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);



