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
-- then run the bat file from from command line

-- to load the data into postgres as separate tables, run: hdf_results_to_pg.py (windpy repo)
-- in this case, data were loaded to 7 separate tables:
	-- diffusion_wind_data.wind_resource_current_small_turbine
	-- diffusion_wind_data.wind_resource_current_mid_turbine
	-- diffusion_wind_data.wind_resource_current_large_turbine
	-- diffusion_wind_data.wind_resource_nearfuture_small_turbine
	-- diffusion_wind_data.wind_resource_nearfuture_mid_and_large_turbine
	-- diffusion_wind_data.wind_resource_future_small_turbine
	-- diffusion_wind_data.wind_resource_future_mid_and_large_turbine

-- add turbine id to each of these tables
ALTER TABLE diffusion_wind_data.wind_resource_current_small_turbine ADD COLUMN turbine_id integer;
UPDATE diffusion_wind_data.wind_resource_current_small_turbine SET turbine_id = 1;

ALTER TABLE diffusion_wind_data.wind_resource_current_mid_turbine ADD COLUMN turbine_id integer;
UPDATE diffusion_wind_data.wind_resource_current_mid_turbine SET turbine_id = 2;

ALTER TABLE diffusion_wind_data.wind_resource_current_large_turbine ADD COLUMN turbine_id integer;
UPDATE diffusion_wind_data.wind_resource_current_large_turbine SET turbine_id = 3;

ALTER TABLE diffusion_wind_data.wind_resource_nearfuture_small_turbine ADD COLUMN turbine_id integer;
UPDATE diffusion_wind_data.wind_resource_nearfuture_small_turbine SET turbine_id = 4;

ALTER TABLE diffusion_wind_data.wind_resource_nearfuture_mid_and_large_turbine ADD COLUMN turbine_id integer;
UPDATE diffusion_wind_data.wind_resource_nearfuture_mid_and_large_turbine SET turbine_id = 5;

ALTER TABLE diffusion_wind_data.wind_resource_future_small_turbine ADD COLUMN turbine_id integer;
UPDATE diffusion_wind_data.wind_resource_future_small_turbine SET turbine_id = 6;

ALTER TABLE diffusion_wind_data.wind_resource_future_mid_and_large_turbine ADD COLUMN turbine_id integer;
UPDATE diffusion_wind_data.wind_resource_future_mid_and_large_turbine SET turbine_id = 7;


--I mistakenly included 100 m profiles, which we don't need -- so delete those records to improve query performance
DELETE FROM diffusion_wind_data.wind_resource_current_small_turbine where height = 100;
DELETE FROM diffusion_wind_data.wind_resource_current_mid_turbine where height = 100;
DELETE FROM diffusion_wind_data.wind_resource_current_large_turbine where height = 100;
DELETE FROM diffusion_wind_data.wind_resource_nearfuture_small_turbine where height = 100;
DELETE FROM diffusion_wind_data.wind_resource_nearfuture_mid_and_large_turbine where height = 100;
DELETE FROM diffusion_wind_data.wind_resource_future_small_turbine where height = 100;
DELETE FROM diffusion_wind_data.wind_resource_future_mid_and_large_turbine where height = 100;


-- create parent table
DROP TABLE IF EXISTS diffusion_wind.wind_resource_annual;
CREATE TABLE diffusion_wind.wind_resource_annual (
        i integer,
        j integer,
        cf_bin integer,
        height integer,
        aep numeric,
        cf_avg numeric,
        turbine_id integer,
        excess_gen_factor numeric);




-- inherit individual turbine tables to the parent tables
-- add check constraint on turbine_id
-- add foreign key constraint to diffusion_wind.turbines table
-- add primary keys (use combos of i, j, icf, and height for now, 
-- add indices on height and turbine_id
	-- but maybe it would be better to link each of these to an iiijjjicf index from iiijjjicf_lookup?)

	--current small
	ALTER TABLE diffusion_wind_data.wind_resource_current_small_turbine INHERIT diffusion_wind.wind_resource_annual;

	ALTER TABLE diffusion_wind_data.wind_resource_current_small_turbine
		ADD CONSTRAINT wind_resource_current_small_turbine_turbine_id_check CHECK (turbine_id = 1);

	ALTER TABLE diffusion_wind_data.wind_resource_current_small_turbine
		ADD CONSTRAINT wind_resource_current_small_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES diffusion_wind.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE diffusion_wind_data.wind_resource_current_small_turbine
		ADD CONSTRAINT wind_resource_current_small_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

	CREATE INDEX wind_resource_current_small_turbine_i_j_cf_bin_btree ON diffusion_wind_data.wind_resource_current_small_turbine using btree(i,j,cf_bin);
	CREATE INDEX wind_resource_current_small_turbine_height_btree ON diffusion_wind_data.wind_resource_current_small_turbine using btree(height);

	--current mid
	ALTER TABLE diffusion_wind_data.wind_resource_current_mid_turbine INHERIT diffusion_wind.wind_resource_annual;

	ALTER TABLE diffusion_wind_data.wind_resource_current_mid_turbine
		ADD CONSTRAINT wind_resource_current_mid_turbine_turbine_id_check CHECK (turbine_id = 2);

	ALTER TABLE diffusion_wind_data.wind_resource_current_mid_turbine
		ADD CONSTRAINT wind_resource_current_mid_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES diffusion_wind.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE diffusion_wind_data.wind_resource_current_mid_turbine
		ADD CONSTRAINT wind_resource_current_mid_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

	CREATE INDEX wind_resource_current_mid_turbine_i_j_cf_bin_btree ON diffusion_wind_data.wind_resource_current_mid_turbine using btree(i,j,cf_bin);
	CREATE INDEX wind_resource_current_mid_turbine_height_btree ON diffusion_wind_data.wind_resource_current_mid_turbine using btree(height);

	--current large
	ALTER TABLE diffusion_wind_data.wind_resource_current_large_turbine INHERIT diffusion_wind.wind_resource_annual;

	ALTER TABLE diffusion_wind_data.wind_resource_current_large_turbine
		ADD CONSTRAINT wind_resource_current_large_turbine_turbine_id_check CHECK (turbine_id = 3);

	ALTER TABLE diffusion_wind_data.wind_resource_current_large_turbine
		ADD CONSTRAINT wind_resource_current_large_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES diffusion_wind.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE diffusion_wind_data.wind_resource_current_large_turbine
		ADD CONSTRAINT wind_resource_current_large_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

	CREATE INDEX wind_resource_current_large_turbine_i_j_cf_bin_btree ON diffusion_wind_data.wind_resource_current_large_turbine using btree(i,j,cf_bin);
	CREATE INDEX wind_resource_current_large_turbine_height_btree ON diffusion_wind_data.wind_resource_current_large_turbine using btree(height);

	--near future small
	ALTER TABLE diffusion_wind_data.wind_resource_nearfuture_small_turbine INHERIT diffusion_wind.wind_resource_annual;

	ALTER TABLE diffusion_wind_data.wind_resource_nearfuture_small_turbine
		ADD CONSTRAINT wind_resource_nearfuture_small_turbine_turbine_id_check CHECK (turbine_id = 4);

	ALTER TABLE diffusion_wind_data.wind_resource_nearfuture_small_turbine
		ADD CONSTRAINT wind_resource_nearfuture_small_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES diffusion_wind.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE diffusion_wind_data.wind_resource_nearfuture_small_turbine
		ADD CONSTRAINT wind_resource_nearfuture_small_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

	CREATE INDEX wind_resource_nearfuture_small_turbine_i_j_cf_bin_btree ON diffusion_wind_data.wind_resource_nearfuture_small_turbine using btree(i,j,cf_bin);
	CREATE INDEX wind_resource_nearfuture_small_turbine_height_btree ON diffusion_wind_data.wind_resource_nearfuture_small_turbine using btree(height);

	--near future mid and large
	ALTER TABLE diffusion_wind_data.wind_resource_nearfuture_mid_and_large_turbine INHERIT diffusion_wind.wind_resource_annual;

	ALTER TABLE diffusion_wind_data.wind_resource_nearfuture_mid_and_large_turbine
		ADD CONSTRAINT wind_resource_nearfuture_mid_and_large_turbine_turbine_id_check CHECK (turbine_id = 5);

	ALTER TABLE diffusion_wind_data.wind_resource_nearfuture_mid_and_large_turbine
		ADD CONSTRAINT wind_resource_nearfuture_mid_and_large_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES diffusion_wind.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE diffusion_wind_data.wind_resource_nearfuture_mid_and_large_turbine
		ADD CONSTRAINT wind_resource_nearfuture_mid_and_large_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

	CREATE INDEX wind_resource_nearfuture_mid_and_large_turbine_i_j_cf_bin_btree ON diffusion_wind_data.wind_resource_nearfuture_mid_and_large_turbine using btree(i,j,cf_bin);
	CREATE INDEX wind_resource_nearfuture_mid_and_large_turbine_height_btree ON diffusion_wind_data.wind_resource_nearfuture_mid_and_large_turbine using btree(height);


	--future small
	ALTER TABLE diffusion_wind_data.wind_resource_future_small_turbine INHERIT diffusion_wind.wind_resource_annual;

	ALTER TABLE diffusion_wind_data.wind_resource_future_small_turbine
		ADD CONSTRAINT wind_resource_future_small_turbine_turbine_id_check CHECK (turbine_id = 6);

	ALTER TABLE diffusion_wind_data.wind_resource_future_small_turbine
		ADD CONSTRAINT wind_resource_future_small_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES diffusion_wind.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE diffusion_wind_data.wind_resource_future_small_turbine
		ADD CONSTRAINT wind_resource_future_small_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);


	CREATE INDEX wind_resource_future_small_turbine_i_j_cf_bin_btree ON diffusion_wind_data.wind_resource_future_small_turbine using btree(i,j,cf_bin);
	CREATE INDEX wind_resource_future_small_turbine_height_btree ON diffusion_wind_data.wind_resource_future_small_turbine using btree(height);


	--future mid and large
	ALTER TABLE diffusion_wind_data.wind_resource_future_mid_and_large_turbine INHERIT diffusion_wind.wind_resource_annual;

	ALTER TABLE diffusion_wind_data.wind_resource_future_mid_and_large_turbine
		ADD CONSTRAINT wind_resource_future_mid_and_large_turbine_turbine_id_check CHECK (turbine_id = 7);

	ALTER TABLE diffusion_wind_data.wind_resource_future_mid_and_large_turbine
		ADD CONSTRAINT wind_resource_future_mid_and_large_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES diffusion_wind.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE diffusion_wind_data.wind_resource_future_mid_and_large_turbine
		ADD CONSTRAINT wind_resource_future_mid_and_large_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

	CREATE INDEX wind_resource_future_mid_and_large_turbine_i_j_cf_bin_btree ON diffusion_wind_data.wind_resource_future_mid_and_large_turbine using btree(i,j,cf_bin);
	CREATE INDEX wind_resource_future_mid_and_large_turbine_height_btree ON diffusion_wind_data.wind_resource_future_mid_and_large_turbine using btree(height);


-- add in excess generation factor data
-- to calculate, first run: /Volumes/Staff/mgleason/DG_Wind/Python/excess_generation_factors/calculate_excess_generation.py
-- load these results into postgres, using: /Volumes/Staff/mgleason/DG_Wind/Python/excess_generation_factors/excess_generation_hdf_results_to_pg.py

-- add columns for excess generation factor values to the wind resource tables
ALTER TABLE diffusion_wind_data.wind_resource_current_small_turbine
ADD COLUMN excess_gen_factor numeric;

ALTER TABLE diffusion_wind_data.wind_resource_current_mid_turbine
ADD COLUMN excess_gen_factor numeric;

ALTER TABLE diffusion_wind_data.wind_resource_current_large_turbine
ADD COLUMN excess_gen_factor numeric;

ALTER TABLE diffusion_wind_data.wind_resource_nearfuture_small_turbine
ADD COLUMN excess_gen_factor numeric;

ALTER TABLE diffusion_wind_data.wind_resource_nearfuture_mid_and_large_turbine
ADD COLUMN excess_gen_factor numeric;

ALTER TABLE diffusion_wind_data.wind_resource_future_small_turbine
ADD COLUMN excess_gen_factor numeric;

ALTER TABLE diffusion_wind_data.wind_resource_future_mid_and_large_turbine
ADD COLUMN excess_gen_factor numeric;

-- add primary keys to the excess generation tables
ALTER TABLE diffusion_wind_data.excess_generation_factors_current_small_turbine
	ADD CONSTRAINT excess_generation_factors_current_small_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

ALTER TABLE diffusion_wind_data.excess_generation_factors_current_mid_turbine
	ADD CONSTRAINT excess_generation_factors_current_mid_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

ALTER TABLE diffusion_wind_data.excess_generation_factors_current_large_turbine
	ADD CONSTRAINT excess_generation_factors_current_large_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

ALTER TABLE diffusion_wind_data.excess_generation_factors_nearfuture_small_turbine
	ADD CONSTRAINT excess_generation_factors_nearfuture_small_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

ALTER TABLE diffusion_wind_data.excess_generation_factors_nearfuture_mid_and_large_turbine
	ADD CONSTRAINT excess_generation_factors_nearfuture_mid_and_large_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

ALTER TABLE diffusion_wind_data.excess_generation_factors_future_small_turbine
	ADD CONSTRAINT excess_generation_factors_future_small_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

ALTER TABLE diffusion_wind_data.excess_generation_factors_future_mid_and_large_turbine
	ADD CONSTRAINT excess_generation_factors_future_mid_and_large_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

-- join the data from excess gen factors to resource tables

-- update
UPDATE diffusion_wind_data.wind_resource_current_small_turbine a
SET excess_gen_factor = b.excess_gen_factor
FROM diffusion_wind_data.excess_generation_factors_current_small_turbine b
where a.i = b.i
and a.j = b.j
and a.cf_bin = b.cf_bin
and a.height = b.height;

-- check for nulls
SELECT *
FROM diffusion_wind_data.wind_resource_current_small_turbine
where excess_gen_factor is null;

-- update
UPDATE diffusion_wind_data.wind_resource_current_mid_turbine a
SET excess_gen_factor = b.excess_gen_factor
FROM diffusion_wind_data.excess_generation_factors_current_mid_turbine b
where a.i = b.i
and a.j = b.j
and a.cf_bin = b.cf_bin
and a.height = b.height;

-- check for nulls
SELECT *
FROM diffusion_wind_data.wind_resource_current_mid_turbine
where excess_gen_factor is null;

-- update
UPDATE diffusion_wind_data.wind_resource_current_large_turbine a
SET excess_gen_factor = b.excess_gen_factor
FROM diffusion_wind_data.excess_generation_factors_current_large_turbine b
where a.i = b.i
and a.j = b.j
and a.cf_bin = b.cf_bin
and a.height = b.height;

-- check for nulls
SELECT *
FROM diffusion_wind_data.wind_resource_current_large_turbine
where excess_gen_factor is null;

-- update
UPDATE diffusion_wind_data.wind_resource_nearfuture_small_turbine a
SET excess_gen_factor = b.excess_gen_factor
FROM diffusion_wind_data.excess_generation_factors_nearfuture_small_turbine b
where a.i = b.i
and a.j = b.j
and a.cf_bin = b.cf_bin
and a.height = b.height;

-- check for nulls
SELECT *
FROM diffusion_wind_data.wind_resource_nearfuture_small_turbine
where excess_gen_factor is null;

-- update
UPDATE diffusion_wind_data.wind_resource_nearfuture_mid_and_large_turbine a
SET excess_gen_factor = b.excess_gen_factor
FROM diffusion_wind_data.excess_generation_factors_nearfuture_mid_and_large_turbine b
where a.i = b.i
and a.j = b.j
and a.cf_bin = b.cf_bin
and a.height = b.height;

-- check for nulls
SELECT *
FROM diffusion_wind_data.wind_resource_nearfuture_mid_and_large_turbine
where excess_gen_factor is null;


-- update
UPDATE diffusion_wind_data.wind_resource_future_small_turbine a
SET excess_gen_factor = b.excess_gen_factor
FROM diffusion_wind_data.excess_generation_factors_future_small_turbine b
where a.i = b.i
and a.j = b.j
and a.cf_bin = b.cf_bin
and a.height = b.height;

-- check for nulls
SELECT *
FROM diffusion_wind_data.wind_resource_future_small_turbine
where excess_gen_factor is null;

-- update
UPDATE diffusion_wind_data.wind_resource_future_mid_and_large_turbine a
SET excess_gen_factor = b.excess_gen_factor
FROM diffusion_wind_data.excess_generation_factors_future_mid_and_large_turbine b
where a.i = b.i
and a.j = b.j
and a.cf_bin = b.cf_bin
and a.height = b.height;

-- check for nulls
SELECT *
FROM diffusion_wind_data.wind_resource_future_mid_and_large_turbine
where excess_gen_factor is null;

-- check parent table for nulls
SELECT *
FROM diffusion_wind.wind_resource_annual
where excess_gen_factor is null;
