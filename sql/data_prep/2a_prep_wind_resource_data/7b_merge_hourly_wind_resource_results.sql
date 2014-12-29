-- to create wind resource AEP and CF AVG data, run: windspeed2power (windpy repo) on each of the turbines of interest
-- to load the data into postgres as separate tables, run: hdf_results_to_pg.py (windpy repo)
-- in this case, data were loaded to 7 separate tables:
	-- diffusion_wind.wind_resource_hourly_current_residential_turbine
	-- diffusion_wind.wind_resource_hourly_current_small_commercial_turbine
	-- diffusion_wind.wind_resource_hourly_current_mid_size_turbine	
	-- diffusion_wind.wind_resource_hourly_current_large_turbine
	-- diffusion_wind.wind_resource_hourly_near_future_residential_turbine
	-- diffusion_wind.wind_resource_hourly_far_future_small_turbine
	-- diffusion_wind.wind_resource_hourly_near_future_mid_size_turbine
	-- diffusion_wind.wind_resource_hourly_far_future_mid_size_and_large_turbine

	
-- add turbine id to each of these tables
ALTER TABLE diffusion_wind.wind_resource_hourly_current_residential_turbine 
ADD COLUMN turbine_id integer;
UPDATE diffusion_wind.wind_resource_hourly_current_residential_turbine 
SET turbine_id = 1;

ALTER TABLE diffusion_wind.wind_resource_hourly_current_small_commercial_turbine 
ADD COLUMN turbine_id integer;
UPDATE diffusion_wind.wind_resource_hourly_current_small_commercial_turbine 
SET turbine_id = 2;

ALTER TABLE diffusion_wind.wind_resource_hourly_current_mid_size_turbine 
ADD COLUMN turbine_id integer;
UPDATE diffusion_wind.wind_resource_hourly_current_mid_size_turbine 
SET turbine_id = 3;

ALTER TABLE diffusion_wind.wind_resource_hourly_current_large_turbine 
ADD COLUMN turbine_id integer;
UPDATE diffusion_wind.wind_resource_hourly_current_large_turbine 
SET turbine_id = 4;

ALTER TABLE diffusion_wind.wind_resource_hourly_near_future_residential_turbine 
ADD COLUMN turbine_id integer;
UPDATE diffusion_wind.wind_resource_hourly_near_future_residential_turbine 
SET turbine_id = 5;

ALTER TABLE diffusion_wind.wind_resource_hourly_far_future_small_turbine 
ADD COLUMN turbine_id integer;
UPDATE diffusion_wind.wind_resource_hourly_far_future_small_turbine 
SET turbine_id = 6;

ALTER TABLE diffusion_wind.wind_resource_hourly_near_future_mid_size_turbine 
ADD COLUMN turbine_id integer;
UPDATE diffusion_wind.wind_resource_hourly_near_future_mid_size_turbine 
SET turbine_id = 7;

ALTER TABLE diffusion_wind.wind_resource_hourly_far_future_mid_size_and_large_turbine 
ADD COLUMN turbine_id integer;
UPDATE diffusion_wind.wind_resource_hourly_far_future_mid_size_and_large_turbine 
SET turbine_id = 8;

-- create parent table
DROP TABLE IF EXISTS diffusion_wind.wind_resource_hourly;
CREATE TABLE diffusion_wind.wind_resource_hourly 
(
        i integer,
        j integer,
        cf_bin integer,
        height integer,
        cf integer[],
        turbine_id integer
);




-- inherit individual turbine tables to the parent tables
-- add check constraint on turbine_id
-- add foreign key constraint to diffusion_wind.turbines table
-- add primary keys (use combos of i, j, icf, and height for now, 
-- add indices on height and turbine_id



	-- wind_resource_hourly_current_residential_turbine
	ALTER TABLE diffusion_wind.wind_resource_hourly_current_residential_turbine INHERIT diffusion_wind.wind_resource_hourly;

	ALTER TABLE diffusion_wind.wind_resource_hourly_current_residential_turbine
		ADD CONSTRAINT wind_resource_hourly_current_residential_turbine_turbine_id_check CHECK (turbine_id = 1);

	ALTER TABLE diffusion_wind.wind_resource_hourly_current_residential_turbine
		ADD CONSTRAINT wind_resource_hourly_current_residential_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES diffusion_wind.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE diffusion_wind.wind_resource_hourly_current_residential_turbine
		ADD CONSTRAINT wind_resource_hourly_current_residential_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

	CREATE INDEX wind_resource_hourly_current_residential_turbine_i_j_cf_bin_btree ON diffusion_wind.wind_resource_hourly_current_residential_turbine using btree(i,j,cf_bin);
	CREATE INDEX wind_resource_hourly_current_residential_turbine_height_btree ON diffusion_wind.wind_resource_hourly_current_residential_turbine using btree(height);

	-- wind_resource_hourly_current_small_commercial_turbine
	ALTER TABLE diffusion_wind.wind_resource_hourly_current_small_commercial_turbine INHERIT diffusion_wind.wind_resource_hourly;

	ALTER TABLE diffusion_wind.wind_resource_hourly_current_small_commercial_turbine
		ADD CONSTRAINT wind_resource_hourly_current_small_commercial_turbine_turbine_id_check CHECK (turbine_id = 2);

	ALTER TABLE diffusion_wind.wind_resource_hourly_current_small_commercial_turbine
		ADD CONSTRAINT wind_resource_hourly_current_small_commercial_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES diffusion_wind.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE diffusion_wind.wind_resource_hourly_current_small_commercial_turbine
		ADD CONSTRAINT wind_resource_hourly_current_small_commercial_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

	CREATE INDEX wind_resource_hourly_current_small_commercial_turbine_i_j_cf_bin_btree ON diffusion_wind.wind_resource_hourly_current_small_commercial_turbine using btree(i,j,cf_bin);
	CREATE INDEX wind_resource_hourly_current_small_commercial_turbine_height_btree ON diffusion_wind.wind_resource_hourly_current_small_commercial_turbine using btree(height);

	-- wind_resource_hourly_current_mid_size_turbine	
	ALTER TABLE diffusion_wind.wind_resource_hourly_current_mid_size_turbine INHERIT diffusion_wind.wind_resource_hourly;

	ALTER TABLE diffusion_wind.wind_resource_hourly_current_mid_size_turbine
		ADD CONSTRAINT wind_resource_hourly_current_mid_size_turbine_turbine_id_check CHECK (turbine_id = 3);

	ALTER TABLE diffusion_wind.wind_resource_hourly_current_mid_size_turbine
		ADD CONSTRAINT wind_resource_hourly_current_mid_size_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES diffusion_wind.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE diffusion_wind.wind_resource_hourly_current_mid_size_turbine
		ADD CONSTRAINT wind_resource_hourly_current_mid_size_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

	CREATE INDEX wind_resource_hourly_current_mid_size_turbine_i_j_cf_bin_btree ON diffusion_wind.wind_resource_hourly_current_mid_size_turbine using btree(i,j,cf_bin);
	CREATE INDEX wind_resource_hourly_current_mid_size_turbine_height_btree ON diffusion_wind.wind_resource_hourly_current_mid_size_turbine using btree(height);

	-- wind_resource_hourly_current_large_turbine
	ALTER TABLE diffusion_wind.wind_resource_hourly_current_large_turbine INHERIT diffusion_wind.wind_resource_hourly;

	ALTER TABLE diffusion_wind.wind_resource_hourly_current_large_turbine
		ADD CONSTRAINT wind_resource_hourly_current_large_turbine_turbine_id_check CHECK (turbine_id = 4);

	ALTER TABLE diffusion_wind.wind_resource_hourly_current_large_turbine
		ADD CONSTRAINT wind_resource_hourly_current_large_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES diffusion_wind.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE diffusion_wind.wind_resource_hourly_current_large_turbine
		ADD CONSTRAINT wind_resource_hourly_current_large_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

	CREATE INDEX wind_resource_hourly_current_large_turbine_i_j_cf_bin_btree ON diffusion_wind.wind_resource_hourly_current_large_turbine using btree(i,j,cf_bin);
	CREATE INDEX wind_resource_hourly_current_large_turbine_height_btree ON diffusion_wind.wind_resource_hourly_current_large_turbine using btree(height);

	-- wind_resource_hourly_near_future_residential_turbine
	ALTER TABLE diffusion_wind.wind_resource_hourly_near_future_residential_turbine INHERIT diffusion_wind.wind_resource_hourly;

	ALTER TABLE diffusion_wind.wind_resource_hourly_near_future_residential_turbine
		ADD CONSTRAINT wind_resource_hourly_near_future_residential_turbine_turbine_id_check CHECK (turbine_id = 5);

	ALTER TABLE diffusion_wind.wind_resource_hourly_near_future_residential_turbine
		ADD CONSTRAINT wind_resource_hourly_near_future_residential_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES diffusion_wind.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE diffusion_wind.wind_resource_hourly_near_future_residential_turbine
		ADD CONSTRAINT wind_resource_hourly_near_future_residential_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

	CREATE INDEX wind_resource_hourly_near_future_residential_turbine_i_j_cf_bin_btree ON diffusion_wind.wind_resource_hourly_near_future_residential_turbine using btree(i,j,cf_bin);
	CREATE INDEX wind_resource_hourly_near_future_residential_turbine_height_btree ON diffusion_wind.wind_resource_hourly_near_future_residential_turbine using btree(height);
	
	-- wind_resource_hourly_far_future_small_turbine
	ALTER TABLE diffusion_wind.wind_resource_hourly_far_future_small_turbine INHERIT diffusion_wind.wind_resource_hourly;

	ALTER TABLE diffusion_wind.wind_resource_hourly_far_future_small_turbine
		ADD CONSTRAINT wind_resource_hourly_far_future_small_turbine_turbine_id_check CHECK (turbine_id = 6);

	ALTER TABLE diffusion_wind.wind_resource_hourly_far_future_small_turbine
		ADD CONSTRAINT wind_resource_hourly_far_future_small_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES diffusion_wind.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE diffusion_wind.wind_resource_hourly_far_future_small_turbine
		ADD CONSTRAINT wind_resource_hourly_far_future_small_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

	CREATE INDEX wind_resource_hourly_far_future_small_turbine_i_j_cf_bin_btree ON diffusion_wind.wind_resource_hourly_far_future_small_turbine using btree(i,j,cf_bin);
	CREATE INDEX wind_resource_hourly_far_future_small_turbine_height_btree ON diffusion_wind.wind_resource_hourly_far_future_small_turbine using btree(height);

	-- wind_resource_hourly_near_future_mid_size_turbine
	ALTER TABLE diffusion_wind.wind_resource_hourly_near_future_mid_size_turbine INHERIT diffusion_wind.wind_resource_hourly;

	ALTER TABLE diffusion_wind.wind_resource_hourly_near_future_mid_size_turbine
		ADD CONSTRAINT wind_resource_hourly_near_future_mid_size_turbine_turbine_id_check CHECK (turbine_id = 7);

	ALTER TABLE diffusion_wind.wind_resource_hourly_near_future_mid_size_turbine
		ADD CONSTRAINT wind_resource_hourly_near_future_mid_size_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES diffusion_wind.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE diffusion_wind.wind_resource_hourly_near_future_mid_size_turbine
		ADD CONSTRAINT wind_resource_hourly_near_future_mid_size_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

	CREATE INDEX wind_resource_hourly_near_future_mid_size_turbine_i_j_cf_bin_btree ON diffusion_wind.wind_resource_hourly_near_future_mid_size_turbine using btree(i,j,cf_bin);
	CREATE INDEX wind_resource_hourly_near_future_mid_size_turbine_height_btree ON diffusion_wind.wind_resource_hourly_near_future_mid_size_turbine using btree(height);
	
	-- wind_resource_hourly_far_future_mid_size_and_large_turbine
	ALTER TABLE diffusion_wind.wind_resource_hourly_far_future_mid_size_and_large_turbine INHERIT diffusion_wind.wind_resource_hourly;

	ALTER TABLE diffusion_wind.wind_resource_hourly_far_future_mid_size_and_large_turbine
		ADD CONSTRAINT wind_resource_hourly_far_future_mid_size_and_large_turbine_turbine_id_check CHECK (turbine_id = 8);

	ALTER TABLE diffusion_wind.wind_resource_hourly_far_future_mid_size_and_large_turbine
		ADD CONSTRAINT wind_resource_hourly_far_future_mid_size_and_large_turbine_id_fkey FOREIGN KEY (turbine_id)
		REFERENCES diffusion_wind.turbines (turbine_id) MATCH FULL
		ON UPDATE RESTRICT ON DELETE RESTRICT;

	ALTER TABLE diffusion_wind.wind_resource_hourly_far_future_mid_size_and_large_turbine
		ADD CONSTRAINT wind_resource_hourly_far_future_mid_size_and_large_turbine_pkey PRIMARY KEY(i, j, cf_bin, height);

	CREATE INDEX wind_resource_hourly_far_future_mid_size_and_large_turbine_i_j_cf_bin_btree ON diffusion_wind.wind_resource_hourly_far_future_mid_size_and_large_turbine using btree(i,j,cf_bin);
	CREATE INDEX wind_resource_hourly_far_future_mid_size_and_large_turbine_height_btree ON diffusion_wind.wind_resource_hourly_far_future_mid_size_and_large_turbine using btree(height);


-- check count of rows in the parent table
-- should be 888875*8 = 7111000
select count(*)
FROM diffusion_wind.wind_resource_hourly;
-- 7111000

-- count should also match wind_resource_annual
select count(*)
FROM diffusion_wind.wind_resource_annual;
-- 7111000

-- check for nulls
SELECT count(*)
FROM diffusion_wind.wind_resource_hourly
where cf = '{}';
-- 0 rows

SELECT count(*)
FROM diffusion_wind.wind_resource_hourly
where cf is null;
-- 0 rows


-- check stats seem legit 
with a as 
(
	Select r_array_sum(cf)/1000 as aep
	from diffusion_wind.wind_resource_hourly
)
select min(aep), max(aep), avg(aep)
from a;
-- 0.00000000000000000000 | 6782.0140000000000000 | 1897.01693073421459710308

select min(naep), max(naep), avg(naep)
from diffusion_wind.wind_resource_annual;
-- 0
-- 6782.27392578125
-- 1897.040326323978806682