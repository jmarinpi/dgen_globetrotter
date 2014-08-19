-- group the incentives into arrays so that there is just one row for each pt_gid
DROP TABLE IF EXISTS diffusion_wind_data.dsire_incentives_combos_lookup_com;
CREATE TABLE diffusion_wind_data.dsire_incentives_combos_lookup_com AS
SELECT pt_gid, array_agg(wind_incentives_uid order by wind_incentives_uid) as wind_incentives_uid_array
FROM diffusion_wind.dsire_incentives_lookup_com
group by pt_gid;

-- find the unique set of incentive arrays
DROP TABLE IF EXISTS diffusion_wind_data.dsire_incentives_unique_combos_com;
CREATE TABLE diffusion_wind_data.dsire_incentives_unique_combos_com AS
SELECT distinct(wind_incentives_uid_array) as wind_incentives_uid_array
FROM diffusion_wind_data.dsire_incentives_combos_lookup_com;

-- add a primary key to the table of incentive arrays
ALTER TABLE diffusion_wind_data.dsire_incentives_unique_combos_com
ADD column incentive_array_id serial primary key;

-- join the incentive array primary key back into the combos_lookup_table
ALTER TABLE diffusion_wind_data.dsire_incentives_combos_lookup_com
ADD column incentive_array_id integer;

UPDATE diffusion_wind_data.dsire_incentives_combos_lookup_com a
SET incentive_array_id = b.incentive_array_id
FROM diffusion_wind_data.dsire_incentives_unique_combos_com b
where a.wind_incentives_uid_array = b.wind_incentives_uid_array;

-- join this info back into the main points table
ALTER TABLE diffusion_shared.pt_grid_us_com
ADD COLUMN wind_incentive_array_id integer;

UPDATE diffusion_shared.pt_grid_us_com a
SET wind_incentive_array_id = b.incentive_array_id
FROM diffusion_wind_data.dsire_incentives_combos_lookup_com b
WHere a.gid = b.pt_gid;
--**
-- add an index
CREATE INDEX pt_grid_us_com_wind_incentive_btree 
ON diffusion_shared.pt_grid_us_com
USING btree(wind_incentive_array_id);

--unnest the data from the unique combos table
DROP TABLE IF EXISTS diffusion_wind.dsire_incentives_simplified_lkup_com;
CREATE TABLE diffusion_wind.dsire_incentives_simplified_lkup_com AS
SELECT incentive_array_id as wind_incentive_array_id, 
	unnest(wind_incentives_uid_array) as wind_incentives_uid
FROM diffusion_wind_data.dsire_incentives_unique_combos_com;

-- create index
CREATE INDEX dsire_incentives_simplified_lkup_com_inc_id_btree
ON diffusion_wind.dsire_incentives_simplified_lkup_com
USING btree(wind_incentive_array_id);