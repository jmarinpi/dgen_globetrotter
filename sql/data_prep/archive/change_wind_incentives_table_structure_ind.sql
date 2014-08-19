-- group the incentives into arrays so that there is just one row for each pt_gid
DROP TABLE IF EXISTS diffusion_wind_data.dsire_incentives_combos_lookup_ind;
CREATE TABLE diffusion_wind_data.dsire_incentives_combos_lookup_ind AS
SELECT pt_gid, array_agg(wind_incentives_uid order by wind_incentives_uid) as wind_incentives_uid_array
FROM diffusion_wind.dsire_incentives_lookup_ind
group by pt_gid;

-- find the unique set of incentive arrays
DROP TABLE IF EXISTS diffusion_wind_data.dsire_incentives_unique_combos_ind;
CREATE TABLE diffusion_wind_data.dsire_incentives_unique_combos_ind AS
SELECT distinct(wind_incentives_uid_array) as wind_incentives_uid_array
FROM diffusion_wind_data.dsire_incentives_combos_lookup_ind;

-- add a primary key to the table of incentive arrays
ALTER TABLE diffusion_wind_data.dsire_incentives_unique_combos_ind
ADD column incentive_array_id serial primary key;

-- join the incentive array primary key back into the combos_lookup_table
ALTER TABLE diffusion_wind_data.dsire_incentives_combos_lookup_ind
ADD column incentive_array_id integer;

UPDATE diffusion_wind_data.dsire_incentives_combos_lookup_ind a
SET incentive_array_id = b.incentive_array_id
FROM diffusion_wind_data.dsire_incentives_unique_combos_ind b
where a.wind_incentives_uid_array = b.wind_incentives_uid_array;

-- join this info back into the main points table
ALTER TABLE diffusion_shared.pt_grid_us_ind
ADD COLUMN wind_incentive_array_id integer;

UPDATE diffusion_shared.pt_grid_us_ind a
SET wind_incentive_array_id = b.incentive_array_id
FROM diffusion_wind_data.dsire_incentives_combos_lookup_ind b
WHere a.gid = b.pt_gid;

-- add an index
CREATE INDEX pt_grid_us_ind_wind_incentive_btree 
ON diffusion_shared.pt_grid_us_ind
USING btree(wind_incentive_array_id);

--unnest the data from the unique combos table
DROP TABLE IF EXISTS diffusion_wind.dsire_incentives_simplified_lkup_ind;
CREATE TABLE diffusion_wind.dsire_incentives_simplified_lkup_ind AS
SELECT incentive_array_id as wind_incentive_array_id, 
	unnest(wind_incentives_uid_array) as wind_incentives_uid
FROM diffusion_wind_data.dsire_incentives_unique_combos_ind;

-- create index
CREATE INDEX dsire_incentives_simplified_lkup_ind_inc_id_btree
ON diffusion_wind.dsire_incentives_simplified_lkup_ind
USING btree(wind_incentive_array_id);