	-- SOLAR
	--create the lookup table		
	DROP TABLE IF EXISTS diffusion_solar.dsire_incentives_lookup_ind;
	CREATE TABLE diffusion_solar.dsire_incentives_lookup_ind AS

	with a as 
	(
		SELECT b.gid, b.the_geom, d.uid as solar_incentives_uid
		FROM dg_wind.incentives_geoms_copy_diced b
		inner JOIN geo_incentives.incentives c
		ON b.gid = c.geom_id
		INNER JOIN geo_incentives.pv_incentives d
		ON c.gid = d.incentive_id
	)

	SELECT e.gid as pt_gid, a.solar_incentives_uid
	FROM a

	INNER JOIN diffusion_shared.pt_grid_us_ind e
	ON ST_Intersects(a.the_geom,e.the_geom_4326);

	CREATE INDEX dsire_incentives_lookup_ind_pt_gid_btree ON diffusion_solar.dsire_incentives_lookup_ind using btree(pt_gid);

	SELECT pt_gid, count(*)
	FROM diffusion_solar.dsire_incentives_lookup_ind
	GROUP BY pt_gid
	ORDER by count desc;

	-- group the incentives into arrays so that there is just one row for each pt_gid
	DROP TABLE IF EXISTS diffusion_solar_data.dsire_incentives_combos_lookup_ind;
	CREATE TABLE diffusion_solar_data.dsire_incentives_combos_lookup_ind AS
	SELECT pt_gid, array_agg(solar_incentives_uid order by solar_incentives_uid) as solar_incentives_uid_array
	FROM diffusion_solar.dsire_incentives_lookup_ind
	group by pt_gid;

	-- find the unique set of incentive arrays
	DROP TABLE IF EXISTS diffusion_solar_data.dsire_incentives_unique_combos_ind;
	CREATE TABLE diffusion_solar_data.dsire_incentives_unique_combos_ind AS
	SELECT distinct(solar_incentives_uid_array) as solar_incentives_uid_array
	FROM diffusion_solar_data.dsire_incentives_combos_lookup_ind;

	-- add a primary key to the table of incentive arrays
	ALTER TABLE diffusion_solar_data.dsire_incentives_unique_combos_ind
	ADD column incentive_array_id serial primary key;

	-- join the incentive array primary key back into the combos_lookup_table
	ALTER TABLE diffusion_solar_data.dsire_incentives_combos_lookup_ind
	ADD column incentive_array_id integer;

	UPDATE diffusion_solar_data.dsire_incentives_combos_lookup_ind a
	SET incentive_array_id = b.incentive_array_id
	FROM diffusion_solar_data.dsire_incentives_unique_combos_ind b
	where a.solar_incentives_uid_array = b.solar_incentives_uid_array;

	-- join this info back into the main points table
	ALTER TABLE diffusion_shared.pt_grid_us_ind
	ADD COLUMN solar_incentive_array_id integer;

	UPDATE diffusion_shared.pt_grid_us_ind a
	SET solar_incentive_array_id = b.incentive_array_id
	FROM diffusion_solar_data.dsire_incentives_combos_lookup_ind b
	WHere a.gid = b.pt_gid;
	
	-- add an index
	CREATE INDEX pt_grid_us_ind_solar_incentive_btree 
	ON diffusion_shared.pt_grid_us_ind
	USING btree(solar_incentive_array_id);

	-- check that we got tem all
	SELECT count(*)
	FROM diffusion_shared.pt_grid_us_ind
	where solar_incentive_array_id is not null;
	--2726709

	SELECT count(*)
	FROM diffusion_solar_data.dsire_incentives_combos_lookup_ind
	where incentive_array_id is not null;
	--2726709

	--unnest the data from the unique combos table
	DROP TABLE IF EXISTS diffusion_solar.dsire_incentives_simplified_lkup_ind;
	CREATE TABLE diffusion_solar.dsire_incentives_simplified_lkup_ind AS
	SELECT incentive_array_id as solar_incentive_array_id, 
		unnest(solar_incentives_uid_array) as solar_incentives_uid
	FROM diffusion_solar_data.dsire_incentives_unique_combos_ind;

	-- create index
	CREATE INDEX dsire_incentives_simplified_lkup_ind_inc_id_btree
	ON diffusion_solar.dsire_incentives_simplified_lkup_ind
	USING btree(solar_incentive_array_id);