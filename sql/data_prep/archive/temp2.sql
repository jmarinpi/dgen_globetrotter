	-- SOLAR
	--create the lookup table		
	DROP TABLE IF EXISTS diffusion_solar.dsire_incentives_lookup_com;
	CREATE TABLE diffusion_solar.dsire_incentives_lookup_com AS

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

	INNER JOIN diffusion_shared.pt_grid_us_com e
	ON ST_Intersects(a.the_geom,e.the_geom_4326);

	CREATE INDEX dsire_incentives_lookup_com_pt_gid_btree ON diffusion_solar.dsire_incentives_lookup_com using btree(pt_gid);

	SELECT pt_gid, count(*)
	FROM diffusion_solar.dsire_incentives_lookup_com
	GROUP BY pt_gid
	ORDER by count desc;

	-- group the incentives into arrays so that there is just one row for each pt_gid
	DROP TABLE IF EXISTS diffusion_solar_data.dsire_incentives_combos_lookup_com;
	CREATE TABLE diffusion_solar_data.dsire_incentives_combos_lookup_com AS
	SELECT pt_gid, array_agg(solar_incentives_uid order by solar_incentives_uid) as solar_incentives_uid_array
	FROM diffusion_solar.dsire_incentives_lookup_com
	group by pt_gid;

	-- find the unique set of incentive arrays
	DROP TABLE IF EXISTS diffusion_solar_data.dsire_incentives_unique_combos_com;
	CREATE TABLE diffusion_solar_data.dsire_incentives_unique_combos_com AS
	SELECT distinct(solar_incentives_uid_array) as solar_incentives_uid_array
	FROM diffusion_solar_data.dsire_incentives_combos_lookup_com;

	-- add a primary key to the table of incentive arrays
	ALTER TABLE diffusion_solar_data.dsire_incentives_unique_combos_com
	ADD column incentive_array_id serial primary key;

	-- join the incentive array primary key back into the combos_lookup_table
	ALTER TABLE diffusion_solar_data.dsire_incentives_combos_lookup_com
	ADD column incentive_array_id integer;

	UPDATE diffusion_solar_data.dsire_incentives_combos_lookup_com a
	SET incentive_array_id = b.incentive_array_id
	FROM diffusion_solar_data.dsire_incentives_unique_combos_com b
	where a.solar_incentives_uid_array = b.solar_incentives_uid_array;

	-- join this info back into the main points table
	ALTER TABLE diffusion_shared.pt_grid_us_com
	ADD COLUMN solar_incentive_array_id integer;

	UPDATE diffusion_shared.pt_grid_us_com a
	SET solar_incentive_array_id = b.incentive_array_id
	FROM diffusion_solar_data.dsire_incentives_combos_lookup_com b
	WHere a.gid = b.pt_gid;
	
	-- add an index
	CREATE INDEX pt_grid_us_com_solar_incentive_btree 
	ON diffusion_shared.pt_grid_us_com
	USING btree(solar_incentive_array_id);

	-- check that we got tem all
	SELECT count(*)
	FROM diffusion_shared.pt_grid_us_com
	where solar_incentive_array_id is not null;
	--5158830

	SELECT count(*)
	FROM diffusion_solar_data.dsire_incentives_combos_lookup_com
	where incentive_array_id is not null;
	--5158830

	--unnest the data from the unique combos table
	DROP TABLE IF EXISTS diffusion_solar.dsire_incentives_simplified_lkup_com;
	CREATE TABLE diffusion_solar.dsire_incentives_simplified_lkup_com AS
	SELECT incentive_array_id as solar_incentive_array_id, 
		unnest(solar_incentives_uid_array) as solar_incentives_uid
	FROM diffusion_solar_data.dsire_incentives_unique_combos_com;

	-- create index
	CREATE INDEX dsire_incentives_simplified_lkup_com_inc_id_btree
	ON diffusion_solar.dsire_incentives_simplified_lkup_com
	USING btree(solar_incentive_array_id);