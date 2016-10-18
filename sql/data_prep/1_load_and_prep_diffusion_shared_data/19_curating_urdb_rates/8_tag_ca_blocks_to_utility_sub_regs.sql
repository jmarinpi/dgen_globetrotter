--Tag CA blocks to utility sub region
	-- CA utilities are parsed up by sub region
	-- use centroid of the block to identify which region it belongs to
	-- ca_block_id | utility_eia_id | utility_reg_gid

------------------------------------------------
-- 1. Create Lkup table with eia_ids and gids
------------------------------------------------
	-- some rates apply to the entire region while others apply to a sub region only
	-- we create a lkup to handle these irregularities
drop table if exists diffusion_data_shared.ca_eia_id_and_gids_lkup;
create table diffusion_data_shared.ca_eia_id_and_gids_lkup as (
	with all_regions as (
		select eia_id, util_reg_gid, the_geom_96703
		from diffusion_data_shared.urdb_rates_geoms_20161005
		where utility_region = 'None' and state_fips = '06'
	),
	sub_regions as (	
		select eia_id, util_reg_gid, utility_region, the_geom_96703
		from diffusion_data_shared.urdb_rates_geoms_20161005
		where utility_region != 'None' and state_fips = '06'
	),
	all_region_sub_region_join as (
		select a.eia_id, a.util_reg_gid as util_reg_gid_of_entire_utility, 
			b.util_reg_gid as util_reg_gid_of_subregion, b.utility_region
		from all_regions a
		left join sub_regions b
		on a.eia_id = b.eia_id
		)
	select a.*, (case when a.utility_region is null then b.the_geom_96703 else c.the_geom_96703 end) as the_geom_96703
	from all_region_sub_region_join a
	left join diffusion_data_shared.urdb_rates_geoms_20161005 b
	on a.util_reg_gid_of_entire_utility = b.util_reg_gid 
	left join diffusion_data_shared.urdb_rates_geoms_20161005 c
	on a.util_reg_gid_of_subregion = c.util_reg_gid
	);

-- B. Add unique Id "CA_GID"
	-- this id refers to the unique combination of eia_id and sub region ids
alter table diffusion_data_shared.ca_eia_id_and_gids_lkup
add column ca_gid serial;

------------------------------------------------
-- 2. Tag CA Block Centroids to a Utility
------------------------------------------------
-- Use the geometry column from diffusion_data_shared.ca_eia_id_and_gids_lkup to tag block centroids to utility
drop table if exists diffusion_data_shared.ca_blocks_to_util_reg;
create table diffusion_data_shared.ca_blocks_to_util_reg
	(
		pgid bigint,
		ca_gid bigint
	);

SELECT parsel_2('dav-gis','mmooney','mmooney', 'diffusion_blocks.block_geoms', 'pgid', 
	'with ca_blocks as 
	(
		select pgid as pgid_i, the_point_96703 
		from diffusion_blocks.block_geoms as x
		where cast(state_fips as int) = 6
	)
	select a.pgid_i, b.ca_gid
	from ca_blocks a
	left join diffusion_data_shared.ca_eia_id_and_gids_lkup b
	on st_intersects(a.the_point_96703, b.the_geom_96703);', 
	'diffusion_data_shared.ca_blocks_to_util_reg', 'aa', 50);

---- B. QAQC (TODO)
--	-- B1. Check to see that the same number of blocks are in the table as block_geoms
--	-- B2. Check to make sure there are no duplicate block pgids
--	-- B2. Check to make sure that all blocks were assigned to a utility

--	-- Check total in CA blocks to utility
--	-select count(*) from diffusion_data_shared.ca_blocks_to_util_reg
--		-- total = 1626213

--	-- check to see how many blocks are in CA
--	select count(*) from diffusion_blocks.block_geoms where state_fips = '06'
--		-- total = 691487 < ca total above

--	select * from diffusion_data_shared.ca_blocks_to_util_reg limit 20
--		--1088291;486
--		--1088291;540
--		-- duplicates for a single block. 1 block is assigned to many ca_gid utilities (makes sense)--

--	select count(distinct pgid) from diffusion_data_shared.ca_blocks_to_util_reg
--		-- 691487 distinct blocks in the table
--

--------------------------------------------------
---- 3. Update Rates to Assign new CA ID??
--------------------------------------------------
--drop table if exists diffusion_data_shared.urdb_rates_attrs_lkup_20161005_2;
--create table diffusion_data_shared.urdb_rates_attrs_lkup_20161005_2 as (
--	with a as (
--		select a.*, cast(b.ca_gid as text) as ca_gid_1, null::text as ca_gid_2
--		from diffusion_data_shared.urdb_rates_attrs_lkup_20161005 a
--		left join diffusion_data_shared.ca_eia_id_and_gids_lkup b
--		on b.util_reg_gid_of_entire_utility = a.util_reg_gid
--		where a.sub_territory_name is null ),
--	b as (
--		select a.*, null::text as ca_gid_1, cast(b.ca_gid as text) as ca_gid_2
--		from diffusion_data_shared.urdb_rates_attrs_lkup_20161005 a
--		left join diffusion_data_shared.ca_eia_id_and_gids_lkup b
--		on b.util_reg_gid_of_subregion = a.util_reg_gid
--		where a.sub_territory_name is not null )
--	select a.* from a
--	union all
--	select b.* from b
--	order by state_abbr, util_reg_gid, sub_territory_name);

---- concatenate fields
--alter table  diffusion_data_shared.urdb_rates_attrs_lkup_20161005_2
--add column ca_gid text;--

--update  diffusion_data_shared.urdb_rates_attrs_lkup_20161005_2
--set ca_gid = case when ca_gid_1 is not null then ca_gid_1 else ca_gid_2 end;

----Drop extra columns & alter name
-- alter table diffusion_data_shared.urdb_rates_attrs_lkup_20161005_2
-- rename to urdb_rates_attrs_lkup_20161005;
-- alter table diffusion_data_shared.urdb_rates_attrs_lkup_20161005_2
-- drop column ca_id_1, drop column ca_id_2;

----------------------------------------------------
-- 4. CA_blocks_to_util_reg to assign util_reg_gid
----------------------------------------------------

-- What was the point in creating these CA IDs???
-- Create lkup for CA Rates? Or should I add it to the existing attributes table?	



-- check in Q to see what the spatial coverage is of these sub regions
	-- Most of CA is covered by these regions:
		-- final table should look like:
			-- pgid, eia_id as eia_id_of_all_region, eia_id_of_sub_region

