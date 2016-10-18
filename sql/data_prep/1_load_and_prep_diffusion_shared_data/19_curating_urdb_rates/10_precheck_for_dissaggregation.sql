#2. Develop Blocks to utility + utility type probabilities
	Disagregate known utility customer counts to blocks
		- County to utility (not utility type) weighting schemes
		- based on number customers/ bldg count and known population

	-- Known utility customer counts:
		-- from Galen's county to eia_id mapping table (eia.eia_861_2013_county_utility_rates)
		-- first check to make sure that all eia_ids are accounted for

-----------------------------------------------------
-- Check to make sure all eia_ids are accounted for:
-----------------------------------------------------
	with b as (select distinct eia_id from diffusion_data_shared.urdb_rates_attrs_lkup_20161005)
	select a.utility_num, b.eia_id 
	from eia.eia_861_2013_county_utility_rates a
	right join b
	on cast(a.utility_num as text) = b.eia_id
	order by a.utility_num desc

	-- 13 eia_ids not accounted for in eia.eia_861_2013_county_utility_rates
		-- "19174"
		-- "2268"
		-- "40300"
		-- "4062"
		-- "5553"
		-- "9026"
		-- "5729"
		-- "11235"
		-- "No eia id given"
		-- "14272"
		-- "3292"
		-- "26751"
		-- "25251"

		-- how many rates are affected by this?
			select * from diffusion_data_shared.urdb_rates_attrs_lkup_20161005 where eia_id in ('19174','2268','40300','4062','5553','9026','5729','11235','No eia id given','14272','3292','26751','25251')
			-- between the 13 missing records, there are 118 rates
	
-- Check to make sure that all utility ids have a customer count
	select distinct (utility_num) from eia.eia_861_2013_county_utility_rates 
	where res_customers is null and comm_customers is null and ind_customers is null
	-- 1145 records are null/ have no customer counts
		-- Check to see if any of our eia_ids fall within these null customer cnts
			with b as (
				select distinct eia_id 
				from diffusion_data_shared.urdb_rates_attrs_lkup_20161005),
			a as (
				select distinct (utility_num) 
				from eia.eia_861_2013_county_utility_rates 
				where res_customers is null and comm_customers is null and ind_customers is null)
			select a.utility_num, b.eia_id 
			from a
			right join b
			on cast(a.utility_num as text) = b.eia_id
			where eia_id not in ('19174','2268','40300','4062','5553','9026','5729','11235','No eia id given','14272','3292','26751','25251')
			and utility_num is null
			order by a.utility_num desc
		-- 804 utilities do not have customer counts

	-- how many of our utilities DO HAVE customer counts?
		--1123 - 804 = * only 306 (out of 1123 utilities) have customer counts (from 2013 data)


---------------------------------------------------------
-- ALL Eia ids not accounted for; Check with 2014 data
--------------------------------------------------------
-- Try to upload 2014 cust cnt data and perform the join to see how many utilies match up and how many do not have counts
-- Check 2014 utility data:
drop table if exists eia.eia_861_2014_util_sales_cust;
create table eia.eia_861_2014_util_sales_cust (
	data_year	int,
	utility_number	int,
	utility_name	text,
	part	text,
	service_type	text,
	data_type	text,
	state	text,
	ownership	text,
	ba_code	text,
	res_revenues_thousands_dlrs	text,
	res_sales_mwh	text,
	res_customers	text,
	com_revenues_thousands_dlrs	text,
	com_sales_mwh	text,
	com_customers	text,
	ind_revenues_thousands_dlrs	text,
	ind_sales_mwh	text,
	ind_customers	text,
	trans_revenues_thousands_dlrs	text,
	trans_sales_mwh	text,
	trans_customers	text,
	total_revenues_thousands_dlrs	text,
	total_sales_mwh	text,
	total_customers text
);

\COPY eia.eia_861_2014_util_sales_cust from '/Users/mmooney/Dropbox (NREL GIS Team)/Projects/2016_10_03_dStorage/data/source/eia/eia_Sales_Ult_Cust_2014.csv' with csv header;

-- Perform updates
update eia.eia_861_2014_util_sales_cust
	set res_customers = null where res_customers = '.';
update eia.eia_861_2014_util_sales_cust
	set res_customers = replace (res_customers, ',', ''),
	ind_customers = replace(ind_customers, ',', ''),
	com_customers = replace(com_customers, ',', '');
update eia.eia_861_2014_util_sales_cust
	set res_customers = case when res_customers is null then 0 else res_customers end,
		ind_customers = case when ind_customers is null then 0 else ind_customers end,
		com_customers = case when com_customers is null then 0 else com_customers end;

-- Make sure all eia_ids are accounted for
	with b as (select distinct eia_id from diffusion_data_shared.urdb_rates_attrs_lkup_20161005)
	select a.utility_number, b.eia_id 
	from eia.eia_861_2014_util_sales_cust a
	right join b
	on cast(a.utility_number as text) = b.eia_id
	order by a.utility_number desc
	-- 310 utilities do not have matches

-- Of the utilities NOT missing matches, how many have customer counts?
	with distinct_eia_id as (
		select distinct eia_id 
		from diffusion_data_shared.urdb_rates_attrs_lkup_20161005),
	joins as (
		select a.utility_number, b.eia_id, cast(a.res_customers as int), cast(a.com_customers as int), cast(a.ind_customers as int)
		from eia.eia_861_2014_util_sales_cust a
		right join distinct_eia_id b
		on cast(a.utility_number as text) = b.eia_id
	order by a.utility_number desc)
	select * from a
	where (res_customers is null or res_customers = 0)
	and (com_customers is null or com_customers = 0)
	and (ind_customers is null or ind_customers = 0)
	--and utility_number is null
		-- 312 total missing matches
		-- only 2 utilies with matches have NO customer count data ("40437", and "27000")
		-- OF the utilities NOT missing matches, All except 2 have customer counts


	-- 312 distinct utility ids is better tha 810, but lets see if we can use both to refine the results and lower the number...

---------------------------------------------------------
-- Try Use 2014 and 2013 Combo
--------------------------------------------------------
-- Merge both the old table with the new 2014 table to see if we can lower the 310 number
	with distinct_eia_id as (
		select distinct eia_id, state_abbr 
		from diffusion_data_shared.urdb_rates_attrs_lkup_20161005
		),
	joins as (
		select a.utility_number, b.eia_id, cast(a.res_customers as int), cast(a.com_customers as int), cast(a.ind_customers as int)
		from eia.eia_861_2014_util_sales_cust a
		right join distinct_eia_id b
		on cast(a.utility_number as text) = b.eia_id --and a.state = b.state_abbr
		order by a.utility_number desc
		),
	number_of_sectors as (
		select *, 
		(case when (res_customers is null or res_customers = 0) then 0 else 1 end) as cnt_res,
		(case when (ind_customers is null or ind_customers = 0) then 0 else 1 end) as cnt_ind,
		(case when (com_customers is null or com_customers = 0) then 0 else 1 end) as cnt_com
		from joins)
	select distinct * from number_of_sectors where cnt_res = 0 and cnt_ind= 0 and cnt_com = 0
	--312 rows returned



-- Merge both the old table with the new 2014 table to see if we can lower the 310 number
	with distinct_eia_id as (
		select distinct eia_id, state_abbr 
		from diffusion_data_shared.urdb_rates_attrs_lkup_20161005
		),
	joins as (
		select a.utility_number, b.eia_id, cast(a.res_customers as int), cast(a.com_customers as int), cast(a.ind_customers as int)
		from eia.eia_861_2014_util_sales_cust a
		right join distinct_eia_id b
		on cast(a.utility_number as text) = b.eia_id --and a.state = b.state_abbr
		order by a.utility_number desc
		),
	number_of_sectors as (
		select *, 
		(case when (res_customers is null or res_customers = 0) then 0 else 1 end) as cnt_res,
		(case when (ind_customers is null or ind_customers = 0) then 0 else 1 end) as cnt_ind,
		(case when (com_customers is null or com_customers = 0) then 0 else 1 end) as cnt_com
		from joins),
	missing_eia_id as (
		select distinct * from number_of_sectors where cnt_res = 0 and cnt_ind= 0 and cnt_com = 0) --312 missing
	-- join the missing ones to the 2013 data
	select distinct a.eia_id, b.utility_num
	from missing_eia_id a
	left join eia.eia_861_2013_county_utility_rates b
	on cast(b.utility_num as text) = a.eia_id
	where b.res_customers is null and b.comm_customers is null and b.ind_customers is null
	order by utility_num desc
	-- Results:
		-- We brought the number down from 312 to 308 by joining the two tables (2013 & 2014)
		-- Not a big gain by combining the two, but a gain none the less
		-- because these numbers are slightly different in the 2013
			-- (they are for the ENTIRE utility across state lines), 
			-- AND because it isn't a big gain, we aren't going to include them

-- 312 utilities do not have matches

---------------------------------------------------------
-- Try Use 2014 and 2013 Combo & Compare these with 2011
--------------------------------------------------------
-- Compare these with the 2011 Data

-- 1. create view with utility regions that are missing customer counts
drop view if exists diffusion_data_shared.utils_missing_customer_counts_pt1_20161005 cascade;
create view diffusion_data_shared.utils_missing_customer_counts_pt1_20161005 as (
		with distinct_eia_id as (
			select distinct eia_id, state_abbr
			from diffusion_data_shared.urdb_rates_attrs_lkup_20161005),
		joins as (
			select a.utility_number, b.eia_id, b.state_abbr, cast(a.res_customers as int), cast(a.com_customers as int), cast(a.ind_customers as int)
			from eia.eia_861_2014_util_sales_cust a
			right join distinct_eia_id b
			on cast(a.utility_number as text) = b.eia_id and a.state = b.state_abbr
			order by a.utility_number desc),
		missing_cust_cnts as (
			select utility_number, eia_id, state_abbr from joins
			where (res_customers is null or res_customers = 0)
			and (com_customers is null or com_customers = 0)
			and (ind_customers is null or ind_customers = 0)
			order by state_abbr
			),
		not_missing_cust_cnts as (
			select utility_number, eia_id, state_abbr, res_customers, com_customers, ind_customers
			from joins
			where (res_customers is not null or res_customers != 0)
			and (com_customers is not null or com_customers != 0)
			and (ind_customers is not null or ind_customers != 0)
			order by state_abbr),
		distinct_gid as (
			select distinct a.util_reg_gid, a.eia_id, a.state_abbr, b.the_geom_96703 
			from diffusion_data_shared.urdb_rates_attrs_lkup_20161005 a
			left join diffusion_data_shared.urdb_rates_geoms_20161005 b
			on a.util_reg_gid = b.util_reg_gid),
		util_sector as (
			select distinct util_reg_gid, sector from  diffusion_data_shared.urdb_rates_attrs_lkup_20161005),
		gids_missing_cust_cnts as (
			select a.util_reg_gid, a.eia_id, a.state_abbr, a.the_geom_96703
			from distinct_gid a
			right join missing_cust_cnts b
			on a.eia_id = b.eia_id and a.state_abbr = b.state_abbr)
		select a.util_reg_gid, a.eia_id, a.state_abbr, --b.service_type
			a.the_geom_96703 
			from gids_missing_cust_cnts a
			left join eia.eia_861_2014_util_sales_cust b 
			on
				a.eia_id = cast(b.utility_number as text) and a.state_abbr = b.state);

-- check to see if these match up with the 2011 customer counts
	drop view if exists diffusion_data_shared.utils_with_customer_counts_20161005;
	create view diffusion_data_shared.utils_with_customer_counts_20161005 as (
		--get distinct eia_id for joining
		with distinct_eia_id as (
			select distinct eia_id, state_abbr
			from diffusion_data_shared.urdb_rates_attrs_lkup_20161005),
		-- join distinct_eia_id with 2014 customer data
		joins as (
			select a.utility_number, b.eia_id, b.state_abbr, 
				-- Add the customer counts by "Service Type" together
				sum(cast(a.res_customers as int)) as res_customers, 
				sum(cast(a.com_customers as int)) as com_customers, 
				sum(cast(a.ind_customers as int)) as ind_customers
			from eia.eia_861_2014_util_sales_cust a
			right join distinct_eia_id b
			on cast(a.utility_number as text) = b.eia_id and a.state = b.state_abbr
			group by a.utility_number, b.eia_id, b.state_abbr
			order by a.utility_number desc),
		-- identify which ids are not missing counts
		not_missing_cust_cnts as (
			select utility_number, eia_id, state_abbr, res_customers, com_customers, ind_customers
			from joins
			where (res_customers is not null or res_customers != 0)
			and (com_customers is not null or com_customers != 0)
			and (ind_customers is not null or ind_customers != 0)
			order by state_abbr),
		-- get distinct utility_reg_gid (+ attributes)
		distinct_gid as (
			select distinct a.util_reg_gid, a.eia_id, a.state_abbr, b.the_geom_96703 
			from diffusion_data_shared.urdb_rates_attrs_lkup_20161005 a
			left join diffusion_data_shared.urdb_rates_geoms_20161005 b
			on a.util_reg_gid = b.util_reg_gid),
		--util_sector as (
			--select distinct util_reg_gid, sector from diffusion_data_shared.urdb_rates_attrs_lkup_20161005),
		gids_not_missing_cust_cnts as (
			with part1 as (
				select distinct
					a.util_reg_gid, 
					a.eia_id, 
					a.state_abbr, 
					sum(b.res_customers) as res_customers, 
					sum(b.com_customers) as com_customers, 
					sum(b.ind_customers) as ind_customers, 
					a.the_geom_96703
			from distinct_gid a
			right join not_missing_cust_cnts b
			on a.eia_id = b.eia_id and a.state_abbr = b.state_abbr
			group by a.util_reg_gid, a.eia_id, a.state_abbr, a.the_geom_96703)
			select a.*--, b.service_type			
			from part1 a
			left join eia.eia_861_2014_util_sales_cust b 
			on
				a.eia_id = cast(b.utility_number as text) and a.state_abbr = b.state
			),
		-- Join with 2011 data (2011 data aligned with the missing_customer_counts table)
		file2 as (
			with part1 as (
				select distinct
					b.eia_id,
					a.state_code as state_abbr, 
					sum(a.residential_consumers) as res_customers, 
					sum(a.commercial_consumers) as com_customers,
					sum(a.industrial_consumers) as ind_customers
				from eia.eia_861_file_2_2011 a
				left join diffusion_data_shared.utils_missing_customer_counts_pt1_20161005 b
				on cast(a.utility_id as text)= b.eia_id and a.state_code = b.state_abbr
				where b.eia_id is not null
				group by b.eia_id, a.state_code
				),
			part2 as (
				select a.util_reg_gid, a.eia_id, a.state_abbr, b.res_customers, b.com_customers, b.ind_customers, a.the_geom_96703
				from distinct_gid a
				right join part1 b
				on a.eia_id = b.eia_id and a.state_abbr = b.state_abbr)
			select a.*--, b.service_type
			from part2 a
			left join eia.eia_861_file_2_2011 b
			on a.eia_id = cast(b.utility_id as text) and a.state_abbr = b.state_code)
-- 		file1 as (
-- 			with part1 as (
-- 				select distinct 
-- 					cast(utility_id as text) as eia_id, 
-- 					state_code as state_abbr,
-- 					residential_consumers as res_customers, 
-- 					commercial_consumers as com_customers,
-- 					industrial_consumers as ind_customers
-- 				from eia.eia_861_file_2_2011
-- 				where cast(utility_id as text) in
-- 					('25251', '3278', '40300', '26751', '25251', '40300', '3292', '40051', '26751', '26751', '40300', '25251', '13214', '25251', '26751', '40300', '14268', 'No eia id given', '25251', '25251', '26751', '27000', '6198', 'No eia id given', '14127', '3292', '5027', '17267', '26751', '12341', '20111', '3292', '4062', '942', '40299', '17184', '26751', '4062', 'No eia id given', '27000', 'No eia id given', '40300', 'No eia id given', '11235', '5553', '44372', '4062', '40382', '27000')
-- 				),
-- 			part2 as (
-- 				select a.util_reg_gid, a.eia_id, a.state_abbr, b.res_customers, b.com_customers, b.ind_customers, a.the_geom_96703
-- 				from distinct_gid a
-- 				right join part1 b
-- 				on a.eia_id = b.eia_id and a.state_abbr = b.state_abbr)
-- 			select a.*, b.service_type
-- 			from part2 a
-- 			left join eia.eia_861_file_1_2011 b
-- 			on a.eia_id = cast(b.utility_id as text) and a.state_abbr = b.state_code)
		select * from gids_not_missing_cust_cnts
		union all
		select * from file2
		--union all select * from file1
		);
	
-- remove utilities we found data for from the "missing" view	
	drop view if exists diffusion_data_shared.utils_missing_customer_counts_pt2_20161005;
	create view diffusion_data_shared.utils_missing_customer_counts_pt2_20161005 as (
		select 
			a.*, 
			b.eia_id as eia_id2, 
			res_customers, 
			com_customers, 
			ind_customers 
		from diffusion_data_shared.utils_missing_customer_counts_pt1_20161005 a
		left join diffusion_data_shared.utils_with_customer_counts_20161005 b
		on a.eia_id = b.eia_id
		where 
			b.eia_id is null
			and ((res_customers is null or res_customers = 0) 
				and (com_customers is null or com_customers = 0) 
				and (ind_customers is null or ind_customers = 0)));

-- count to compare
	select count(a.*) from diffusion_data_shared.utils_missing_customer_counts_pt1_20161005 a
	-- 355 missing
	select count(b.*) from diffusion_data_shared.utils_missing_customer_counts_pt2_20161005 b
	-- 31 missing total (state and utility geom)
	select count(distinct b.eia_id) from diffusion_data_shared.utils_missing_customer_counts_pt2_20161005 b
	-- 10 missing (utility_id only)	-- 448 missing????
--------------------------------------------
-- make final tables to speed things up later on
--------------------------------------------
	drop table if exists diffusion_data_shared.temp_utils_missing_customer_counts_20161005;
	create table diffusion_data_shared.temp_utils_missing_customer_counts_20161005 as (
		with u as (
			select distinct util_reg_gid, eia_id, sector, state_abbr, utility_type
			from diffusion_data_shared.urdb_rates_attrs_lkup_20161005)
		select a.util_reg_gid, a.eia_id, a.state_abbr, b.sector, b.utility_type, a.the_geom_96703
		from diffusion_data_shared.utils_missing_customer_counts_pt2_20161005 a
		left join u b
		on a.util_reg_gid = b.util_reg_gid
		order by a.util_reg_gid, a.state_abbr, b.sector, b.utility_type);

	-- B. Customer Count Table
	drop table if exists diffusion_data_shared.temp_utils_with_customer_counts_20161005;
	create table diffusion_data_shared.temp_utils_with_customer_counts_20161005 as ( 
		with u1 as (
			select distinct util_reg_gid, eia_id, sector, state_abbr, utility_type
			from diffusion_data_shared.urdb_rates_attrs_lkup_20161005),
		u2 as (
			select distinct * from diffusion_data_shared.utils_with_customer_counts_20161005),
		res as (
			select 
				a.util_reg_gid,
				a.eia_id, --a.service_type,
		 		a.state_abbr, 
		 		'R'::text as sector, 
		 		b.utility_type, 
		 		a.res_customers as cust_cnt,
		 		a.the_geom_96703
			from u2 a
			left join u1 b
			on a.util_reg_gid = b.util_reg_gid
			where b.sector = 'R'
			),
		com as (
			select 
				a.util_reg_gid,
				a.eia_id, --a.service_type,
		 		a.state_abbr, 
		 		'C'::text as sector, 
		 		b.utility_type, 
		 		a.com_customers as cust_cnt,
		 		a.the_geom_96703
			from u2 a
			left join u1 b
			on a.util_reg_gid = b.util_reg_gid
			where b.sector = 'C'
			),
		ind as (
			select 
				a.util_reg_gid,
				a.eia_id, --a.service_type,
		 		a.state_abbr, 
		 		'I'::text as sector, 
		 		b.utility_type, 
		 		a.ind_customers as cust_cnt,
		 		a.the_geom_96703
			from u2 a
			left join u1 b
			on a.util_reg_gid = b.util_reg_gid
			where b.sector = 'I'
			)
		select * from res 
		union all
		select * from com 
		union all
		select * from ind
		order by util_reg_gid, state_abbr, sector, utility_type);

-------------------
-- Clean Up
-------------------
-- drop temp views 
	drop view if exists diffusion_data_shared.utils_missing_customer_counts_pt1_20161005 cascade;
-- Update null values
	

-----------------
-- Sanity Checks
-----------------
	select count(distinct eia_id) from diffusion_data_shared.temp_utils_with_customer_counts_20161005;
	--1113 + 10 missing = 1123 which the total!
	select count(*) from diffusion_data_shared.temp_utils_missing_customer_counts_20161005 where state_abbr = 'CA';
	-- 0 = there are no missing utility cnts from CA (GOod!!)
	--------------------------------
	-- ** check service types **
	--------------------------------
		-- Note: I need to uncomment out the "service_type" text when creating the views for code to run successfully
--		with a as (
--			with a as (
--				select distinct eia_id 
--				from diffusion_data_shared.temp_utils_with_customer_counts_20161005),
--			d as (
--				select distinct eia_id, (service_type = 'Delivery')::boolean as delivery from diffusion_data_shared.temp_utils_with_customer_counts_20161005 where (service_type = 'Delivery') = true),
--			b as (
--				select distinct eia_id, (service_type = 'Bundled' or service_type = 'Bundle')::boolean as bundled 
--				from diffusion_data_shared.temp_utils_with_customer_counts_20161005 where (service_type = 'Bundled' or service_type = 'Bundle') = true),
--			e as (select distinct eia_id, (service_type = 'Energy')::boolean as energy from diffusion_data_shared.temp_utils_with_customer_counts_20161005 where (service_type = 'Energy') = True)
--			select a.eia_id, d.delivery, b.bundled, e.energy
--			from a
--			left join d on d.eia_id = a.eia_id
--			left join b on b.eia_id = a.eia_id
--			left join e on e.eia_id = a.eia_id
--			),
--		b as (
--		select eia_id, count(case when delivery is not null then delivery end) as delivery_count, count(case when bundled is not null then bundled end) as bundle_count, count(case when energy is not null then energy end) as energy_count
--		from a
--		group by eia_id)
--		--select * from b where energy_count > 0 -- 1 row: "12341" (1
--		--select * from b where delivery_count > 0 -- 49 rows; 45 of which are delivery AND bundle
--		select * from b where delivery_count > 0 and bundle_count = 0 -- 4 rows ("5609", "8883", "11522", "1179")






-- Action Plan:
	-- Need to make a nearest neighbor for utilities missing customer counts