-- we know that we are missing 31

-- Perform Nearest Neighbor & create final table
-- use the sum of all customer counts for each "service type"
drop table if exists diffusion_data_shared.temp_utils_with_customer_counts_sum_20161005;
create table diffusion_data_shared.temp_utils_with_customer_counts_sum_20161005 as (
	select 
		util_reg_gid,
		eia_id,
		state_abbr,
		sector,
		utility_type,
		sum(res_customers) as res_customers,
		sum(com_customers) as com_customers,
		sum(ind_customers) as ind_customers,
		--the_geom_96703
	from diffusion_data_shared.temp_utils_with_customer_counts_20161005
	group by
		util_reg_gid,
		eia_id,
		state_abbr,
		sector,
		utility_type,
		--the_geom_96703
	);

diffusion_data_shared.temp_utils_missing_customer_counts_20161005

-- Join all together -- join all customer counts together