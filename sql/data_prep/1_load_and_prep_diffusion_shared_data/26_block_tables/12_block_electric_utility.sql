set role 'diffusion-writers';

------------------------------------------------------------------------------------------------
-- create the table
DROP TABLE IF EXISTS diffusion_blocks.block_electric_utility;
CREATE TABLE diffusion_blocks.block_electric_utility AS
with b as
(
	select statefp as state_fips, countyfp as county_fips, 
		array_agg(utility_num order by utility_num) as utility_nums
	from eia.eia_861_2013_county_utility_rates
	GROUP BY statefp, countyfp
)
select a.pgid, b.utility_nums
from diffusion_blocks.block_geoms a
LEFT JOIN b
on a.state_fips = b.state_fips
and a.county_fips = b.county_fips;
-- 10535171 rows

-------------------------------------------------------------------------------------------------
-- QA/QC

-- add primary key
ALTER TABLE diffusion_blocks.block_electric_utility
ADD PRIMARY KEY (pgid);

-- check count
select count(*)
FROM diffusion_blocks.block_electric_utility;
-- 10535171

-- how many nulls?
select count(*)
FROM diffusion_blocks.block_electric_utility
where utility_nums is null;
-- 287

-- where are they?
select distinct b.state_abbr, b.county_fips
FROM diffusion_blocks.block_electric_utility a
left join diffusion_blocks.block_geoms b
ON a.pgid = b.pgid
where a.utility_nums is null;
-- all are in AK, county_fips = 105

select *
FROM diffusion_blocks.county_geoms
where state_abbr = 'AK'
and county_fips = '105';
-- Hoonah-ANgoon

-- according to the EIA-861 2013 Service Territory FOrm
-- Skagway Hoonah ANgoon is part of 6 utility numbers

-- do these utilities exist in the eia table with the same utility_nums??
select *
FROM diffusion_blocks.electric_utility_names
where utility_num in 
(
	219, 
	4329, 
	7822, 
	18541, 
	18963, 
	29297
);
-- yes -- all exist 

-- so, fix the nulls by filling with this array:
UPDATE diffusion_blocks.block_electric_utility
set utility_nums = ARRAY[
		219, 
		4329, 
		7822, 
		18541, 
		18963, 
		29297
]
where utility_nums is null;
-- 287 rows fixed

-- any nulls remain?
select count(*)
FROM diffusion_blocks.block_electric_utility
where utility_nums is null;
-- 0 -- all set
-------------------------------------------------------------------------------------------------
