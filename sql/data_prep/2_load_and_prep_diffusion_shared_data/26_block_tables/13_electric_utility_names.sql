set role 'diffusion-writers';
------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS diffusion_blocks.electric_utility_names;
CREATE TABLE diffusion_blocks.electric_utility_names AS
SELECT distinct utility_num, utility_name
FROM eia.eia_861_2013_county_utility_rates;
-- 2395

--------------------------------------------------------------------------------------------------
-- QA/QC

-- add primary key
ALTER TABLE diffusion_blocks.electric_utility_names
ADD PRIMARY KEY (utility_num);

-- check for nulls
select count(*)
FROM diffusion_blocks.electric_utility_names
where utility_name is null;
-- 0 -- all set