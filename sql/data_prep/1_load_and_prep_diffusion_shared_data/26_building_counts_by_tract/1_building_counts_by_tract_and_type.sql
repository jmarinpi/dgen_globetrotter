set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_blocks.bldg_counts_by_type_res_temp;
CREATE TABLE diffusion_blocks.bldg_counts_by_type_res_temp AS
with a as
(
	select  c.tract_id_alias,
		unnest(diffusion_shared.r_array_extract(a.bldg_probs_res, b.bldg_types)) as bldg_type,
		unnest(diffusion_shared.r_array_extract(a.bldg_probs_res, a.bldg_probs_res)) as bldg_count
	from diffusion_blocks.block_bldg_types a
	LEFT JOIN diffusion_blocks.bldg_type_arrays b
			on b.sector_abbr = 'res'
	LEFT JOIN diffusion_blocks.block_tract_id_alias c
		on a.pgid = c.pgid
)
select tract_id_alias, bldg_type,
	sum(bldg_count) as bldg_count
FROM a
GROUP BY tract_id_alias, bldg_type;
-- 364683 rows

DROP TABLE IF EXISTS diffusion_blocks.bldg_counts_by_type_com_temp;
CREATE TABLE diffusion_blocks.bldg_counts_by_type_com_temp AS
with a as
(
	select  c.tract_id_alias,
		unnest(diffusion_shared.r_array_extract(a.bldg_probs_com, b.bldg_types)) as bldg_type,
		unnest(diffusion_shared.r_array_extract(a.bldg_probs_com, a.bldg_probs_com)) as bldg_count
	from diffusion_blocks.block_bldg_types a
	LEFT JOIN diffusion_blocks.bldg_type_arrays b
			on b.sector_abbr = 'com'
	LEFT JOIN diffusion_blocks.block_tract_id_alias c
		on a.pgid = c.pgid
)
select tract_id_alias, bldg_type,
	sum(bldg_count) as bldg_count
FROM a
GROUP BY tract_id_alias, bldg_type;
-- 244901 rows

DROP TABLE IF EXISTS diffusion_blocks.bldg_counts_by_type_ind_temp;
CREATE TABLE diffusion_blocks.bldg_counts_by_type_ind_temp AS
with a as
(
	select  c.tract_id_alias,
		unnest(diffusion_shared.r_array_extract(a.bldg_probs_ind, b.bldg_types)) as bldg_type,
		unnest(diffusion_shared.r_array_extract(a.bldg_probs_ind, a.bldg_probs_ind)) as bldg_count
	from diffusion_blocks.block_bldg_types a
	LEFT JOIN diffusion_blocks.bldg_type_arrays b
			on b.sector_abbr = 'ind'
	LEFT JOIN diffusion_blocks.block_tract_id_alias c
		on a.pgid = c.pgid
)
select tract_id_alias, bldg_type,
	sum(bldg_count) as bldg_count
FROM a
GROUP BY tract_id_alias, bldg_type;
-- 792419 rows

-- adjust the counts for commercial
DROP TABLE IF EXISTS diffusion_blocks.bldg_counts_by_type_com_temp_adjust;
CREATE TABLE diffusion_blocks.bldg_counts_by_type_com_temp_adjust as
select a.tract_id_alias, 
	a.bldg_type,
	a.bldg_count::NUMERIC/b.bldg_count_com * c.bldg_count_com as bldg_count
from diffusion_blocks.bldg_counts_by_type_com_temp a
lEFT JOIN diffusion_blocks.tract_building_count_by_sector_temp b
	on a.tract_id_alias = b.tract_id_alias
lEFT JOIN diffusion_blocks.tract_building_count_by_sector c
	on a.tract_id_alias = b.tract_id_alias;
-- ** running on screen now

-- RENAME THE TABLES 
ALTER TABLE diffusion_blocks.bldg_counts_by_type_res_temp
RENAME tract_building_count_by_type_res;

ALTER TABLE diffusion_blocks.bldg_counts_by_type_com_temp_adjust
RENAME tract_building_count_by_type_com;

ALTER TABLE diffusion_blocks.bldg_counts_by_type_ind_temp
RENAME tract_building_count_by_type_ind;

-- add primary keys
-- count distinct tract ids
-- check commercial building count vs census division total from cbecs

-- cleanup intermediate tables
-- DROP TABLE IF EXISTS diffusion_blocks.bldg_counts_by_type_com_temp;
-- DROP TABLE IF EXISTS diffusion_blocks.tract_building_count_by_sector_temp;