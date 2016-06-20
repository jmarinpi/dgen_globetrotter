
DROP TABLE IF EXISTS mgleason.tract_block_sample;
CREATE TABLE mgleason.tract_block_sample AS
with a as
(
	select tract_id_alias, bldg_count_com,
		r_array_max(array[round(bldg_count_com * 0.05,0)::INTEGER, 5])::INTEGER as sample_size
	from diffusion_blocks.tract_building_count_by_sector
	where tract_id_alias in (1,2,3,4,5)
),
b as
(
	select tract_id_alias,
		array_agg(pgid ORDER BY pgid) as pgids, 
		array_agg(bldg_count_com ORDER BY pgid) as weights
	from diffusion_blocks.block_microdata_com
	GROUP BY tract_id_alias
)
select a.tract_id_alias, bldg_count_com as tract_bldg_count,
	unnest(diffusion_shared.sample(b.pgids, 
				a.sample_size,
				1, -- seed
				True,
				weights)) as pgid
FROM a
LEFT JOIN b
ON a.tract_id_alias = b.tract_id_alias;


-- add a unique id
ALTER TABLE mgleason.tract_block_sample
ADD COLUMN uid serial primary key;
-- need these to ensure randomness in next step

DROP TABLE IF EXISTS mgleason.tract_block_sample_w_bldg_type;
CREATE TABLE mgleason.tract_block_sample_w_bldg_type AS
select b.*, a.uid, a.tract_bldg_count,
	unnest(diffusion_shared.sample(c.bldg_types, 
				1,
				2 * a.uid, -- seed
				True,
				b.bldg_probs_com)) as bldg_type,
	unnest(diffusion_shared.sample(b.bldg_probs_com, 
				1,
				2 * a.uid, -- seed
				True,
				b.bldg_probs_com)) as bldg_count
from mgleason.tract_block_sample a
LEFT JOIN diffusion_blocks.block_microdata_com b
ON a.pgid = b.pgid
lEFT JOIN diffusion_blocks.bldg_type_arrays c
ON c.sector_abbr = 'com';

-- left join the posssible pba plus
DROP TABLE IF EXISTS mgleason.tract_block_eia_sample;
CREATE TABLE  mgleason.tract_block_eia_sample AS
with a as
(
	select a.uid, a.census_division_abbr, b.pbaplus
	FROM mgleason.tract_block_sample_w_bldg_type a
	LEFT JOIN diffusion_shared.cdms_bldg_types_to_pba_plus_lkup b
		ON a.bldg_type = b.cdms
),
b as
(
	select a.*, b.building_id, b.sample_wt as bldg_sample_wt
	from a
	lEFT JOIN diffusion_shared.eia_microdata_cbecs_2003_expanded b
	ON a.census_division_abbr = b.census_division_abbr
	ANd a.pbaplus = b.pbaplus
	where b.sample_wt is not null -- this should be removed -- it's just for debugging
),
c as
(
	select b.uid, 
		    unnest(diffusion_shared.sample(array_agg(building_id ORDER BY building_id), 
						   1, 
						   1 * b.uid, 
						   True, 
						   array_agg(bldg_sample_wt::NUMERIC ORDER BY building_id))
						   ) as building_id
	from b
	GROUP BY b.uid
)
select *
FROM c;

-- combine -- and then move on to appending other attributes?
-- DROP TABLE IF EXISTS mgleason.combined_sample;
-- CREATE TABLE  mgleason.combined_sample AS
select c.*, b.*, (c.bldg_count::NUMERIC/sum(c.bldg_count) OVER (PARTITION BY tract_id_alias)) * c.tract_bldg_count as n_bldgs
from mgleason.tract_block_eia_sample a
LEFT JOIN diffusion_shared.eia_microdata_cbecs_2003_expanded b
ON a.building_id = b.building_id
LEFT JOIN mgleason.tract_block_sample_w_bldg_type c
ON a.uid = c.uid;
-- recalculate the population


-- issues to address:
	-- n bldgs is fractional -- does this matter?
	
	-- how to do this for residential 
		-- how to incorporate baseline heating system type frequencies
		-- how to deal with multiple occupancy and/or non-owner occupied bldgs?
	-- need to ensure total # of buildings and total thermal load sum to known totals at larger regional levels (county, census division ,etc.
	-- waht is the role of owner occupied buildings in commercial?
	-- what to do for industrial?
	-- how to add new builds?
	-- issue to fix:
		-- cbecs pba plus x census_division-abbr combos are missing (pbaplus = 20, census_division_abbr = MTN) -- fix by either switching to a different region or generalizing to pba code
-- additional attributes to add:
-- immutable
	-- system age simulated
	-- capital cost multipliers (not for agents -- for plants)
	-- map to a baseline system type (for costs)
-- mutable
	-- system expected lifetime parameters
	-- system lifetime expired in any given year?
	-- local or regional cost of fuel
