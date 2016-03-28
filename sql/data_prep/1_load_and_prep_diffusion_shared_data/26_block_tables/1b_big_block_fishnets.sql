set role 'diffusion-writers';

------------------------------------------------------------------------------------------------
-- create the table
DROP TABLE IF EXISTS diffusion_blocks.big_block_fishnets;
CREATE TABLE diffusion_blocks.big_block_fishnets AS
with a as
(
	select a.pgid, a.the_poly_96703,
		ST_Centroid(ST_Fishnet(a.the_poly_96703,201.168)) as the_point_96703
	from diffusion_blocks.block_geoms a
	where a.exceeds_10_acres = true
)
select a.pgid, a.the_point_96703
FROM a
WHERE ST_Intersects(a.the_poly_96703, a.the_point_96703);

------------------------------------------------------------------------------------------------
-- add primary key
ALTER TABLE diffusion_blocks.big_block_fishnets
ADD PRIMARY KEY (pgid);

------------------------------------------------------------------------------------------------
-- add index
CREATE INDEX big_block_fishnets_gist_the_point_96703
ON diffusion_blocks.big_block_fishnets
USING GIST(the_point_96703);

------------------------------------------------------------------------------------------------
-- vacuum
vacuum analyze diffusion_blocks.big_block_fishnets;

------------------------------------------------------------------------------------------------
-- QA/QC

-- check all big blocks are represented
SELECT count(*)
FROM diffusion_blocks.block_geoms a
LEFT JOIN diffusion_blocks.big_block_fishnets b
ON a.pgid = b.pgid
where a.exceeds_10_acres = true
and b.pgid is null;