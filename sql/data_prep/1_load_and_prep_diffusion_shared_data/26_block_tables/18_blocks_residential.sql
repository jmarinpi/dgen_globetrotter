SET ROLE 'diffusion-writers';

------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS diffusion_blocks.block_res;
CREATE TABLE  diffusion_blocks.blocks_res AS
select pgid
from diffusion_blocks.block_housing_units
where housing_units > 0;
-- 6379963 rows
------------------------------------------------------------------------------------------

-- add primary key
ALTER TABLE diffusion_blocks.blocks_res
ADD PRIMARY KEY (pgid);

