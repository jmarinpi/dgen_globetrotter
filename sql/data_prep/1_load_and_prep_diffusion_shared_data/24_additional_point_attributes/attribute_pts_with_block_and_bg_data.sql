---------------------------------------------------------------------------
select count(*)
from diffusion_shared.pt_grid_us_res a
LEFT JOIN diffusion_data_wind.pt_grid_us_res_new_acs_2012_blockgroup_lkup b
ON a.gid = b.gid
where b.gid is null;
-- 0  missing -- all set

select count(*)
from diffusion_shared.pt_grid_us_res a
LEFT JOIN diffusion_data_wind.pt_grid_us_res_new_census_2010_block_lkup b
ON a.gid = b.gid
where b.gid is null;
-- 0 missing - all set
---------------------------------------------------------------------------
-- 
