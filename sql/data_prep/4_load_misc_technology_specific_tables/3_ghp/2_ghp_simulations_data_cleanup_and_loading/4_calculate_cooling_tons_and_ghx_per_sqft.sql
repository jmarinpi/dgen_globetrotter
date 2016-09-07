set role 'diffusion-writers';

-- add cooling_ton_per_sqft and ghx_length_ft_per_cooling_ton cols
ALTER TABLE  diffusion_geo.ghp_simulations_com
ADD COLUMN cooling_ton_per_sqft numeric,
ADD COLUMN ghx_length_ft_per_cooling_ton numeric;

UPDATE diffusion_geo.ghp_simulations_com
set cooling_ton_per_sqft = crb_cooling_capacity_ton/crb_totsqft;
-- 169 rows

UPDATE diffusion_geo.ghp_simulations_com
set ghx_length_ft_per_cooling_ton = crb_ghx_length_ft/crb_cooling_capacity_ton;
-- 169 rows

-- check for nulls
select *
FROM diffusion_geo.ghp_simulations_com
where ghx_length_ft_per_cooling_ton is null
or cooling_ton_per_sqft is null;