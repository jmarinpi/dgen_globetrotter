set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_geo.crb_building_sizes;
CREATE TABLE diffusion_geo.crb_building_sizes
(
	building_type text,
	tot_sqft numeric
);

INSERT INTO diffusion_geo.crb_building_sizes
SELECT unnest(array['Medium Office', 'Small Hotel', 'School']), unnest(array[53620, 40095, 210954]);



-- add this info to the main table
ALTER TABLE diffusion_geo.ghp_simulations_com
ADD COLUMN tot_sqft numeric;

UPDATE diffusion_geo.ghp_simulations_com a 
set tot_sqft = b.tot_sqft
from diffusion_geo.crb_building_sizes b
where a.building_type = b.building_type;

-- check result
select distinct building_type, tot_sqft
from diffusion_geo.ghp_simulations_com;
-- looks good