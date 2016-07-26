set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_geo.crb_building_names_and_sizes;
CREATE TABLE diffusion_geo.crb_building_names_and_sizes
(
	building_type text,
	crb_model text,
	tot_sqft numeric
);

\COPY diffusion_geo.crb_building_names_and_sizes FROM '/Users/mgleason/NREL_Projects/github/diffusion/sql/data_prep/4_load_misc_technology_specific_tables/3_ghp/2_ghp_simulations_data_cleanup_and_loading/helper/crb_bldg_names_and_sizes.csv' with csv header;


-- add this info to the main table
ALTER TABLE diffusion_geo.ghp_simulations_com
ADD COLUMN tot_sqft numeric,
add column crb_model text;

UPDATE diffusion_geo.ghp_simulations_com a 
set (tot_sqft, crb_model) = (b.tot_sqft, b.crb_model)
from diffusion_geo.crb_building_names_and_sizes b
where a.building_type = b.building_type;

-- check result
select distinct building_type, crb_model, tot_sqft
from diffusion_geo.ghp_simulations_com;
-- looks good