set role 'diffusion-writers';

-- check for missing combos 
with a as
(
	select distinct typehuq
	from diffusion_shared.eia_microdata_recs_2009_expanded_bldgs
),
b as
(
	select distinct reportable_domain
	from diffusion_shared.eia_microdata_recs_2009_expanded_bldgs
),
c as
(
	select a.typehuq, b.reportable_domain
	from a
	cross join b
),
d as
(
	select distinct reportable_domain, typehuq
	from diffusion_shared.eia_microdata_recs_2009_expanded_bldgs
)
select c.reportable_domain, c.typehuq
from c
left join d
ON c.typehuq = d.typehuq
and c.reportable_domain = d.reportable_domain
where d.typehuq is null;
-- only one missing reportable_domain/type_huq combo:
-- 6,1 -- Illinois and mobile home

-- solution:
-- replace with data from Indiana/Ohio (rd= 7)and Mobile Home
-- need to use new building ids
select max(building_id)
FROM diffusion_shared.eia_microdata_recs_2009_expanded_bldgs;
-- 12083

DROP SEQUENCE IF EXISTS diffusion_shared.eia_microdata_recs_2009_expanded_bldgs_bldg_ids;
CREATE SEQUENCE diffusion_shared.eia_microdata_recs_2009_expanded_bldgs_bldg_ids
INCREMENT 1
START 12084;

INSERT INTO diffusion_shared.eia_microdata_recs_2009_expanded_bldgs
select nextval('diffusion_shared.eia_microdata_recs_2009_expanded_bldgs_bldg_ids') as building_id, 
	sample_wt, census_region, census_division_abbr, 
       6 as reportable_domain, climate_zone, pba, pbaplus, typehuq, roof_material, 
       owner_occupied, kwh, year_built, single_family_res, num_tenants, 
       num_floors, space_heat_equip, space_heat_fuel, space_heat_age_min, 
       space_heat_age_max, water_heat_equip, water_heat_fuel, water_heat_age_min, 
       water_heat_age_max, space_cool_equip, space_cool_fuel, space_cool_age_min, 
       space_cool_age_max, ducts, totsqft, totsqft_heat, totsqft_cool, 
       kbtu_space_heat, kbtu_space_cool, kbtu_water_heat, crb_model, 
       roof_style, roof_sqft
from diffusion_shared.eia_microdata_recs_2009_expanded_bldgs
where reportable_domain = 7
and typehuq = 1;

-- drop the sequence
DROP SEQUENCE IF EXISTS diffusion_shared.eia_microdata_recs_2009_expanded_bldgs_bldg_ids;

-- run the original query again to check for missing vals
-- result: all set this time