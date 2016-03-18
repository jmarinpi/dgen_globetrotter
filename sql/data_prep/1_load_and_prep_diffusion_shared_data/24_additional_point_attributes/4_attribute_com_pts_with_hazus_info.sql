set role 'diffusion-writers';



------------------------------------------------------------------------------------------------------
-- COM
------------------------------------------------------------------------------------------------------
-- add columns to table
ALTER TABLE diffusion_shared.pt_grid_us_com
ADD COLUMN acres_per_bldg numeric,
ADD COLUMN bldg_type_probs integer[];


WITH b as
(
	select a.gid, 
		(b.res1i + b.res2i + b.res3ai + b.res3bi + b.res3ci + b.res3di + b.res3ei + b.res3fi + b.res4i + b.res5i + b.res6i + 
		b.com1i + b.com2i + b.com3i + b.com4i + b.com5i + b.com6i + b.com7i + b.com8i + b.com9i + b.com10i + 
		b.ind1i + b.ind2i + b.ind3i + b.ind4i + b.ind5i + b.ind6i + 
		b.agr1i + 
		b.rel1i + 
		b.gov1i + b.gov2i + 
		b.edu1i + b.edu2i) as bldg_count, 
		a.aland10/4046.86 as aland_acres,
		array
		[
			b.com1i::INTEGER, b.com2i::INTEGER, b.com3i::INTEGER, b.com4i::INTEGER, b.com5i::INTEGER, 
			b.com6i::INTEGER, b.com7i::INTEGER, b.com8i::INTEGER, b.com9i::INTEGER, b.com10i::INTEGER, 
			b.edu1i::INTEGER, b.edu2i::INTEGER,
			b.gov1i::INTEGER, b.gov2i::INTEGER, 
			b.rel1i::INTEGER, 
			b.res4i::INTEGER, b.res5i::INTEGER, b.res6i::INTEGER, 
		] as bldg_type_probs
	from diffusion_data_wind.pt_grid_us_com_new_census_2010_block_lkup a
	LEFT JOIN hazus.hzbldgcountoccupb b
	ON b.block_gisjoin = b.census_2010_gisjoin
	where b.has_bldgs = True
)
UPDATE diffusion_shared.pt_grid_us_com a
set (acres_per_bldg, bldg_type_probs) = 
    (CASE 
	WHEN b.bldg_count > 0 THEN b.aland_acres/b.bldg_count
	ELSE 1000 
     END,
     b.bldg_type_probs
    )
FROM b
where a.gid = b.gid;

-- add a comment on the column defining the order of the fuel types
COMMENT ON COLUMN diffusion_shared.pt_grid_us_com.bldg_type_probs IS
'Bldg Types are (in order): com1, com2, com3, com4, com5, com6, com7, com8, com9, com10, edu1, edu2, gov1, gov2, rel1, res4, res5, res6';

-- check for nulls
-- check for zeros
-- check for values = 1000
-- map it for a couple counties?


-- create a table that defines the order of all commercial bldg types
DROP TABLE IF EXISTS diffusion_shared.cdms_bldg_type_array_com;
CREATE TABLE diffusion_shared.cdms_bldg_type_array_com
(
	bldg_type_array text[]
);

INSERT INTO diffusion_shared.cdms_bldg_type_array_com
select array
[
	'com1', 
	'com2', 
	'com3', 
	'com4', 
	'com5', 
	'com6', 
	'com7', 
	'com8', 
	'com9', 
	'com10', 
	'edu1', 
	'edu2', 
	'gov1', 
	'gov2', 
	'rel1', 
	'res4', 
	'res5', 
	'res6'
];
-- note: the formatting matches the distinct space_heat_fuel values in diffusion_shared.eia_microdata_recs_2009_expanded_bldgs

-- check results
select *
FROM diffusion_shared.cdms_com_bldg_type_array;
-- check for no bldg types
	-- delete if not that many
-- for conservative purposes, add a column that is the smaller of acres_per_hu and acres_per_building
ALTER TABLE diffusion_shared.pt_grid_us_com
ADD COLUMN acres_per_hu_or_bldg numeric;

UPDATE diffusion_shared.pt_grid_us_com
SET acres_per_hu_or_bldg = r_min(array[acres_per_hu, acres_per_bldg]);
