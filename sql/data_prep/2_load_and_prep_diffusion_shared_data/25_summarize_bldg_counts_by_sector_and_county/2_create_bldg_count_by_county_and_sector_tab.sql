set role 'diffusion-writers';

-- get the res cdms types
select *
FROM diffusion_shared.cdms_bldg_types_to_sectors_lkup
where sector_abbr = 'ind';

DROP TABLE IF EXISTS diffusion_shared.county_building_counts_by_sector;
CREATE TABLE diffusion_shared.county_building_counts_by_sector AS
SELECT county_id,
	res1i + res2i + res3ai + res3bi + res3ci + res3di + res3ei + res3fi as bldg_count,
	'res'::VARCHAR(3) as sector_abbr
FROM hazus.sum_stats_bldg_count_block_county 
UNION ALL
SELECT county_id,
	res4i + res5i + res6i + com1i + com2i + com3i + com4i + com5i + com6i + 
	com7i + com8i + com9i + com10i + rel1i + gov1i + gov2i + edu1i + edu2i  as bldg_count,
	'com'::VARCHAR(3) as sector_abbr
FROM hazus.sum_stats_bldg_count_block_county 
UNION ALL
SELECT county_id,
	ind1i + ind2i + ind3i + ind4i + ind5i + ind6i + agr1i  as bldg_count,
	'ind'::VARCHAR(3) as sector_abbr
FROM hazus.sum_stats_bldg_count_block_county;
-- 9423 rows (= 3 sectors x 3141 counties)

-- add primary key
ALTER TABLE diffusion_shared.county_building_counts_by_sector
ADD PRIMARY KEY (county_id, sector_abbr);

-- check for nulls or zeros?
SELECT *
FROM diffusion_shared.county_building_counts_by_sector
where bldg_count is null
or bldg_count = 0;
-- 0 industrial buildings in one county

select county, state_abbr
from diffusion_shared.county_geom
where county_id = 15;
-- this Kalawao in hawaii, which i think is mostly a state or national park, so this is probably not a problem
-- would be good to map... 

-- create view for mapping
DROP TABLE IF EXISTS dgeo.county_building_counts_by_sector_res;
CREATE TABLE dgeo.county_building_counts_by_sector_res AS
select a.the_geom_96703, b.bldg_count as bldg_count
from diffusion_shared.county_geom a
LEFT JOIN diffusion_shared.county_building_counts_by_sector b
ON a.county_id = b.county_id
where b.sector_abbr = 'res';

DROP TABLE IF EXISTS dgeo.county_building_counts_by_sector_com;
CREATE TABLE dgeo.county_building_counts_by_sector_com AS
select a.the_geom_96703, b.bldg_count as bldg_count
from diffusion_shared.county_geom a
LEFT JOIN diffusion_shared.county_building_counts_by_sector b
ON a.county_id = b.county_id
where b.sector_abbr = 'com';

DROP TABLE IF EXISTS dgeo.county_building_counts_by_sector_ind;
CREATE TABLE dgeo.county_building_counts_by_sector_ind AS
select a.the_geom_96703, b.bldg_count as bldg_count
from diffusion_shared.county_geom a
LEFT JOIN diffusion_shared.county_building_counts_by_sector b
ON a.county_id = b.county_id
where b.sector_abbr = 'ind';
-- drop tables after export
DROP TABLE IF EXISTS dgeo.county_building_counts_by_sector_res;
DROP TABLE IF EXISTS dgeo.county_building_counts_by_sector_com;
DROP TABLE IF EXISTS dgeo.county_building_counts_by_sector_ind;
