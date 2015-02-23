-- residential
DROP TABLE IF EXISTS diffusion_wind.point_microdata_res_us CASCADE;
SET seed to 1;
CREATE TABLE diffusion_wind.point_microdata_res_us AS
WITH a AS
(
	SELECT a.county_id, 
		'p'::text || a.pca_reg::text AS pca_reg, 
		a.reeds_reg, 
		a.wind_incentive_array_id as incentive_array_id,
		a.ranked_rate_array_id, 
		a.hdf_load_index,
		a.utility_type, 
		-- wind only
		b.i, b.j, b.cf_bin, 
		a.hi_dev_pct,
		a.acres_per_hu,
		a.canopy_ht_m,
		a.canopy_pct,
		-- res only		
		sum(a.blkgrp_ownocc_sf_hu_portion) as point_weight
	FROM diffusion_shared.pt_grid_us_res a
	LEFT JOIN diffusion_wind.ij_cfbin_lookup_res_pts_us b 
	ON a.gid = b.pt_gid
	GROUP BY a.county_id,
		a.pca_reg,
		a.reeds_reg,
		a.wind_incentive_array_id,
		a.ranked_rate_array_id,
		a.hdf_load_index,
		a.utility_type,
		-- wind only
		b.i, b.j, b.cf_bin,
		a.hi_dev_pct,
		a.acres_per_hu,
		a.canopy_ht_m,
		a.canopy_pct	
)
SELECT (row_number() OVER (ORDER BY county_id, random()))::integer as micro_id, *
FROM a
ORDER BY county_id;
--use setseed() and order by random() as a secondary sort key to ensure order will be the same if we have to re run
-- previous version had 1,882,517 rows
-- new version has:
select count(*)
FROM diffusion_wind.point_microdata_res_us;
-- 5,527,728 rows

-- primary key and indices
ALTER TABLE diffusion_wind.point_microdata_res_us
ADD primary key (micro_id);

CREATE INDEX point_microdata_res_us_county_id_btree
  ON diffusion_wind.point_microdata_res_us
  USING btree (county_id);

CREATE INDEX point_microdata_res_us_utility_type_btree
  ON diffusion_wind.point_microdata_res_us
  USING btree (utility_type);

VACUUM ANALYZE diffusion_wind.point_microdata_res_us;

----------------------------------------------------------------------------------------------------
-- commercial
-- select count(*)
-- FROM diffusion_wind.point_microdata_com_us;--426113

DROP TABLE IF EXISTS diffusion_wind.point_microdata_com_us CASCADE;
SET seed to 1;
CREATE TABLE diffusion_wind.point_microdata_com_us AS
WITH a AS
(
	SELECT a.county_id, 
		'p'::text || a.pca_reg::text AS pca_reg, 
		a.reeds_reg, 
		a.wind_incentive_array_id as incentive_array_id,
		a.ranked_rate_array_id, 
		a.hdf_load_index,
		a.utility_type, 
		-- wind only
		b.i, b.j, b.cf_bin, 
		a.hi_dev_pct,
		a.acres_per_hu,
		a.canopy_ht_m,
		a.canopy_pct,
		count(*)::integer as point_weight
	FROM diffusion_shared.pt_grid_us_com a
	LEFT JOIN diffusion_wind.ij_cfbin_lookup_com_pts_us b 
	ON a.gid = b.pt_gid
	GROUP BY a.county_id,
		a.pca_reg,
		a.reeds_reg,
		a.wind_incentive_array_id,
		a.ranked_rate_array_id,
		a.hdf_load_index,
		a.utility_type,
		-- wind only
		b.i, b.j, b.cf_bin,
		a.hi_dev_pct,
		a.acres_per_hu,
		a.canopy_ht_m,
		a.canopy_pct	
)
SELECT (row_number() OVER (ORDER BY county_id, random()))::integer as micro_id, *
FROM a
ORDER BY county_id;
--use setseed() and order by random() as a secondary sort key to ensure order will be the same if we have to re run
-- previous version had 856,691 rows
-- new version has:
select count(*)
FROM diffusion_wind.point_microdata_com_us;
-- 1,495,568  rows
-- vs full pt table:
select count(*)
FROM diffusion_shared.pt_grid_us_com;
-- 1,603,958


-- primary key and indices
ALTER TABLE diffusion_wind.point_microdata_com_us
ADD primary key (micro_id);

CREATE INDEX point_microdata_com_us_county_id_btree
  ON diffusion_wind.point_microdata_com_us
  USING btree (county_id);

CREATE INDEX point_microdata_com_us_utility_type_btree
  ON diffusion_wind.point_microdata_com_us
  USING btree (utility_type);

VACUUM ANALYZE diffusion_wind.point_microdata_com_us;


----------------------------------------------------------------------------------------------------
-- industrial
DROP TABLE IF EXISTS diffusion_wind.point_microdata_ind_us CASCADE;
SET seed to 1;
CREATE TABLE diffusion_wind.point_microdata_ind_us AS
WITH a AS
(
	SELECT a.county_id, 
		'p'::text || a.pca_reg::text AS pca_reg, 
		a.reeds_reg, 
		a.wind_incentive_array_id as incentive_array_id,
		a.ranked_rate_array_id, 
		a.hdf_load_index,
		a.utility_type, 
		-- wind only
		b.i, b.j, b.cf_bin, 
		a.hi_dev_pct,
		a.acres_per_hu,
		a.canopy_ht_m,
		a.canopy_pct,
		count(*)::integer as point_weight
	FROM diffusion_shared.pt_grid_us_ind a
	LEFT JOIN diffusion_wind.ij_cfbin_lookup_ind_pts_us b 
	ON a.gid = b.pt_gid
	GROUP BY a.county_id,
		a.pca_reg,
		a.reeds_reg,
		a.wind_incentive_array_id,
		a.ranked_rate_array_id,
		a.hdf_load_index,
		a.utility_type,
		-- wind only
		b.i, b.j, b.cf_bin,
		a.hi_dev_pct,
		a.acres_per_hu,
		a.canopy_ht_m,
		a.canopy_pct	
)
SELECT (row_number() OVER (ORDER BY county_id, random()))::integer as micro_id, *
FROM a
ORDER BY county_id;
--use setseed() and order by random() as a secondary sort key to ensure order will be the same if we have to re run
-- previous version had 482,706 rows
-- new version has:
select count(*)
FROM diffusion_wind.point_microdata_ind_us;
-- 1077660 rows

-- primary key and indices
ALTER TABLE diffusion_wind.point_microdata_ind_us
ADD primary key (micro_id);

CREATE INDEX point_microdata_ind_us_county_id_btree
  ON diffusion_wind.point_microdata_ind_us
  USING btree (county_id);

CREATE INDEX point_microdata_ind_us_utility_type_btree
  ON diffusion_wind.point_microdata_ind_us
  USING btree (utility_type);

VACUUM ANALYZE diffusion_wind.point_microdata_ind_us;
