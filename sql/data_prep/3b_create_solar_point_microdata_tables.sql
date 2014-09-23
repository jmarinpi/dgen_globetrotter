-- residential
DROP TABLE IF EXISTS diffusion_solar.point_microdata_res_us;
SET seed to 1;
CREATE TABLE diffusion_solar.point_microdata_res_us AS
WITH a AS
(
	SELECT a.county_id, 
		a.annual_rate_gid, 
		'p'::text || a.pca_reg::text AS pca_reg, a.reeds_reg, a.solar_incentive_array_id,
		a.utility_type, 
		a.solar_re_9809_gid,
		count(*)::integer as point_weight
	FROM diffusion_shared.pt_grid_us_res a
	GROUP BY a.county_id, 
		a.annual_rate_gid, 
		a.pca_reg, a.reeds_reg, a.wind_incentive_array_id,
		a.utility_type, 
		a.solar_re_9809_gid
)
SELECT (row_number() OVER (ORDER BY county_id, random()))::integer as micro_id, *
FROM a
ORDER BY county_id;
--use setseed() and order by random() as a secondary sort key to ensure order will be the same if we have to re run


-- primary key and indices
ALTER TABLE diffusion_solar.point_microdata_res_us
ADD primary key (micro_id);

CREATE INDEX point_microdata_res_us_county_id_btree
  ON diffusion_solar.point_microdata_res_us
  USING btree (county_id);

CREATE INDEX point_microdata_res_us_annual_rate_gid_btree
  ON diffusion_solar.point_microdata_res_us
  USING btree (annual_rate_gid);

CREATE INDEX point_microdata_res_us_utility_type_btree
  ON diffusion_solar.point_microdata_res_us
  USING btree (utility_type);

BEGIN;
VACUUM ANALYZE diffusion_solar.point_microdata_res_us;
commit;

----------------------------------------------------------------------------------------------------
-- commercial
DROP TABLE IF EXISTS diffusion_solar.point_microdata_com_us;
SET seed to 1;
CREATE TABLE diffusion_solar.point_microdata_com_us AS
WITH a AS
(
	SELECT a.county_id, 
		a.annual_rate_gid, 
		'p'::text || a.pca_reg::text AS pca_reg, a.reeds_reg, a.solar_incentive_array_id,
		a.utility_type, 
		a.solar_re_9809_gid,
		count(*)::integer as point_weight
	FROM diffusion_shared.pt_grid_us_com a
	GROUP BY a.county_id, 
		a.annual_rate_gid, 
		a.pca_reg, a.reeds_reg, a.solar_incentive_array_id,
		a.utility_type, 
		a.solar_re_9809_gid
)
SELECT (row_number() OVER (ORDER BY county_id, random()))::integer as micro_id, *
FROM a
ORDER BY county_id;
--use setseed() and order by random() as a secondary sort key to ensure order will be the same if we have to re run


-- primary key and indices
ALTER TABLE diffusion_solar.point_microdata_com_us
ADD primary key (micro_id);

CREATE INDEX point_microdata_com_us_county_id_btree
  ON diffusion_solar.point_microdata_com_us
  USING btree (county_id);

CREATE INDEX point_microdata_com_us_annual_rate_gid_btree
  ON diffusion_solar.point_microdata_com_us
  USING btree (annual_rate_gid);

CREATE INDEX point_microdata_com_us_utility_type_btree
  ON diffusion_solar.point_microdata_com_us
  USING btree (utility_type);

BEGin;
VACUUM ANALYZE diffusion_solar.point_microdata_com_us;
commit;


----------------------------------------------------------------------------------------------------
-- industrial
DROP TABLE IF EXISTS diffusion_solar.point_microdata_ind_us;
SET seed to 1;
CREATE TABLE diffusion_solar.point_microdata_ind_us AS
WITH a AS
(
	SELECT a.county_id, 
		a.annual_rate_gid, 
		'p'::text || a.pca_reg::text AS pca_reg, a.reeds_reg, a.solar_incentive_array_id,
		a.utility_type, 
		a.solar_re_9809_gid,
		count(*)::integer as point_weight
	FROM diffusion_shared.pt_grid_us_ind a
	GROUP BY a.county_id, 
		a.annual_rate_gid, 
		a.pca_reg, a.reeds_reg, a.solar_incentive_array_id,
		a.utility_type, 
		a.solar_re_9809_gid
)
SELECT (row_number() OVER (ORDER BY county_id, random()))::integer as micro_id, *
FROM a
ORDER BY county_id;
--use setseed() and order by random() as a secondary sort key to ensure order will be the same if we have to re run


-- primary key and indices
ALTER TABLE diffusion_solar.point_microdata_ind_us
ADD primary key (micro_id);

CREATE INDEX point_microdata_ind_us_county_id_btree
  ON diffusion_solar.point_microdata_ind_us
  USING btree (county_id);

CREATE INDEX point_microdata_ind_us_annual_rate_gid_btree
  ON diffusion_solar.point_microdata_ind_us
  USING btree (annual_rate_gid);

CREATE INDEX point_microdata_ind_us_utility_type_btree
  ON diffusion_solar.point_microdata_ind_us
  USING btree (utility_type);

Begin;
VACUUM ANALYZE diffusion_solar.point_microdata_ind_us;
commit;