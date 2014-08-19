-- residential
DROP TABLE IF EXISTS diffusion_wind.point_microdata_res_us;
SET seed to 1;
CREATE TABLE diffusion_wind.point_microdata_res_us AS
WITH dsire as 
(
	SELECT pt_gid, array_agg(wind_incentives_uid ORDER BY wind_incentives_uid ASC) as 
	FROM diffusion_wind.dsire_incentives_lookup_res 
	GROUP BY pt_gid
),
a AS 
(
	SELECT a.county_id, 
		a.maxheight_m_popdens, a.maxheight_m_popdenscancov20pc, a.maxheight_m_popdenscancov40pc,
		a.annual_rate_gid, 
		'p'::text || a.pca_reg::text AS pca_reg, a.reeds_reg, a.utility_type, 
		b.i, b.j, b.cf_bin, b.aep_scale_factor,
		count(*) as weight
	FROM diffusion_shared.pt_grid_us_res a
	LEFT JOIN diffusion_wind.ij_cfbin_lookup_res_pts_us b 
	ON a.gid = b.pt_gid
	LEFT JOIN dsire c
	ON a.gid = b.pt_gid
	GROUP BY a.county_id, 
		a.maxheight_m_popdens, a.maxheight_m_popdenscancov20pc, a.maxheight_m_popdenscancov40pc,
		a.annual_rate_gid, 
		a.pca_reg, a.reeds_reg, a.utility_type, 
		b.i, b.j, b.cf_bin, b.aep_scale_factor
)
SELECT *, row_number() OVER (ORDER BY county_id, random()) as md_id
FROM a
ORDER BY county_id;

-- primary and foreign keys
ALTER TABLE diffusion_wind.point_microdata_res_us
ADD primary key (md_id);

CREATE INDEX point_microdata_res_us_county_id_btree
  ON diffusion_wind.point_microdata_res_us
  USING btree (county_id);

CREATE INDEX point_microdata_res_us_annual_rate_gid_btree
  ON diffusion_wind.point_microdata_res_us
  USING btree (annual_rate_gid);


----------------------------------------------------------------------------------------------------
-- commercial
DROP TABLE IF EXISTS diffusion_wind.point_microdata_com_us;
SET seed to 1;
CREATE TABLE diffusion_wind.point_microdata_com_us AS
WITH a AS 
(
	SELECT a.county_id, 
		a.maxheight_m_popdens, a.maxheight_m_popdenscancov20pc, a.maxheight_m_popdenscancov40pc,
		a.annual_rate_gid, 
		'p'::text || a.pca_reg::text AS pca_reg, a.reeds_reg, a.utility_type, 
		b.i, b.j, b.cf_bin, b.aep_scale_factor,
		count(*) as weight
	FROM diffusion_shared.pt_grid_us_com a
	LEFT JOIN diffusion_wind.ij_cfbin_lookup_com_pts_us b 
	ON a.gid = b.pt_gid
	GROUP BY a.county_id, 
		a.maxheight_m_popdens, a.maxheight_m_popdenscancov20pc, a.maxheight_m_popdenscancov40pc,
		a.annual_rate_gid, 
		a.pca_reg, a.reeds_reg, a.utility_type, 
		b.i, b.j, b.cf_bin, b.aep_scale_factor
)
SELECT *, row_number() OVER (ORDER BY county_id, random()) as md_id
FROM a
ORDER BY county_id;


-- primary and foreign keys
ALTER TABLE diffusion_wind.point_microdata_com_us
ADD primary key (md_id);

CREATE INDEX point_microdata_com_us_county_id_btree
  ON diffusion_wind.point_microdata_com_us
  USING btree (county_id);

CREATE INDEX point_microdata_com_us_annual_rate_gid_btree
  ON diffusion_wind.point_microdata_com_us
  USING btree (annual_rate_gid);


----------------------------------------------------------------------------------------------------
-- industrial
DROP TABLE IF EXISTS diffusion_wind.point_microdata_ind_us;
SET seed to 1;
CREATE TABLE diffusion_wind.point_microdata_ind_us AS
WITH a AS 
(
	SELECT a.county_id, 
		a.maxheight_m_popdens, a.maxheight_m_popdenscancov20pc, a.maxheight_m_popdenscancov40pc,
		a.annual_rate_gid, 
		'p'::text || a.pca_reg::text AS pca_reg, a.reeds_reg, a.utility_type, 
		b.i, b.j, b.cf_bin, b.aep_scale_factor,
		count(*) as weight
	FROM diffusion_shared.pt_grid_us_ind a
	LEFT JOIN diffusion_wind.ij_cfbin_lookup_ind_pts_us b 
	ON a.gid = b.pt_gid
	GROUP BY a.county_id, 
		a.maxheight_m_popdens, a.maxheight_m_popdenscancov20pc, a.maxheight_m_popdenscancov40pc,
		a.annual_rate_gid, 
		a.pca_reg, a.reeds_reg, a.utility_type, 
		b.i, b.j, b.cf_bin, b.aep_scale_factor
)
SELECT *, row_number() OVER (ORDER BY county_id, random()) as md_id
FROM a
ORDER BY county_id;


-- primary and foreign keys
ALTER TABLE diffusion_wind.point_microdata_ind_us
ADD primary key (md_id);

CREATE INDEX point_microdata_ind_us_county_id_btree
  ON diffusion_wind.point_microdata_ind_us
  USING btree (county_id);

CREATE INDEX point_microdata_ind_us_annual_rate_gid_btree
  ON diffusion_wind.point_microdata_ind_us
  USING btree (annual_rate_gid);