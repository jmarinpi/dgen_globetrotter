-- Create my table, you idiot
DROP TABLE IF EXISTS diffusion_solar.solar_resource_by_pca_summary;
CREATE TABLE diffusion_solar.solar_resource_by_pca_summary
(
pca int,
npoints int,
tilt int,
azimuth character varying(2),
h01 numeric,
h02 numeric,
h03 numeric,
h04 numeric,
h05 numeric,
h06 numeric,
h07 numeric,
h08 numeric,
h09 numeric,
h10 numeric,
h11 numeric,
h12 numeric,
h13 numeric,
h14 numeric,
h15 numeric,
h16 numeric,
h17 numeric
);

ALTER TABLE diffusion_solar.solar_resource_by_pca_summary
  OWNER TO "diffusion-writers";

--Calculate the average CF by timeslice averaging over gids in the pca, you dipshit
INSERT INTO diffusion_solar.solar_resource_by_pca_summary
SELECT a.pca_reg, count(a.solar_re_9809_gid) as n, b.tilt, b.azimuth, AVG(b.h01) as h01, AVG(b.h02) as h02, AVG(b.h03) as h03, AVG(b.h04) as h04,AVG(b.h05) as h05,AVG(b.h06) as h06,AVG(b.h07) as h07,AVG(b.h08) as h08,AVG(b.h09) as h09,AVG(b.h10) as h10,AVG(b.h11) as h11,AVG(b.h12) as h12,AVG(b.h13) as h13,AVG(b.h14) as h14,AVG(b.h15) as h15,AVG(b.h16) as h16, AVG(b.h03 * c.ratio) as h17
FROM
(
SELECT DISTINCT solar_re_9809_gid, pca_reg FROM diffusion_shared.pt_grid_us_com --Find the distinct solar_re_9809_gid and pca_reg combinations
UNION
SELECT DISTINCT solar_re_9809_gid, pca_reg FROM diffusion_shared.pt_grid_us_res
UNION
SELECT DISTINCT solar_re_9809_gid, pca_reg FROM diffusion_shared.pt_grid_us_ind
)a
LEFT JOIN diffusion_solar.hourly_resource_by_time_slice b
ON a.solar_re_9809_gid = b.solar_re_9809_gid
LEFT JOIN diffusion_solar.h17_to_h3_ratio_by_pca c -- Use a precalculated ratio of h17 to h3 (the h17 hour indices vary by PCA, so this is less robust but simpler)
ON a.pca_reg = c.pca_reg
GROUP BY a.pca_reg, tilt, azimuth
;

ALTER TABLE diffusion_solar.solar_resource_by_pca_summary
   OWNER TO "diffusion-writers";
;


