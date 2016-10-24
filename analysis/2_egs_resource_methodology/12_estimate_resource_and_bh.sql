DROP TABLE IF EXISTS dgeo.egs_resource_shallow_lowt_extractable_and_beneficial_heat;
CREATE TABLE dgeo.egs_resource_shallow_lowt_extractable_and_beneficial_heat AS
select gid, 
CASE WHEN t500est < 30 or t500est > 150 THEN 0 ELSE diffusion_geo.extractable_resource_joules_recovery_factor((area_sqm * 450)/1e9, t500est, 0.02, 15,2.6)/3.6e+9 end as extr_res_mwh_500est,
CASE WHEN t500min < 30 or t500min > 150 THEN 0 ELSE diffusion_geo.extractable_resource_joules_recovery_factor((area_sqm * 450)/1e9, t500min, 0.02, 15,2.6)/3.6e+9 end as extr_res_mwh_500min,
CASE WHEN t500max < 30 or t500max > 150 THEN 0 ELSE diffusion_geo.extractable_resource_joules_recovery_factor((area_sqm * 450)/1e9, t500max, 0.02, 15,2.6)/3.6e+9 end as extr_res_mwh_500max,
CASE WHEN t1000est < 30 or t1000est > 150 THEN 0 ELSE diffusion_geo.extractable_resource_joules_recovery_factor((area_sqm * 500)/1e9, t1000est, 0.02, 15,2.6)/3.6e+9 end as extr_res_mwh_1000est,
CASE WHEN t1000min < 30 or t1000min > 150 THEN 0 ELSE diffusion_geo.extractable_resource_joules_recovery_factor((area_sqm * 500)/1e9, t1000min, 0.02, 15,2.6)/3.6e+9 end as extr_res_mwh_1000min,
CASE WHEN t1000max < 30 or t1000max > 150 THEN 0 ELSE diffusion_geo.extractable_resource_joules_recovery_factor((area_sqm * 500)/1e9, t1000max, 0.02, 15,2.6)/3.6e+9 end as extr_res_mwh_1000max,
CASE WHEN t1500est < 30 or t1500est > 150 THEN 0 ELSE diffusion_geo.extractable_resource_joules_recovery_factor((area_sqm * 500)/1e9, t1500est, 0.02, 15,2.6)/3.6e+9 end as extr_res_mwh_1500est,
CASE WHEN t1500min < 30 or t1500min > 150 THEN 0 ELSE diffusion_geo.extractable_resource_joules_recovery_factor((area_sqm * 500)/1e9, t1500min, 0.02, 15,2.6)/3.6e+9 end as extr_res_mwh_1500min,
CASE WHEN t1500max < 30 or t1500max > 150 THEN 0 ELSE diffusion_geo.extractable_resource_joules_recovery_factor((area_sqm * 500)/1e9, t1500max, 0.02, 15,2.6)/3.6e+9 end as extr_res_mwh_1500max,
CASE WHEN t2000est < 30 or t2000est > 150 THEN 0 ELSE diffusion_geo.extractable_resource_joules_recovery_factor((area_sqm * 500)/1e9, t2000est, 0.02, 15,2.6)/3.6e+9 end as extr_res_mwh_2000est,
CASE WHEN t2000min < 30 or t2000min > 150 THEN 0 ELSE diffusion_geo.extractable_resource_joules_recovery_factor((area_sqm * 500)/1e9, t2000min, 0.02, 15,2.6)/3.6e+9 end as extr_res_mwh_2000min,
CASE WHEN t2000max < 30 or t2000max > 150 THEN 0 ELSE diffusion_geo.extractable_resource_joules_recovery_factor((area_sqm * 500)/1e9, t2000max, 0.02, 15,2.6)/3.6e+9 end as extr_res_mwh_2000max,
CASE WHEN t2500est < 30 or t2500est > 150 THEN 0 ELSE diffusion_geo.extractable_resource_joules_recovery_factor((area_sqm * 500)/1e9, t2500est, 0.02, 15,2.6)/3.6e+9 end as extr_res_mwh_2500est,
CASE WHEN t2500min < 30 or t2500min > 150 THEN 0 ELSE diffusion_geo.extractable_resource_joules_recovery_factor((area_sqm * 500)/1e9, t2500min, 0.02, 15,2.6)/3.6e+9 end as extr_res_mwh_2500min,
CASE WHEN t2500max < 30 or t2500max > 150 THEN 0 ELSE diffusion_geo.extractable_resource_joules_recovery_factor((area_sqm * 500)/1e9, t2500max, 0.02, 15,2.6)/3.6e+9 end as extr_res_mwh_2500max,
CASE WHEN t3000est < 30 or t3000est > 150 THEN 0 ELSE diffusion_geo.extractable_resource_joules_recovery_factor((area_sqm * 500)/1e9, t3000est, 0.02, 15,2.6)/3.6e+9 end as extr_res_mwh_3000est,
CASE WHEN t3000min < 30 or t3000min > 150 THEN 0 ELSE diffusion_geo.extractable_resource_joules_recovery_factor((area_sqm * 500)/1e9, t3000min, 0.02, 15,2.6)/3.6e+9 end as extr_res_mwh_3000min,
CASE WHEN t3000max < 30 or t3000max > 150 THEN 0 ELSE diffusion_geo.extractable_resource_joules_recovery_factor((area_sqm * 500)/1e9, t3000max, 0.02, 15,2.6)/3.6e+9 end as extr_res_mwh_3000max,
CASE WHEN t500est < 30 or t500est > 150 THEN 0 ELSE diffusion_geo.beneficial_heat_joules_recovery_factor((area_sqm * 450)/1e9, t500est, 0.02)/3.6e+9 end as ben_heat_mwh_500est,
CASE WHEN t500min < 30 or t500min > 150 THEN 0 ELSE diffusion_geo.beneficial_heat_joules_recovery_factor((area_sqm * 450)/1e9, t500min, 0.02)/3.6e+9 end as ben_heat_mwh_500min,
CASE WHEN t500max < 30 or t500max > 150 THEN 0 ELSE diffusion_geo.beneficial_heat_joules_recovery_factor((area_sqm * 450)/1e9, t500max, 0.02)/3.6e+9 end as ben_heat_mwh_500max,
CASE WHEN t1000est < 30 or t1000est > 150 THEN 0 ELSE diffusion_geo.beneficial_heat_joules_recovery_factor((area_sqm * 500)/1e9, t1000est, 0.02)/3.6e+9 end as ben_heat_mwh_1000est,
CASE WHEN t1000min < 30 or t1000min > 150 THEN 0 ELSE diffusion_geo.beneficial_heat_joules_recovery_factor((area_sqm * 500)/1e9, t1000min, 0.02)/3.6e+9 end as ben_heat_mwh_1000min,
CASE WHEN t1000max < 30 or t1000max > 150 THEN 0 ELSE diffusion_geo.beneficial_heat_joules_recovery_factor((area_sqm * 500)/1e9, t1000max, 0.02)/3.6e+9 end as ben_heat_mwh_1000max,
CASE WHEN t1500est < 30 or t1500est > 150 THEN 0 ELSE diffusion_geo.beneficial_heat_joules_recovery_factor((area_sqm * 500)/1e9, t1500est, 0.02)/3.6e+9 end as ben_heat_mwh_1500est,
CASE WHEN t1500min < 30 or t1500min > 150 THEN 0 ELSE diffusion_geo.beneficial_heat_joules_recovery_factor((area_sqm * 500)/1e9, t1500min, 0.02)/3.6e+9 end as ben_heat_mwh_1500min,
CASE WHEN t1500max < 30 or t1500max > 150 THEN 0 ELSE diffusion_geo.beneficial_heat_joules_recovery_factor((area_sqm * 500)/1e9, t1500max, 0.02)/3.6e+9 end as ben_heat_mwh_1500max,
CASE WHEN t2000est < 30 or t2000est > 150 THEN 0 ELSE diffusion_geo.beneficial_heat_joules_recovery_factor((area_sqm * 500)/1e9, t2000est, 0.02)/3.6e+9 end as ben_heat_mwh_2000est,
CASE WHEN t2000min < 30 or t2000min > 150 THEN 0 ELSE diffusion_geo.beneficial_heat_joules_recovery_factor((area_sqm * 500)/1e9, t2000min, 0.02)/3.6e+9 end as ben_heat_mwh_2000min,
CASE WHEN t2000max < 30 or t2000max > 150 THEN 0 ELSE diffusion_geo.beneficial_heat_joules_recovery_factor((area_sqm * 500)/1e9, t2000max, 0.02)/3.6e+9 end as ben_heat_mwh_2000max,
CASE WHEN t2500est < 30 or t2500est > 150 THEN 0 ELSE diffusion_geo.beneficial_heat_joules_recovery_factor((area_sqm * 500)/1e9, t2500est, 0.02)/3.6e+9 end as ben_heat_mwh_2500est,
CASE WHEN t2500min < 30 or t2500min > 150 THEN 0 ELSE diffusion_geo.beneficial_heat_joules_recovery_factor((area_sqm * 500)/1e9, t2500min, 0.02)/3.6e+9 end as ben_heat_mwh_2500min,
CASE WHEN t2500max < 30 or t2500max > 150 THEN 0 ELSE diffusion_geo.beneficial_heat_joules_recovery_factor((area_sqm * 500)/1e9, t2500max, 0.02)/3.6e+9 end as ben_heat_mwh_2500max,
CASE WHEN t3000est < 30 or t3000est > 150 THEN 0 ELSE diffusion_geo.beneficial_heat_joules_recovery_factor((area_sqm * 500)/1e9, t3000est, 0.02)/3.6e+9 end as ben_heat_mwh_3000est,
CASE WHEN t3000min < 30 or t3000min > 150 THEN 0 ELSE diffusion_geo.beneficial_heat_joules_recovery_factor((area_sqm * 500)/1e9, t3000min, 0.02)/3.6e+9 end as ben_heat_mwh_3000min,
CASE WHEN t3000max < 30 or t3000max > 150 THEN 0 ELSE diffusion_geo.beneficial_heat_joules_recovery_factor((area_sqm * 500)/1e9, t3000max, 0.02)/3.6e+9 end as ben_heat_mwh_3000max
from dgeo.egs_resource_shallow_lowt;

-- add columns to sum them up
ALTER TABLE dgeo.egs_resource_shallow_lowt_extractable_and_beneficial_heat
ADD column extr_res_mwh_totest numeric,
ADD column extr_res_mwh_totmin numeric,
ADD column extr_res_mwh_totmax numeric,
ADD column ben_heat_mwh_totest numeric,
ADD column ben_heat_mwh_totmin numeric,
ADD column ben_heat_mwh_totmax numeric;

-- extractable resource
UPDATE dgeo.egs_resource_shallow_lowt_extractable_and_beneficial_heat
SET extr_res_mwh_totest = extr_res_mwh_500est + extr_res_mwh_1000est + extr_res_mwh_1500est + extr_res_mwh_2000est + extr_res_mwh_2500est + extr_res_mwh_3000est;

UPDATE dgeo.egs_resource_shallow_lowt_extractable_and_beneficial_heat
SET extr_res_mwh_totmin = extr_res_mwh_500min + extr_res_mwh_1000min + extr_res_mwh_1500min + extr_res_mwh_2000min + extr_res_mwh_2500min + extr_res_mwh_3000min;

UPDATE dgeo.egs_resource_shallow_lowt_extractable_and_beneficial_heat
SET extr_res_mwh_totmax = extr_res_mwh_500max + extr_res_mwh_1000max + extr_res_mwh_1500max + extr_res_mwh_2000max + extr_res_mwh_2500max + extr_res_mwh_3000max;

-- ben heat
UPDATE dgeo.egs_resource_shallow_lowt_extractable_and_beneficial_heat
SET ben_heat_mwh_totest = ben_heat_mwh_500est + ben_heat_mwh_1000est + ben_heat_mwh_1500est + ben_heat_mwh_2000est + ben_heat_mwh_2500est + ben_heat_mwh_3000est;

UPDATE dgeo.egs_resource_shallow_lowt_extractable_and_beneficial_heat
SET ben_heat_mwh_totmin = ben_heat_mwh_500min + ben_heat_mwh_1000min + ben_heat_mwh_1500min + ben_heat_mwh_2000min + ben_heat_mwh_2500min + ben_heat_mwh_3000min;

UPDATE dgeo.egs_resource_shallow_lowt_extractable_and_beneficial_heat
SET ben_heat_mwh_totmax = ben_heat_mwh_500max + ben_heat_mwh_1000max + ben_heat_mwh_1500max + ben_heat_mwh_2000max + ben_heat_mwh_2500max + ben_heat_mwh_3000max;


-- total up results
select SUM(extr_res_mwh_totest)/1000./1e6 as extr_res_mwh_totest,
	SUM(extr_res_mwh_totmin)/1000./1e6 as extr_res_mwh_totmin,
	SUM(extr_res_mwh_totmax)/1000./1e6 as extr_res_mwh_totmax,
	SUM(ben_heat_mwh_totest)/1000./1e6 as ben_heat_mwh_totest,
	SUM(ben_heat_mwh_totmin)/1000./1e6 as ben_heat_mwh_totmin,
	SUM(ben_heat_mwh_totmax)/1000./1e6 as ben_heat_mwh_totmax
from dgeo.egs_resource_shallow_lowt_extractable_and_beneficial_heat;


-- summarize accessible resource too
select SUM(restotest)/1e9 as restotest,
	SUM(restotmin)/1e9 as restotmin,
	SUM(restotmax)/1e9 as restotmax
from dgeo.egs_resource_shallow_lowt;
