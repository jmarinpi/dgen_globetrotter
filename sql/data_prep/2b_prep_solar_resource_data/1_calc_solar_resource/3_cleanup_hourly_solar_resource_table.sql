-- add values for gid = 3101 (missing due to a bad tm2 file)
-- to fix, simply replce with the corresponding values from one of its neighbors
-- in this case, the cell to the south (3451) appears to be the best optoin
INSERT INTO diffusion_solar.solar_resource_hourly
SELECT 3101::integer as solar_re_9809_gid, 
	tilt, azimuth, derate, cf
FROM diffusion_solar.solar_resource_hourly
where solar_re_9809_gid = 3451

select *
from diffusion_solar.solar_resource_hourly
where solar_re_9809_gid in (3101,3451)
order by tilt, azimuth, derate;

-- update the derate column to be rounded
UPDATE diffusion_solar.solar_resource_hourly
set derate = round(derate,3);

-- create primary key (on gid, tilt, azimuth)
ALTER TABLE diffusion_solar.solar_resource_hourly
ADD primary key (solar_re_9809_gid, tilt, azimuth, derate);

-- create separate indices
CREATE INDEX solar_resource_hourly_gid_btree
ON diffusion_solar.solar_resource_hourly USING btree(solar_re_9809_gid);

CREATE INDEX solar_resource_hourly_tilt_btree
ON diffusion_solar.solar_resource_hourly USING btree(tilt);

CREATE INDEX solar_resource_hourly_azimuth_btree
ON diffusion_solar.solar_resource_hourly USING btree(azimuth);

CREATE INDEX solar_resource_hourly_derate_btree
ON diffusion_solar.solar_resource_hourly USING btree(derate);

vacuum analyze diffusion_solar.solar_resource_hourly;

-- compare to the solar_resource_annual table as a quality check
-- check that the row count matches the annual table
select count(*)
FROM diffusion_solar.solar_resource_hourly;
-- 640066
select count(*)
FROM diffusion_solar.solar_resource_annual;
-- 640066

-- check stats seem legit (also map these, if possible)
with a as 
(
	Select r_array_sum(cf) as aep
	from diffusion_solar.solar_resource_hourly
)
select min(aep), max(aep), avg(aep)
from a;
-- 754642123 | 1983574854 | 1330267367.36399059

select min(naep), max(naep), avg(naep)
from diffusion_solar.solar_resource_annual;
-- 754.6421
-- 1983.5748
-- 1330.2673

-- they match exactly