-- check stats seem legit (also map these, if possible)
select min(naep), max(naep), avg(naep)
from diffusion_solar.solar_resource_annual;

-- check for nulls
select *
from diffusion_solar.solar_resource_annual
where naep is null;
-- this only happens currently for gid = 3101, due to a bad tm2 file
-- to fix, simply replce with the corresponding values from one of its neighbors
-- in this case, the cell to the south (3451) appears to be the best optoin

with replacements as 
(
	SELECT *
	FROM diffusion_solar.solar_resource_annual
	where solar_re_9809_gid = 3451
)
UPDATE diffusion_solar.solar_resource_annual a
set (naep, cf_avg) = (b.naep,b.cf_avg)
FROM replacements b
where a.solar_re_9809_gid = 3101
and a.tilt = b.tilt
and a.azimuth = b.azimuth;

select *
from diffusion_solar.solar_resource_annual
where solar_re_9809_gid in (3101,3451)
order by tilt, azimuth;

-- alter type of the tilt column
SELECT distinct(tilt)
FROM diffusion_solar.solar_resource_annual;

ALTER TABLE diffusion_solar.solar_resource_annual
ALTER COLUMN tilt TYPE integer using tilt::integer;

-- create primary key (on gid, tilt, azimuth)
ALTER TABLE diffusion_solar.solar_resource_annual
ADD primary key (solar_re_9809_gid, tilt, azimuth);

-- create separate indices
CREATE INDEX solar_resource_annual_gid_btree
ON diffusion_solar.solar_resource_annual USING btree(solar_re_9809_gid);

CREATE INDEX solar_resource_annual_tilt_btree
ON diffusion_solar.solar_resource_annual USING btree(tilt);

CREATE INDEX solar_resource_annual_azimuth_btree
ON diffusion_solar.solar_resource_annual USING btree(azimuth);

vacuum analyze diffusion_solar.solar_resource_annual;
