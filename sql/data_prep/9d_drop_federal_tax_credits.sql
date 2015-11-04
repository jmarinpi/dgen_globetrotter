-- federal tax credits are no longer pulled from dsire
-- they are pulled from the input sheet instead


--------------------------------------------------------------------------------
-- SOLAR
-- identify which incentives to delete
select a.*, b.*
from diffusion_solar.incentives a
left join geo_incentives.incentives b
ON a.incentive_id = b.gid
where state = 'Federal';
-- ids 122 and 124

-- make sure those are the only incentives with those ids
select *
FROM diffusion_solar.incentives
where uid in (124, 122);
-- all set

-- drop them
DELETE FROM diffusion_solar.incentives
where uid in (124, 122)
-- 3 rows deleted

-- check first query again to make sure no federal incentives remain
-- all set

--------------------------------------------------------------------------------
-- WIND
select a.*, b.*
from diffusion_wind.incentives a
left join geo_incentives.incentives b
ON a.incentive_id = b.gid
where state = 'Federal';
-- there are actuall 5 row -- ITC for res,com,ind
-- and PTC for com and ind

select *
FROM diffusion_wind.incentives
where uid in (124, 122, 123);

DELETE FROM diffusion_wind.incentives
where uid in (124, 122, 123);
-- 5 rows deleted

-- check first query again to make sure no federal incentives remain
-- all set