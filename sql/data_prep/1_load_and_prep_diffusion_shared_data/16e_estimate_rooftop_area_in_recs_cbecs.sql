-- RECS

-- make sure we don't have any apartment buildings
-- in the subset of buildings that are evaluated by solards
-- (these have rooftype = -2)
select distinct(rooftype)
from diffusion_shared.eia_microdata_recs_2009
where typehuq in (1,2,3) AND kownrent = 1
order by 1;

-- now check the coding of the stories variable
select stories, sum(nweight), count(*)
from diffusion_shared.eia_microdata_recs_2009
where typehuq in (1,2,3) AND kownrent = 1
group by 1
order by 1;
-- 10	One story
-- 20	Two stories
-- 31	Three stories
-- 32	Four or more stories (this is so rare -- 85309.764261 in the whole country) -- so it is reasonable to use 4 as an estimate 
-- 40	Split-level
-- 50	Other type
-- -2"	Not Applicable"

-- 'other type' and 'not applicable' are not present in the data subset, so we can ignore these
-- recode as follows:
-- 10	One story 		--> 1
-- 20	Two stories		--> 2
-- 31	Three stories		--> 3
-- 32	Four or more stories	--> 4
-- 40	Split-level		--> 2

-- add a column for the roof area in sqft
ALTER TABLE diffusion_shared.eia_microdata_recs_2009
ADD COLUMN roof_sqft integer;

-- calculate the roof area in sqft as the total sqft / number of floors in the home
with a as
(
	select *, 
		case 	when stories = 10 then 1
			when stories = 20 then 2
			when stories = 31 then 3
			when stories = 32 then 4
			when stories = 40 then 2
		end as num_floors
	from diffusion_shared.eia_microdata_recs_2009
	where typehuq in (1,2,3) AND kownrent = 1
),
b AS
(
	select doeid, totsqft/num_floors as roof_sqft
	from a
	order by 1
)
UPDATE diffusion_shared.eia_microdata_recs_2009 c
set roof_sqft = b.roof_sqft
from b
where c.doeid = b.doeid

-- make sure no nulls
select count(*)
from diffusion_shared.eia_microdata_recs_2009
where typehuq in (1,2,3) AND kownrent = 1
and roof_sqft is null;
-- zero --  all set

-- check results
select roof_sqft, *
from diffusion_shared.eia_microdata_recs_2009
where roof_sqft is not null
order by 1;
-- there are some very roof areas but they all appear to be single story buildings with large sq ft,
-- so unless the stories field is coded incorrectly, the algebra should be correct

---------------------------------------------------------------------------------------------------

-- CBECS
