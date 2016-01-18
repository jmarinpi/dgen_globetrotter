set role 'diffusion-writers';
-----------------------------------------------------------------------------------------------------
-- check row counts of all tables is the same and equal to 888875
with a as
(
	select count(*) FROM diffusion_wind.wind_resource_annual_turbine_1 UNION ALL
	select count(*) FROM diffusion_wind.wind_resource_annual_turbine_2 UNION ALL
	select count(*) FROM diffusion_wind.wind_resource_annual_turbine_3 UNION ALL
	select count(*) FROM diffusion_wind.wind_resource_annual_turbine_4 UNION ALL
	select count(*) FROM diffusion_wind.wind_resource_annual_turbine_5 UNION ALL
	select count(*) FROM diffusion_wind.wind_resource_annual_turbine_6 UNION ALL
	select count(*) FROM diffusion_wind.wind_resource_annual_turbine_7 UNION ALL
	select count(*) FROM diffusion_wind.wind_resource_annual_turbine_8 UNION ALL
	select count(*) FROM diffusion_wind.wind_resource_hourly_turbine_1 UNION ALL
	select count(*) FROM diffusion_wind.wind_resource_hourly_turbine_2 UNION ALL
	select count(*) FROM diffusion_wind.wind_resource_hourly_turbine_3 UNION ALL
	select count(*) FROM diffusion_wind.wind_resource_hourly_turbine_4 UNION ALL
	select count(*) FROM diffusion_wind.wind_resource_hourly_turbine_5 UNION ALL
	select count(*) FROM diffusion_wind.wind_resource_hourly_turbine_6 UNION ALL
	select count(*) FROM diffusion_wind.wind_resource_hourly_turbine_7 UNION ALL
	select count(*) FROM diffusion_wind.wind_resource_hourly_turbine_8
)
select  distinct count
from a;
-- 888875
-- all set

-----------------------------------------------------------------------------------------------------
-- check annual aep is not very different from previous results for same turbine class and model
-- current small residential
with a as
(
	select round(a.aep/b.aep, 2) as diff, a.aep, b.aep, b.height
	from diffusion_wind.wind_resource_annual_turbine_1 a
	LEFT JOIN diffusion_wind.wind_resource_current_residential_turbine b
	ON a.i = b.i
	and a.j = b.j
	and a.cf_bin = b.cf_bin
	and a.height = b.height
	where b.aep > 0
)
select min(diff), avg(diff), max(diff)
from a;
-- average difference is about 1.5x better

-- current small commercial
with a as
(
	select round(a.aep/b.aep, 2) as diff, a.aep, b.aep, b.height
	from diffusion_wind.wind_resource_annual_turbine_2 a
	LEFT JOIN diffusion_wind.wind_resource_current_small_commercial_turbine b
	ON a.i = b.i
	and a.j = b.j
	and a.cf_bin = b.cf_bin
	and a.height = b.height
	where b.aep > 0
)
select min(diff), avg(diff), max(diff)
from a;
-- average difference is about 1.05x better

-- current mid size
with a as
(
	select round(a.aep/b.aep, 2) as diff, a.aep, b.aep, b.height
	from diffusion_wind.wind_resource_annual_turbine_3 a
	LEFT JOIN diffusion_wind.wind_resource_current_mid_size_turbine b
	ON a.i = b.i
	and a.j = b.j
	and a.cf_bin = b.cf_bin
	and a.height = b.height
	where b.aep > 0
)
select min(diff), avg(diff), max(diff)
from a;
-- average difference is about 0.9x of old (10% worse)

-- current large
with a as
(
	select round(a.aep/b.aep, 2) as diff, a.aep, b.aep, b.height
	from diffusion_wind.wind_resource_annual_turbine_4 a
	LEFT JOIN diffusion_wind.wind_resource_current_large_turbine b
	ON a.i = b.i
	and a.j = b.j
	and a.cf_bin = b.cf_bin
	and a.height = b.height
	where b.aep > 0
)
select min(diff), avg(diff), max(diff)
from a;
-- average difference is about 0.99x of old -- effectively no change

-- small res - 10% improvement
with a as
(
	select round(a.aep/b.aep, 2) as diff, a.aep, b.aep, b.height
	from diffusion_wind.wind_resource_annual_turbine_5 a
	LEFT JOIN diffusion_wind.wind_resource_residential_near_future_turbine b
	ON a.i = b.i
	and a.j = b.j
	and a.cf_bin = b.cf_bin
	and a.height = b.height
	where b.aep > 0
)
select min(diff), avg(diff), max(diff)
from a;
-- about 1.18x of old (18% better)

-- small res - 25% improvement
with a as
(
	select round(a.aep/b.aep, 2) as diff, a.aep, b.aep, b.height
	from diffusion_wind.wind_resource_annual_turbine_6 a
	LEFT JOIN diffusion_wind.wind_resource_residential_far_future_turbine b
	ON a.i = b.i
	and a.j = b.j
	and a.cf_bin = b.cf_bin
	and a.height = b.height
	where b.aep > 0
)
select min(diff), avg(diff), max(diff)
from a;
-- about 0.95x of old (5% worse)

-- all other - 10% improvement
with a as
(
	select round(a.aep/b.aep, 2) as diff, a.aep, b.aep, b.height
	from diffusion_wind.wind_resource_annual_turbine_7 a
	LEFT JOIN diffusion_wind.wind_resource_sm_mid_lg_near_future_turbine b
	ON a.i = b.i
	and a.j = b.j
	and a.cf_bin = b.cf_bin
	and a.height = b.height
	where b.aep > 0
)
select min(diff), avg(diff), max(diff)
from a;
-- about 0.97x of old (3% worse)

-- all other - 25% improvement
with a as
(
	select round(a.aep/b.aep, 2) as diff, a.aep, b.aep, b.height
	from diffusion_wind.wind_resource_annual_turbine_8 a
	LEFT JOIN diffusion_wind.wind_resource_sm_mid_lg_far_future_turbine b
	ON a.i = b.i
	and a.j = b.j
	and a.cf_bin = b.cf_bin
	and a.height = b.height
	where b.aep > 0
)
select min(diff), avg(diff), max(diff)
from a;
-- about 0.98x of old (2% worse)

-- I asked Robert and Trudy about the average differences and they will confirm taht the values are within
-- a reasonable range
-- response from robert (regarding turbine 1 differences): 
 -- "So the bottom line is that I think that we have corrected a distortion rather than created one."

-----------------------------------------------------------------------------------------------------
-- compare annual aep across 3 performance improvements for each turbine class -- should see moderate improvements in the 10% and 25% ranges from "current tech"

-- small residential
with a as
(
	select b.aep/a.aep as diff_1, c.aep/a.aep as diff_2
	from diffusion_wind.wind_resource_annual_turbine_1 a
	LEFT JOIN diffusion_wind.wind_resource_annual_turbine_5 b
		ON a.i = b.i
		and a.j = b.j
		and a.cf_bin = b.cf_bin
		and a.height = b.height
	LEFT JOIN diffusion_wind.wind_resource_annual_turbine_6 c
		ON a.i = c.i
		and a.j = c.j
		and a.cf_bin = c.cf_bin
		and a.height = c.height
	where a.aep > 0
	and a.height <= 50
)
select round(avg(diff_1), 2), round(avg(diff_2), 2)
from a;
-- 1.36, 1.58

-- small commercial
with a as
(
	select b.aep/a.aep as diff_1, c.aep/a.aep as diff_2
	from diffusion_wind.wind_resource_annual_turbine_2 a
	LEFT JOIN diffusion_wind.wind_resource_annual_turbine_7 b
		ON a.i = b.i
		and a.j = b.j
		and a.cf_bin = b.cf_bin
		and a.height = b.height
	LEFT JOIN diffusion_wind.wind_resource_annual_turbine_8 c
		ON a.i = c.i
		and a.j = c.j
		and a.cf_bin = c.cf_bin
		and a.height = c.height
	where a.aep > 0
	and a.height >= 30 and a.height <= 50
)
select round(avg(diff_1),2), round(avg(diff_2),2)
from a;
-- 1.14, 1.30

-- midsize
with a as
(
	select b.aep/a.aep as diff_1, c.aep/a.aep as diff_2
	from diffusion_wind.wind_resource_annual_turbine_3 a
	LEFT JOIN diffusion_wind.wind_resource_annual_turbine_7 b
		ON a.i = b.i
		and a.j = b.j
		and a.cf_bin = b.cf_bin
		and a.height = b.height
	LEFT JOIN diffusion_wind.wind_resource_annual_turbine_8 c
		ON a.i = c.i
		and a.j = c.j
		and a.cf_bin = c.cf_bin
		and a.height = c.height
	where a.aep > 0
	and a.height >= 50
)
select round(avg(diff_1),2), round(avg(diff_2),2)
from a;
-- 1.73, 1.95
-- -- why is the mid size turbine less performant than the small commercial????????


-- large
with a as
(
	select b.aep/a.aep as diff_1, c.aep/a.aep as diff_2
	from diffusion_wind.wind_resource_annual_turbine_4 a
	LEFT JOIN diffusion_wind.wind_resource_annual_turbine_7 b
		ON a.i = b.i
		and a.j = b.j
		and a.cf_bin = b.cf_bin
		and a.height = b.height
	LEFT JOIN diffusion_wind.wind_resource_annual_turbine_8 c
		ON a.i = c.i
		and a.j = c.j
		and a.cf_bin = c.cf_bin
		and a.height = c.height
	where a.aep > 0
	and a.height >= 50
)
select round(avg(diff_1),2), round(avg(diff_2),2)
from a;
-- 1.15, 1.30
-- why is the mid size turbine less performant than the small commercial????????
-- are these improvement numbers within reasonable range?
-- response from robert: "The improvements over time make sense"

-----------------------------------------------------------------------------------------------------
-- check that aep increases at each fixed time from small res --> small com --> midsize --> large

-- current
with a as
(
	select b.aep/a.aep as diff_1, c.aep/a.aep as diff_2, d.aep/a.aep as diff_3
	from diffusion_wind.wind_resource_annual_turbine_1 a
	LEFT JOIN diffusion_wind.wind_resource_annual_turbine_2 b
		ON a.i = b.i
		and a.j = b.j
		and a.cf_bin = b.cf_bin
		and a.height = b.height
	LEFT JOIN diffusion_wind.wind_resource_annual_turbine_3 c
		ON a.i = c.i
		and a.j = c.j
		and a.cf_bin = c.cf_bin
		and a.height = c.height
	LEFT JOIN diffusion_wind.wind_resource_annual_turbine_4 d
		ON a.i = d.i
		and a.j = d.j
		and a.cf_bin = d.cf_bin
		and a.height = d.height
	where a.aep > 0
)
select round(avg(diff_1),2), round(avg(diff_2),2), round(avg(diff_3),2)
from a;
-- 1.32,0.85,1.29
-- this doesn't make a whole lot of sense either.... 
-- biggest improvement over small res is small com, then large, then mid size
-- check with Robert...
-- (these results ARE consistent with the power curves ordering....)
-- response from robert: "what you see is exactly what I would expect."

-- 10% improvement (i.e., "near future")
with a as
(
	select b.aep/a.aep as diff_1
	from diffusion_wind.wind_resource_annual_turbine_5 a
	LEFT JOIN diffusion_wind.wind_resource_annual_turbine_7 b
		ON a.i = b.i
		and a.j = b.j
		and a.cf_bin = b.cf_bin
		and a.height = b.height
	where a.aep > 0
)
select round(avg(diff_1),2)
from a;
-- 1.11
-- good -- small res is worse


-- 25% improvement (i.e., "far future")
with a as
(
	select b.aep/a.aep as diff_1
	from diffusion_wind.wind_resource_annual_turbine_6 a
	LEFT JOIN diffusion_wind.wind_resource_annual_turbine_8 b
		ON a.i = b.i
		and a.j = b.j
		and a.cf_bin = b.cf_bin
		and a.height = b.height
	where a.aep > 0
)
select round(avg(diff_1),2)
from a;
-- 1.10
-- good -- small res is still worse, but slight narrowing of the gap

-----------------------------------------------------------------------------------------------------
-- check cf_avg is close to the cf_bin (depending on the turbine height and power curve, it will likely be lower -- could be higher for future big turbines )
-- only check at 80 m (sicne that is where the cf_bin is measured)
-- NOTE: THis check really isn't all that helpful since there are so many variables in play that it is not really an apples to apples comparison


with a as
(
	select cf_avg/(cf_bin::numeric/100) as cf_diff, cf_avg, cf_bin::numeric/100
	from diffusion_wind.wind_resource_annual_turbine_1
	where cf_bin > 0
	and height = 80
)
select round(min(cf_diff), 2) as min, round(avg(cf_diff), 2) as avg, round(max(cf_diff), 2) as max
from a;
-- 0.59,0.90,1.95

with a as
(
	select cf_avg/(cf_bin::numeric/100) as cf_diff, cf_avg, cf_bin::numeric/100
	from diffusion_wind.wind_resource_annual_turbine_2
	where cf_bin > 0
	and height = 80
)
select round(min(cf_diff), 2) as min, round(avg(cf_diff), 2) as avg, round(max(cf_diff), 2) as max
from a;
-- 0.67,1.16,2.67

with a as
(
	select cf_avg/(cf_bin::numeric/100) as cf_diff, cf_avg, cf_bin::numeric/100
	from diffusion_wind.wind_resource_annual_turbine_3
	where cf_bin > 0
	and height = 80
)
select round(min(cf_diff), 2) as min, round(avg(cf_diff), 2) as avg, round(max(cf_diff), 2) as max
from a;
-- 0.53,0.77,1.65

with a as
(
	select cf_avg/(cf_bin::numeric/100) as cf_diff, cf_avg, cf_bin::numeric/100
	from diffusion_wind.wind_resource_annual_turbine_4
	where cf_bin > 0
	and height = 80
)
select round(min(cf_diff), 2) as min, round(avg(cf_diff), 2) as avg, round(max(cf_diff), 2) as max
from a;
-- 0.67,1.14,2.61
-- a little strange that this one is higher. it should be very close...


with a as
(
	select cf_avg/(cf_bin::numeric/100) as cf_diff, cf_avg, cf_bin::numeric/100
	from diffusion_wind.wind_resource_annual_turbine_5
	where cf_bin > 0
	and height = 80
)
select round(min(cf_diff), 2) as min, round(avg(cf_diff), 2) as avg, round(max(cf_diff), 2) as max
from a;
-- 0.68,1.18,2.75

with a as
(
	select cf_avg/(cf_bin::numeric/100) as cf_diff, cf_avg, cf_bin::numeric/100
	from diffusion_wind.wind_resource_annual_turbine_6
	where cf_bin > 0
	and height = 80
)
select round(min(cf_diff), 2) as min, round(avg(cf_diff), 2) as avg, round(max(cf_diff), 2) as max
from a;
-- 0.71,1.34,3.27

with a as
(
	select cf_avg/(cf_bin::numeric/100) as cf_diff, cf_avg, cf_bin::numeric/100
	from diffusion_wind.wind_resource_annual_turbine_7
	where cf_bin > 0
	and height = 80
)
select round(min(cf_diff), 2) as min, round(avg(cf_diff), 2) as avg, round(max(cf_diff), 2) as max
from a;
-- 0.70,1.30,3.11


with a as
(
	select cf_avg/(cf_bin::numeric/100) as cf_diff, cf_avg, cf_bin::numeric/100
	from diffusion_wind.wind_resource_annual_turbine_8
	where cf_bin > 0
	and height = 80
)
select round(min(cf_diff), 2) as min, round(avg(cf_diff), 2) as avg, round(max(cf_diff), 2) as max
from a;
-- 0.73,1.45,3.67


-----------------------------------------------------------------------------------------------------
-- sum cf timeseries and compare to aep values -- should be close (use random sample of 5000 rows since full check is too slow)
with b as
(
	select *
	from diffusion_wind.wind_resource_annual_turbine_1 a
	where aep > 0
	ORDER BY RANDOM()
	limit 5000
),
a as
(
	select (r_array_sum(a.cf)/1e3)/b.aep as diff
	from b
	LEFT JOIN diffusion_wind.wind_resource_hourly_turbine_1 a
		on a.i = b.i
		and a.j = b.j
		and a.cf_bin = b.cf_bin
		and a.height = b.height
)
select round(min(diff), 5) as min, round(avg(diff), 5) as avg, round(max(diff), 5) as max
from a;
-- 0.99768,0.99999,1.00152

with b as
(
	select *
	from diffusion_wind.wind_resource_annual_turbine_2 a
	where aep > 0
	ORDER BY RANDOM()
	limit 5000
),
a as
(
	select (r_array_sum(a.cf)/1e3)/b.aep as diff
	from b
	LEFT JOIN diffusion_wind.wind_resource_hourly_turbine_2 a
		on a.i = b.i
		and a.j = b.j
		and a.cf_bin = b.cf_bin
		and a.height = b.height
)
select round(min(diff), 5) as min, round(avg(diff), 5) as avg, round(max(diff), 5) as max
from a;
-- 0.99710,0.99999,1.00116

with b as
(
	select *
	from diffusion_wind.wind_resource_annual_turbine_3 a
	where aep > 0
	ORDER BY RANDOM()
	limit 5000
),
a as
(
	select (r_array_sum(a.cf)/1e3)/b.aep as diff
	from b
	LEFT JOIN diffusion_wind.wind_resource_hourly_turbine_3 a
		on a.i = b.i
		and a.j = b.j
		and a.cf_bin = b.cf_bin
		and a.height = b.height
)
select round(min(diff), 5) as min, round(avg(diff), 5) as avg, round(max(diff), 5) as max
from a;
-- 0.99870,0.99999,1.00204

with b as
(
	select *
	from diffusion_wind.wind_resource_annual_turbine_4 a
	where aep > 0
	ORDER BY RANDOM()
	limit 5000
),
a as
(
	select (r_array_sum(a.cf)/1e3)/b.aep as diff
	from b
	LEFT JOIN diffusion_wind.wind_resource_hourly_turbine_4 a
		on a.i = b.i
		and a.j = b.j
		and a.cf_bin = b.cf_bin
		and a.height = b.height
)
select round(min(diff), 5) as min, round(avg(diff), 5) as avg, round(max(diff), 5) as max
from a;
-- 0.99672,0.99999,1.00204

with b as
(
	select *
	from diffusion_wind.wind_resource_annual_turbine_5 a
	where aep > 0
	ORDER BY RANDOM()
	limit 5000
),
a as
(
	select (r_array_sum(a.cf)/1e3)/b.aep as diff
	from b
	LEFT JOIN diffusion_wind.wind_resource_hourly_turbine_5 a
		on a.i = b.i
		and a.j = b.j
		and a.cf_bin = b.cf_bin
		and a.height = b.height
)
select round(min(diff), 5) as min, round(avg(diff), 5) as avg, round(max(diff), 5) as max
from a;
-- 0.99917,0.99999,1.00167

with b as
(
	select *
	from diffusion_wind.wind_resource_annual_turbine_6 a
	where aep > 0
	ORDER BY RANDOM()
	limit 5000
),
a as
(
	select (r_array_sum(a.cf)/1e3)/b.aep as diff
	from b
	LEFT JOIN diffusion_wind.wind_resource_hourly_turbine_6 a
		on a.i = b.i
		and a.j = b.j
		and a.cf_bin = b.cf_bin
		and a.height = b.height
)
select round(min(diff), 5) as min, round(avg(diff), 5) as avg, round(max(diff), 5) as max
from a;
-- 0.99922,0.99999,1.00041

with b as
(
	select *
	from diffusion_wind.wind_resource_annual_turbine_7 a
	where aep > 0
	ORDER BY RANDOM()
	limit 5000
),
a as
(
	select (r_array_sum(a.cf)/1e3)/b.aep as diff
	from b
	LEFT JOIN diffusion_wind.wind_resource_hourly_turbine_7 a
		on a.i = b.i
		and a.j = b.j
		and a.cf_bin = b.cf_bin
		and a.height = b.height
)
select round(min(diff), 5) as min, round(avg(diff), 5) as avg, round(max(diff), 5) as max
from a;
-- 0.99940,0.99999,1.00121

with b as
(
	select *
	from diffusion_wind.wind_resource_annual_turbine_7 a
	where aep > 0
	ORDER BY RANDOM()
	limit 5000
),
a as
(
	select (r_array_sum(a.cf)/1e3)/b.aep as diff
	from b
	LEFT JOIN diffusion_wind.wind_resource_hourly_turbine_7 a
		on a.i = b.i
		and a.j = b.j
		and a.cf_bin = b.cf_bin
		and a.height = b.height
)
select round(min(diff), 5) as min, round(avg(diff), 5) as avg, round(max(diff), 5) as max
from a;
-- 0.99918,0.99999,1.00069

-- All results are very similar and suggest that the hourly and annual data
-- are totally consistent except for very minor issues related to rounding 
-- in the fixed precision hourly values

-----------------------------------------------------------------------------------------------------


