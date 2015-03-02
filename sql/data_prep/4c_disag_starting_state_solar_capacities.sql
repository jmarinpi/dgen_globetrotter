SET role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_solar.starting_capacities_mw_2012_q4_us;
CREAtE TABLE diffusion_solar.starting_capacities_mw_2012_q4_us AS
WITH customers_sums_by_sector AS
(
	SELECT a.state_abbr, 
		sum(b.total_customers_2011_residential) as state_customers_residential, 
		sum(b.total_customers_2011_commercial) as state_customers_commercial, 
		sum(b.total_customers_2011_industrial) as state_customers_industrial
	FROM diffusion_shared.county_geom a
	LEFT JOIN diffusion_shared.load_and_customers_by_county_us b
	ON a.county_id = b.county_id
	where a.state_abbr not in ('AK','HI')
	GROUP BY a.state_abbr
),
sector_alloc_factors AS
(
	SELECT state_abbr, 
		1::integer as res_alloc_factor,
		state_customers_commercial::numeric/(state_customers_commercial+state_customers_industrial) as com_alloc_factor,
		state_customers_industrial::numeric/(state_customers_commercial+state_customers_industrial) as ind_alloc_factor,
		state_customers_residential,
		state_customers_commercial,
		state_customers_industrial
	FROM customers_sums_by_sector
),
state_seia AS
(
	SELECT  a.state_abbr,
		a.res_cap_mw,
		round(a.nonres_cap_mw*com_alloc_factor,1) as com_cap_mw,
		round(a.nonres_cap_mw*ind_alloc_factor,1) as ind_cap_mw,
		a.res_systems_count,
		round(a.nonres_systems_count*com_alloc_factor,0)as com_systems_count,
		round(a.nonres_systems_count*ind_alloc_factor,0) as ind_systems_count,
		state_customers_residential,
		state_customers_commercial,
		state_customers_industrial
	FROM seia.cumulative_pv_capacity_by_state_2012_Q4 a
	LEFT JOIN sector_alloc_factors b
	ON a.state_abbr = b.state_abbr
	where a.state_abbr not in ('AK','HI')
),
combined as 
(
	SELECT a.state_abbr, a.county_id,
		coalesce(b.total_customers_2011_residential/c.state_customers_residential,0) as res_alloc_factor,
		coalesce(b.total_customers_2011_commercial/c.state_customers_commercial,0) as com_alloc_factor,
		coalesce(b.total_customers_2011_industrial/c.state_customers_industrial,0) as ind_alloc_factor,
		coalesce(c.res_cap_mw,0) as state_res_cap_mw,
		coalesce(c.com_cap_mw,0) as  state_com_cap_mw,
		coalesce(c.ind_cap_mw,0) as  state_ind_cap_mw,
		coalesce(c.res_systems_count,0) as  state_res_systems_count,
		coalesce(c.com_systems_count,0) as  state_com_systems_count,
		coalesce(c.ind_systems_count,0) as  state_ind_systems_count
	FROM diffusion_shared.county_geom a
	LEFT JOIN diffusion_shared.load_and_customers_by_county_us b
	ON a.county_id = b.county_id
	LEFT JOIN state_seia c
	ON a.state_abbr = c.state_abbr
	where a.state_abbr not in ('AK','HI')
	order by state_abbr
)
SELECT state_abbr, county_id, 
	res_alloc_factor * state_res_cap_mw as capacity_mw_residential,
	com_alloc_factor * state_com_cap_mw as capacity_mw_commercial,
	ind_alloc_factor * state_ind_cap_mw as capacity_mw_industrial,
	res_alloc_factor * state_res_systems_count as systems_count_residential,
	com_alloc_factor * state_com_systems_count as systems_count_commercial,
	ind_alloc_factor * state_ind_systems_count as systems_count_industrial
FROM combined
;
-- 3109 rows

select count(*)
FROM diffusion_shared.county_geom
where state_abbr not in ('AK','HI');
-- 3109 (row count matches)

-- any nulls
select count(*)
FROM diffusion_solar.starting_capacities_mw_2012_q4_us
where capacity_mw_residential is null
or capacity_mw_commercial is null
or capacity_mw_industrial is null
or systems_count_residential is null
or systems_count_commercial is null
or systems_count_industrial is null;
-- nope

-- create primary key and foreign key
ALTER TABLE diffusion_solar.starting_capacities_mw_2012_q4_us
  ADD CONSTRAINT starting_capacities_mw_2012_q4_us_pkey PRIMARY KEY(county_id);


-- check results
with a AS
(
	select state_abbr, 
		round(sum(capacity_mw_residential),2) as res_cap, round(sum(capacity_mw_commercial),2) com_cap, round(sum(capacity_mw_industrial),2) ind_cap,
		round(sum(systems_count_residential),2) res_sys, round(sum(systems_count_commercial),2) com_sys, round(sum(systems_count_industrial),2) ind_sys
	FROM diffusion_solar.starting_capacities_mw_2012_q4_us
	group by state_abbr
	order by state_abbr
)
SELECT a.state_abbr, res_cap, b.res_cap_mw,
	com_cap+ind_cap as nonres_cap, b.nonres_cap_mw,
	res_sys, b.res_systems_count,
	com_sys+ind_sys as nonres_sys,
	b.nonres_systems_count 
FROM a
LEFT JOIN seia.cumulative_pv_capacity_by_state_2012_Q4 b
ON a.state_abbr = b.state_abbr;

-- compare to the old version of the data
select a.capacity_mw_residential, b.capacity_mw_residential, a.capacity_mw_residential-b.capacity_mw_residential as diff1,
	a.capacity_mw_commercial, b.capacity_mw_commercial, a.capacity_mw_commercial-b.capacity_mw_commercial as diff2,
	a.capacity_mw_industrial, b.capacity_mw_industrial, a.capacity_mw_industrial-b.capacity_mw_industrial as diff3,
	a.systems_count_residential, b.systems_count_residential, a.systems_count_residential-b.systems_count_residential as diff4,
	a.systems_count_commercial, b.systems_count_commercial, a.systems_count_commercial-b.systems_count_commercial as diff5,
	a.systems_count_industrial , b.systems_count_industrial , a.systems_count_industrial -b.systems_count_industrial  as diff6
FROM diffusion_solar.starting_capacities_mw_2012_q4_us a
left join diffusion_solar.starting_capacities_mw_2014_us b
ON a.county_id = b.county_id
order by diff2 desc;