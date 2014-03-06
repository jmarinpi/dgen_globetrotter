-- find the county_id for boulder, CO
select county_id
FROM wind_ds.county_geom a
WHERE a.county = 'Boulder' -- limit by county
and a.state_abbr = 'CO'; -- and state

-- find all of the residential points in Boulder, CO
select b.gid -- this is the point gid
FROM wind_ds.county_geom a
-- left join means that we keep everything in a, and only the matching records from b
LEFT JOIN wind_ds.pt_grid_us_res b
ON a.county_id = b.county_id -- join on the county_id field in both tables
WHERE a.county = 'Boulder'
and a.state_abbr = 'CO';
-- this should return 4566 points

-- randomly select 100 residential points in Boulder, CO (note-- results willl change every time you run this)
select b.gid -- this is the point gid
FROM wind_ds.county_geom a
-- left join means that we keep everything in a, and only the matching records from b
LEFT JOIN wind_ds.pt_grid_us_res b
ON a.county_id = b.county_id -- join on the county_id field in both tables
WHERE a.county = 'Boulder'
and a.state_abbr = 'CO'
order by random() -- delete this line if you want to simply select the first 100 points
LIMIT 100;
-- should return 100 points

-- get the wind incentives uids associated with each point
WITH selected_points AS (
	select b.gid -- this is the point gid
	FROM wind_ds.county_geom a
	-- left join means that we keep everything in a, and only the matching records from b
	LEFT JOIN wind_ds.pt_grid_us_res b
	ON a.county_id = b.county_id -- join on the county_id field in both tables
	WHERE a.county = 'Boulder'
	and a.state_abbr = 'CO'
	order by random() -- delete this line if you want to simply select the first 100 points
	LIMIT 100)
SELECT a.gid, b.wind_incentives_uid
FROM selected_points a
LEFT JOIN wind_ds.dsire_incentives_lookup_res b
ON a.gid = b.pt_gid;
-- should return roughly 300 points, depending on your random sample

-- get the wind incentives uids associated with each point
WITH selected_points AS (
	select b.gid -- this is the point gid
	FROM wind_ds.county_geom a
	-- left join means that we keep everything in a, and only the matching records from b
	LEFT JOIN wind_ds.pt_grid_us_res b
	ON a.county_id = b.county_id -- join on the county_id field in both tables
	WHERE a.county = 'Boulder'
	and a.state_abbr = 'CO'
	order by random() -- delete this line if you want to simply select the first 100 points
	LIMIT 100)
SELECT a.gid, b.wind_incentives_uid
FROM selected_points a
LEFT JOIN wind_ds.dsire_incentives_lookup_res b
ON a.gid = b.pt_gid;
-- should return roughly 300 points, depending on your random sample

-- get the wind incentives data associated with each point
-- get the wind incentives uids associated with each point
WITH selected_points AS (
	select b.gid -- this is the point gid
	FROM wind_ds.county_geom a
	-- left join means that we keep everything in a, and only the matching records from b
	LEFT JOIN wind_ds.pt_grid_us_res b
	ON a.county_id = b.county_id -- join on the county_id field in both tables
	WHERE a.county = 'Boulder'
	and a.state_abbr = 'CO'
	order by random() -- delete this line if you want to simply select the first 100 points
	LIMIT 100)
SELECT a.gid, b.wind_incentives_uid,c.*
FROM selected_points a
LEFT JOIN wind_ds.dsire_incentives_lookup_res b
ON a.gid = b.pt_gid
LEFT JOIN wind_ds.incentives_raw c
ON b.wind_incentives_uid = c.uid
where c.sector = 'RES';
-- should return roughly 300 points, depending on your random sample